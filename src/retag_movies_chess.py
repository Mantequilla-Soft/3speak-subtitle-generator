"""
One-off background sweep: re-tag movie/TV and chessbrothers videos under the
updated taxonomy.

Fixes applied by re-running fusion on these videos:
  * the new `film-tv` leaf + CineTV/Movies&TV community + cinetv/movies/... tags
  * `chessbrothers` no longer means sports (so wrongly-`sports` reviews clear)
  * analysis-first fusion (title/description primary, author tags fallback)

Text + evidence only — NO vision. The decisive signal here is the description
and the community/tags, and most legacy videos have dead IPFS anyway, so frame
fetching would only add hours of timeouts. Force-overwrites tags_list_v2 for the
matched set; the classic v1 fields and manual locks are untouched, as always.

    python3 src/retag_movies_chess.py --dry-run --limit 20
    python3 src/retag_movies_chess.py
"""

import argparse
import logging
import os
import re
import signal
import sys
import time
from types import SimpleNamespace

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tag_taxonomy_v2 as v2  # noqa: E402
from tag_videos_v2 import TextTagger, fuse, _write, MIN_TEXT_CHARS  # noqa: E402
from db_manager import DatabaseManager  # noqa: E402
from video_meta import normalize_video_metadata  # noqa: E402
from tag_taxonomy import clean_post_body  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# tags that should point at film-tv, plus the chessbrothers safety net
MOVIE_TAGS = {'cinetv', 'movies', 'movie', 'film', 'films', 'tvshow', 'tvshows',
              'tvseries', 'series', 'serie', 'netflix', 'cinema', 'moviereview',
              'filmreview', 'boxoffice'}
TRIGGER_TAGS = MOVIE_TAGS | {'chessbrothers'}
EMBED_COMMUNITIES = {'hive-121744', 'hive-166847'}
LEGACY_COMMUNITIES = {'cinetv', 'movies & tv shows'}
_PREFILTER = re.compile('|'.join(TRIGGER_TAGS | {'movies & tv'}), re.I)

_stop = False


def _handle_stop(*_):
    global _stop
    _stop = True
    logger.info("stop requested — finishing current video")


def _matches(meta):
    """True if this video is in-scope: movie community/tag or chessbrothers."""
    cat = (meta['category'] or '').strip().lower()
    if cat in EMBED_COMMUNITIES or cat in LEGACY_COMMUNITIES:
        return True
    tags = {str(t).strip().lower() for t in (meta['hive_tags'] or [])}
    return bool(tags & TRIGGER_TAGS)


def collect(db):
    """Videos in scope, newest first, embed then legacy. De-duped by (owner, permlink)."""
    seen, out = set(), []
    embed_q = {'status': 'published', '$or': [
        {'category': {'$in': list(EMBED_COMMUNITIES)}},
        {'hive_tags': {'$in': list(TRIGGER_TAGS)}}]}
    for v in db.embed_collection.find(embed_q).sort('createdAt', -1):
        k = (v['owner'], v['permlink'])
        if k not in seen:
            seen.add(k); v['_type'] = 'embed'; out.append(v)
    legacy_q = {'status': 'published', '$or': [
        {'community': {'$regex': '^(CineTV|Movies & TV Shows)$', '$options': 'i'}},
        {'tags': {'$regex': _PREFILTER}}]}
    for v in db.videos_collection.find(legacy_q).sort('created', -1):
        k = (v['owner'], v['permlink'])
        if k not in seen:
            seen.add(k); v['_type'] = 'legacy'; out.append(v)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--sleep', type=float, default=0.05)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = yaml.safe_load(open(args.config))
    db = DatabaseManager(config)
    text = TextTagger(config)
    wargs = SimpleNamespace(dry_run=args.dry_run)

    work = collect(db)
    logger.info(f"in-scope videos: {len(work):,}")
    if args.limit:
        work = work[:args.limit]

    stats = {'exclusive': 0, 'retagged': 0, 'empty': 0, 'skipped_prefilter': 0}
    for n, vdoc in enumerate(work):
        if _stop:
            break
        meta = normalize_video_metadata(vdoc)
        # legacy prefilter regex can over-match ('movies' inside another word) —
        # confirm the precise membership before touching the row.
        if not _matches(meta):
            stats['skipped_prefilter'] += 1
            continue
        owner, permlink = vdoc['owner'], vdoc['permlink']

        # exclusive creator / community rule wins outright (CineTV -> film-tv)
        excl = v2.exclusive_tags_for_author(owner) or v2.exclusive_tags_for(meta['category'])
        if excl:
            _write(db, wargs, owner, permlink, excl, {}, excl, 'community-rule-v2')
            stats['exclusive'] += 1
            continue

        # analysis (text) + evidence fallback, via the shared fuse()
        evidence = v2.tags_from_hive_tags(meta['hive_tags']) | v2.tags_from_category(meta['category'])
        title, body = meta['title'], clean_post_body(meta['body'], 1200)
        text_tags = []
        if len(body) >= MIN_TEXT_CHARS or title:
            try:
                text_tags = text.analyze((f"Title: {title}\n" if title else '') + body)
            except Exception as e:
                logger.warning(f"  text classify failed {owner}/{permlink}: {e}")
        tags = fuse(evidence, None, None, text_tags)
        scores = {leaf: s for leaf, s in text_tags}

        if tags:
            _write(db, wargs, owner, permlink, tags, scores, sorted(evidence), 'v2-retag')
            stats['retagged'] += 1
        else:
            stats['empty'] += 1

        if n % 100 == 0:
            logger.info(f"[{n}/{len(work)}] {stats}")
        if args.sleep:
            time.sleep(args.sleep)

    logger.info(f"done: {stats}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
