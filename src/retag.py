"""
Re-tag videos that were tagged by the old transcript-only pipeline.

No transcription or IPFS download is needed: the English summary is already on
the subtitles doc, and the Hive title/body/tags are either on the video doc or
in the `subtitles-hive-meta` cache (--fetch-missing fills gaps from the Hive
API and caches them).

Designed to run unattended for hours:
  * the work list is materialized up front, so writing to the tags collection
    cannot cause the cursor to revisit or skip documents;
  * --only-stale skips anything already re-tagged, so an interrupted run
    resumes simply by being restarted;
  * SIGINT/SIGTERM finish the current video, print a summary, then exit.

    python3 src/retag.py --dry-run --limit 50           # inspect first
    python3 src/retag.py --all --fetch-missing          # the real thing
"""

import argparse
import logging
import os
import re
import signal
import sys
import time
from collections import Counter

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_manager import DatabaseManager  # noqa: E402
from hive_client import fetch_hive_post  # noqa: E402
from tagger import ContentTagger  # noqa: E402
from transcript_source import local_transcript_text, word_count  # noqa: E402
from video_meta import hive_reference, merge_hive_post, normalize_video_metadata  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('tagger').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

_stop = False


def _request_stop(signum, _frame):
    global _stop
    _stop = True
    logger.warning(f"signal {signum} received — finishing current video, then exiting")


def build_metadata(db, video, allow_fetch, fetch_sleep):
    """Video metadata, topped up from the Hive post when the local doc is thin."""
    meta = normalize_video_metadata(video)
    if meta['body'] and meta['hive_tags']:
        return meta

    author, permlink = hive_reference(video)
    if not author or not permlink:
        return meta

    post = db.get_hive_meta(author, permlink)
    if post is None and allow_fetch:
        post = fetch_hive_post(author, permlink)
        if post:
            db.save_hive_meta(author, permlink, post)
        time.sleep(fetch_sleep)  # be a good Hive API citizen

    return merge_hive_post(meta, post) if post else meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--limit', type=int, default=100)
    ap.add_argument('--all', action='store_true', help='process every matching doc')
    ap.add_argument('--dry-run', action='store_true', help='write nothing')
    ap.add_argument('--fetch-missing', action='store_true',
                    help='hit the Hive API when metadata is not cached (slower)')
    ap.add_argument('--fetch-sleep', type=float, default=0.2)
    ap.add_argument('--only-stale', action='store_true',
                    help='skip docs already re-tagged by the current pipeline')
    ap.add_argument('--only-empty', action='store_true',
                    help='process only docs that currently have no tags')
    ap.add_argument('--community', default='',
                    help='re-tag only videos in this community (hive-XXXX id or '
                         'display name). Use after removing a community rule, to '
                         'hand those videos back to the classifier.')
    ap.add_argument('--subtitles-dir', default='/app/subtitles',
                    help='where local .srt files live, for transcript fallback')
    ap.add_argument('--min-content-words', type=int, default=0,
                    help='skip a doc if it has no author-tag evidence and fewer '
                         'than this many words of usable text (summary or SRT). '
                         'Avoids re-churning near-wordless shorts.')
    ap.add_argument('--progress-every', type=int, default=25)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    with open(args.config) as f:
        config = yaml.safe_load(f)
    db = DatabaseManager(config)
    tagger = ContentTagger(config)

    # Never touch hand-corrected videos (set_tags.py marks them manual=true).
    # Without this, a re-tag would silently overwrite every manual fix.
    query = {'manual': {'$ne': True}}
    if args.only_stale:
        query['tag_model'] = {'$exists': False}
    if args.only_empty:
        # tags_list is the structured field; fall back to the legacy string too.
        query['$or'] = [{'tags_list': {'$size': 0}}, {'tags': ''},
                        {'tags': {'$exists': False}}]

    members = None
    if args.community:
        # A community is stored as EITHER its hive-XXXX id or its display name,
        # depending on the record — so pass BOTH forms, comma-separated:
        #
        #     --community "hive-153850,Hive Learners"
        #
        # These are matched EXACTLY (case-insensitively). We deliberately do NOT
        # derive the id<->name pairing from `community_title`: that field is
        # unreliable. Most rows with community='hive-153850' carry the title
        # 'Threespeak', so auto-resolving would have swept 13k unrelated videos
        # into the work list. Explicit beats clever here.
        aliases = [c.strip() for c in args.community.split(',') if c.strip()]
        logger.info(f"community values (exact match): {aliases}")

        variants = []
        for a in aliases:
            variants.append(a)
            variants.append(re.compile(f'^{re.escape(a)}$', re.IGNORECASE))

        members = set()
        for cname, field in (('embed-video', 'category'), ('videos', 'community'),
                             ('videos', 'category'), ('embed-audio', 'category')):
            for v in db.db[cname].find({field: {'$in': variants}},
                                       {'owner': 1, 'permlink': 1, '_id': 0}):
                members.add((v.get('owner'), v.get('permlink')))
        logger.info(f"community {want!r}: {len(members)} videos")
        if not members:
            logger.error("no videos found in that community")
            return 1

    # Materialize the work list: we write to this same collection as we go, and
    # a live cursor could then return a document twice or skip one entirely.
    cursor = db.tags_collection.find(
        query, {'author': 1, 'permlink': 1, 'tags': 1, '_id': 0}
    ).sort('_id', 1)

    worklist = []
    for d in cursor:
        # Filter membership here rather than with a giant $or, which would not
        # scale to a community with thousands of videos.
        if members is not None and (d['author'], d['permlink']) not in members:
            continue
        worklist.append(d)
        if not args.all and len(worklist) >= args.limit:
            break

    total = len(worklist)
    logger.info(f"{total} videos to re-tag"
                f"{' (dry run)' if args.dry_run else ''}"
                f"{' [only-stale]' if args.only_stale else ''}"
                f"{f' [community={args.community}]' if args.community else ''}")
    if not total:
        return 0

    before, after = Counter(), Counter()
    processed = changed = no_video = skipped_thin = 0
    started = time.time()

    for doc in worklist:
        if _stop:
            break

        author, permlink = doc['author'], doc['permlink']
        try:
            video = db.get_video_by_owner_permlink(author, permlink)
            if not video:
                no_video += 1
                continue

            old_tags = [t for t in (doc.get('tags') or '').split(',') if t]
            meta = build_metadata(db, video, args.fetch_missing, args.fetch_sleep)
            sub = db.subtitles_collection.find_one(
                {'author': author, 'permlink': permlink}, {'summary_en': 1, '_id': 0}
            ) or {}

            # The transcript is not in Mongo; recover it from the local SRT so the
            # classifier sees the same English text the live pipeline had. Prefer
            # the summary (denser) and fall back to the full transcript.
            content_text = sub.get('summary_en') or ''
            if not content_text:
                content_text = local_transcript_text(
                    author, permlink, base_dir=args.subtitles_dir)

            # Skip near-wordless videos that have no author-tag evidence either:
            # re-tagging them would just reconfirm zero tags and churn the row.
            if args.min_content_words:
                evidence = (meta.get('hive_tags') or meta.get('category'))
                if not evidence and word_count(content_text) < args.min_content_words:
                    skipped_thin += 1
                    continue

            result = tagger.generate_tags(metadata=meta, content_text=content_text)
            new_tags = result['tags']

            before.update(old_tags)
            after.update(new_tags)
            processed += 1

            if old_tags != new_tags:
                changed += 1
                logger.info(f"@{author}/{permlink}: [{', '.join(old_tags) or '-'}] "
                            f"-> [{', '.join(new_tags) or '-'}] "
                            f"(evidence: {', '.join(result['evidence']) or 'none'})")

            if not args.dry_run:
                db.save_tags(author, permlink, new_tags,
                             scores=result['scores'], evidence=result['evidence'],
                             model=result['model'])

        except Exception as e:
            # One bad video must not end a six-hour run.
            logger.error(f"@{author}/{permlink} failed: {e}")
            continue

        if processed and processed % args.progress_every == 0:
            rate = processed / (time.time() - started)
            eta_min = (total - processed) / rate / 60 if rate else 0
            logger.info(f"progress {processed}/{total} "
                        f"({rate * 60:.1f}/min, ETA {eta_min:.0f} min)")

    elapsed = (time.time() - started) / 60
    logger.info(f"\nprocessed={processed}/{total} changed={changed} "
                f"video_missing={no_video} skipped_thin={skipped_thin} "
                f"elapsed={elapsed:.0f}min"
                f"{'  (DRY RUN — nothing written)' if args.dry_run else ''}"
                f"{'  (INTERRUPTED — rerun with --only-stale to resume)' if _stop else ''}")

    if processed:
        logger.info(f"\n{'tag':<16}{'before':>8}{'after':>8}")
        for tag in sorted(set(before) | set(after), key=lambda t: -before[t]):
            logger.info(f"{tag:<16}{100 * before[tag] / processed:>7.0f}%"
                        f"{100 * after[tag] / processed:>7.0f}%")
    return 0


if __name__ == '__main__':
    sys.exit(main())
