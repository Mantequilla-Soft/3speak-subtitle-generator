"""
Backfill: the `ai_generated_v2` facet (TAXONOMY_V2.md's cheap-facet pattern —
is this video AI-made, detected from title/description/Hive tags alone, no
model) for videos that already have v2 topic tags from before this facet
existed.

Why a separate script rather than just re-running tag_videos_v2.py: its
worklist is gated on `tags_list_v2` missing, so any video already v2-tagged
is invisible to it forever. The AI flag is independent of topic tagging (see
tag_videos_v2.process_one / tag_live_v2.tag_video_v2, which now write it for
every NEW video going forward) — this script is the one-shot catch-up for
everything tagged before that wiring landed.

Scope: embed-video (all statuses reachable on a frontend) and the legacy
`videos` collection, restricted to roughly the last 3 years by default —
older content is long-tail and AI-video generation wasn't common before then.
Pass --since to widen it or --all-time to drop the date filter entirely.

Resumable: already-flagged videos (ai_generated_v2 present) are skipped, so
it's safe to re-run or leave on --watch like the other tools/ services.

    python3 src/backfill_ai_flag.py --dry-run --limit 50
    python3 src/backfill_ai_flag.py --limit 5000 --sleep 0.02
    python3 src/backfill_ai_flag.py --watch 3600   # long-running service mode
    python3 src/backfill_ai_flag.py --author mdmilon12 --recheck --all-time
"""

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tag_taxonomy_v2 as v2  # noqa: E402
from db_manager import DatabaseManager  # noqa: E402
from hive_client import fetch_hive_post, fetch_incubation_post  # noqa: E402
from video_meta import hive_reference, merge_hive_post, normalize_video_metadata  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_SINCE_YEARS = 3

_stop = False


def _handle_stop(signum, _frame):
    global _stop
    _stop = True
    logger.info("stop requested — finishing current video")


def _is_manual_evidence(evidence):
    """True if an ai_generated_v2 value was set by an admin (see server.js
    POST /videos/ai-flag), never by the automated detector."""
    return any(str(e).startswith('manual:') for e in (evidence or []))


def build_worklist(db, since_date, embed_only=False, recheck=False, author=None):
    """
    Videos needing an ai_generated_v2 (re)check, newest first.

    recheck=False (default): only videos with no flag yet — the normal
        one-shot catch-up.
    recheck=True: also revisit videos the AUTOMATED detector already flagged,
        so a tightened/loosened detect_ai_generated() can correct earlier
        calls across the backlog. Never touches a video an admin corrected
        by hand through the Videos page (ai_generated_evidence_v2 starts
        with 'manual:') — those are always left alone.

    Same eligibility as tag_videos_v2.build_worklist (orphan embeds and
    hidden creators are unreachable on every frontend, so skip them too) —
    EXCEPT a `warmup_approved` orphan: a Hive-less ("incubating") creator's
    upload that an admin approved on the Warm-up Review page. Those ARE
    reachable (3speak.tv renders them straight from the checker service, no
    Hive post needed yet — confirmed via gusna/9uvmf4h4), so excluding them
    the same way as a genuinely dead/unlinked orphan was wrong. Small subset
    (~13 as of 2026-10-18), so this doesn't change the worklist's scale.
    """
    raw = db.db
    flagged = set()
    for d in db.tags_collection.find(
            {'ai_generated_v2': {'$exists': True}},
            {'author': 1, 'permlink': 1, 'ai_generated_evidence_v2': 1, '_id': 0}):
        key = (d.get('author'), d.get('permlink'))
        if recheck:
            if _is_manual_evidence(d.get('ai_generated_evidence_v2')):
                flagged.add(key)   # admin-set — always skip, recheck or not
        else:
            flagged.add(key)
    needs = []
    hidden = {d['username'] for d in
              raw['contentcreators'].find({'hidden': True}, {'username': 1, '_id': 0})}
    logger.info(f"{'excluded (manual)' if recheck else 'already flagged'}: "
                f"{len(flagged):,}  hidden creators: {len(hidden)}")

    embed_q = {'status': 'published'}
    if author:
        embed_q['owner'] = author
    if since_date:
        embed_q['createdAt'] = {'$gte': since_date}
    for vdoc in db.embed_collection.find(embed_q).sort('createdAt', -1):
        if vdoc['owner'] in hidden:
            continue
        if (vdoc.get('hive_permlink') or '').strip() == '' and not vdoc.get('warmup_approved'):
            continue
        if (vdoc['owner'], vdoc['permlink']) not in flagged:
            needs.append(vdoc)

    if not embed_only:
        legacy_q = {'status': 'published'}
        if author:
            legacy_q['owner'] = author
        if since_date:
            legacy_q['created'] = {'$gte': since_date}
        for vdoc in db.videos_collection.find(legacy_q).sort('created', -1):
            if vdoc['owner'] in hidden:
                continue
            if (vdoc['owner'], vdoc['permlink']) not in flagged:
                needs.append(vdoc)
    return needs


def _enrich_meta(db, vdoc, meta):
    """
    Fall back to a fetched Hive post, then 3speak's incubation API, when the
    local doc's title/body/tags are sparse — same fallback chain as the live
    pipeline (main.py SubtitleService._video_metadata), so a backfill/recheck
    pass sees the same metadata a freshly-processed video would.

    The Hive branch checks our own subtitles-hive-meta cache first (built by
    backfill_hive_metadata.py), so it's a live fetch only on a cache miss —
    not a per-video network call for the whole backlog. The incubation branch
    only triggers when hive_reference() can't resolve an author/permlink at
    all, i.e. the small warmup_approved-orphan subset build_worklist now
    includes (see its docstring) — also not a broad fetch.
    """
    if meta['body'] and meta['hive_tags']:
        return meta
    author, permlink = hive_reference(vdoc)
    if author and permlink:
        post = db.get_hive_meta(author, permlink)
        if post is None:
            post = fetch_hive_post(author, permlink)
            if post:
                db.save_hive_meta(author, permlink, post)
        if post:
            return merge_hive_post(meta, post)
    owner, vpermlink = vdoc.get('owner'), vdoc.get('permlink')
    if owner and vpermlink:
        post = fetch_incubation_post(owner, vpermlink)
        if post:
            return merge_hive_post(meta, post)
    return meta


def process_one(db, args, vdoc, stats):
    owner, permlink = vdoc['owner'], vdoc['permlink']
    meta = normalize_video_metadata(vdoc)
    meta = _enrich_meta(db, vdoc, meta)
    is_ai, evidence = v2.detect_ai_generated(meta['hive_tags'], meta['title'], meta['body'],
                                           author=owner)

    # _enrich_meta already tried a Hive-post + incubation fetch, so blank
    # metadata here usually means there's genuinely nothing to find (not a
    # sync race like the live pipeline can hit) — but on a transient fetch
    # failure, writing "false, no evidence" would look identical to a real
    # negative and this doc would never be retried outside --recheck. Leave
    # it unset instead, same guard as tag_live_v2._save_ai_flag.
    has_metadata = bool(meta['title'] or meta['body'] or meta['hive_tags'])
    if not is_ai and not has_metadata:
        stats['skipped_blank'] += 1
        return

    line = f"{owner}/{permlink} -> ai_generated_v2={is_ai} {evidence if is_ai else ''}"
    if args.dry_run:
        logger.info(f"DRY {line}")
        stats['flagged_ai' if is_ai else 'flagged_not_ai'] += 1
        return

    existing = db.tags_collection.find_one(
        {'author': owner, 'permlink': permlink},
        {'manual': 1, 'ai_generated_evidence_v2': 1})
    if existing and (existing.get('manual')
                     or _is_manual_evidence(existing.get('ai_generated_evidence_v2'))):
        stats['skipped_manual'] += 1
        return

    db.tags_collection.update_one(
        {'author': owner, 'permlink': permlink},
        {'$set': {
            'author': owner, 'permlink': permlink,
            'ai_generated_v2': is_ai,
            'ai_generated_evidence_v2': evidence,
            'ai_generated_checked_at': datetime.now(),
        }},
        upsert=True)
    stats['flagged_ai' if is_ai else 'flagged_not_ai'] += 1
    if is_ai:
        logger.info(line)


def run_pass(db, args, since_date):
    work = build_worklist(db, since_date, embed_only=args.embed_only, recheck=args.recheck,
                          author=args.author)
    logger.info(f"worklist: {len(work):,} videos needing an ai_generated_v2 "
                f"{'recheck' if args.recheck else 'flag'}")
    if args.limit:
        work = work[:args.limit]
    stats = {'flagged_ai': 0, 'flagged_not_ai': 0, 'skipped_manual': 0, 'skipped_blank': 0,
             'errors': 0}
    for n, vdoc in enumerate(work):
        if _stop:
            break
        try:
            process_one(db, args, vdoc, stats)
        except Exception:
            # One malformed doc (e.g. unexpected Hive json_metadata shape —
            # see hive_client.fetch_hive_post) must not lose an hour-long
            # pass over the other ~138k videos. Left unflagged, so it's
            # retried (and logged again if still broken) next pass.
            stats['errors'] += 1
            logger.exception(f"process_one failed for {vdoc.get('owner')}/{vdoc.get('permlink')} — skipping")
        if n % 200 == 0:
            logger.info(f"[{n}/{len(work)}] {stats}")
        if args.sleep:
            time.sleep(args.sleep)
    logger.info(f"pass done: {stats}")
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=0, help='0 = whole worklist (per pass)')
    ap.add_argument('--embed-only', action='store_true',
                    help='skip the legacy `videos` collection entirely')
    ap.add_argument('--since', default=None,
                    help=f'only videos created on/after this ISO date '
                         f'(default: {DEFAULT_SINCE_YEARS} years ago)')
    ap.add_argument('--all-time', action='store_true',
                    help='ignore --since entirely, cover the full backlog')
    ap.add_argument('--recheck', action='store_true',
                    help='also re-run detect_ai_generated() on videos the automated '
                         'detector already flagged (e.g. after a detection-logic fix); '
                         'never touches a video an admin corrected by hand')
    ap.add_argument('--author', default=None,
                    help='only this creator\'s videos (combine with --recheck --all-time '
                         'to re-evaluate one channel after a detection change)')
    ap.add_argument('--watch', type=int, default=0, metavar='SECONDS',
                    help='service mode: re-scan every N seconds')
    ap.add_argument('--sleep', type=float, default=0.0)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    if args.all_time:
        since_date = None
    elif args.since:
        since_date = datetime.fromisoformat(args.since)
    else:
        since_date = datetime.now() - timedelta(days=365 * DEFAULT_SINCE_YEARS)

    config = yaml.safe_load(open(args.config))
    db = DatabaseManager(config)

    if not args.watch:
        run_pass(db, args, since_date)
        return 0

    logger.info(f"watch mode: scanning every {args.watch}s")
    while not _stop:
        try:
            run_pass(db, args, since_date)
        except Exception:
            logger.exception("pass failed — retrying next cycle")
        for _ in range(args.watch):
            if _stop:
                break
            time.sleep(1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
