"""
Backfill subtitles for legacy videos silently skipped by a selection-query
bug: db_manager's video queries required a non-empty 'filename' (a direct
IPFS file CID), but the uploader stopped setting 'filename' on native
uploads around 2026-06 — they carry only the HLS manifest in 'video_v2'
since. Those videos were never even offered to the pipeline, even though
process_video() has always known how to download HLS. Fixed 2026-08-10
(db_manager.HAS_VIDEO_SOURCE now matches either field); this script is the
one-shot catch-up for everything that slipped through while the bug was live.

Reuses the exact same pipeline as the live generator (SubtitleService.
process_video: transcribe -> translate -> tag -> pin -> Mongo write) instead
of reimplementing it, scoped to just the affected backlog so it can run
detached alongside the main service without touching its START_DATE cursor
or its priority queue.

Safe to interrupt: SIGTERM finishes the current video, then exits. Nothing
is remembered between runs beyond what's already in Mongo — a restart
re-derives the backlog and get_fully_processed_keys excludes anything a
prior run completed, so it resumes rather than redoing work.

    python3 src/backfill_hls_gap.py --dry-run --since 2026-02-18
    python3 src/backfill_hls_gap.py --since 2026-02-18
"""

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import SubtitleService  # noqa: E402 — reuse the full pipeline as-is

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

_stop = False


def _request_stop(signum, _frame):
    global _stop
    _stop = True
    logger.warning(f"signal {signum} received — finishing current video, then exiting")


def find_backlog(svc, since_date):
    """Legacy videos with only 'video_v2' (no 'filename') — what the bug skipped."""
    query = {
        'created': {'$gte': since_date},
        'status': 'published',
        'filename': {'$in': [None, '']},
        'video_v2': {'$exists': True, '$nin': [None, ''], '$regex': '^ipfs://'},
    }
    videos = list(svc.db.videos_collection.find(query).sort('created', 1))
    for v in videos:
        v['_video_type'] = 'legacy'
    return videos


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--since', default='2026-02-18',
                    help='only videos created on/after this date (YYYY-MM-DD)')
    ap.add_argument('--dry-run', action='store_true', help='list the backlog, process nothing')
    ap.add_argument('--limit', type=int, default=0, help='cap how many videos to process')
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    since_date = datetime.strptime(args.since, '%Y-%m-%d')

    # --dry-run only lists candidates; skip the (slow) model loading for it.
    if args.dry_run:
        import yaml
        from db_manager import DatabaseManager
        config = yaml.safe_load(open(args.config))
        db = DatabaseManager(config)

        class _Stub:
            pass
        stub = _Stub()
        stub.db = db
        backlog = find_backlog(stub, since_date)
        all_lang_codes = [lang['code'] for lang in config['languages']]
        exclude = (db.get_fully_processed_keys(all_lang_codes)
                  | db.get_max_failed_keys(config.get('processing', {}).get('max_retries', 3))
                  | db.get_blacklisted_keys())
        bl_authors = db.get_blacklisted_authors()
        backlog = [v for v in backlog
                  if (v['owner'], v['permlink']) not in exclude and v['owner'] not in bl_authors]
        logger.info(f"DRY-RUN — backlog: {len(backlog)} video_v2-only videos since {args.since}")
        for v in backlog[:30]:
            logger.info(f"  would process: {v['created']:%Y-%m-%d} {v['owner']}/{v['permlink']}")
        if len(backlog) > 30:
            logger.info(f"  ... and {len(backlog) - 30} more")
        return 0

    svc = SubtitleService(args.config)

    backlog = find_backlog(svc, since_date)
    all_lang_codes = [lang['code'] for lang in svc.language_configs]
    fully_done = svc.db.get_fully_processed_keys(all_lang_codes)
    max_failed = svc.db.get_max_failed_keys(svc.max_retries)
    bl_keys = svc.db.get_blacklisted_keys()
    bl_authors = svc.db.get_blacklisted_authors()
    exclude = fully_done | max_failed | bl_keys
    before = len(backlog)
    backlog = [v for v in backlog
              if (v['owner'], v['permlink']) not in exclude and v['owner'] not in bl_authors]
    logger.info(f"Found {before} video_v2-only videos since {args.since}, "
               f"{len(fully_done)} already complete, {len(max_failed)} max-failed, "
               f"{len(bl_keys)} blacklisted, {len(backlog)} to process")

    if args.limit:
        backlog = backlog[:args.limit]

    svc._premium_users = svc.db.get_premium_users()
    if svc._premium_users:
        logger.info(f"Premium users loaded: {len(svc._premium_users)}")

    ok = 0
    failed = 0
    for i, video in enumerate(backlog, 1):
        if _stop:
            logger.warning(f"stopping before video {i}/{len(backlog)} (SIGTERM/SIGINT)")
            break

        owner = video.get('owner', 'unknown')
        permlink = video.get('permlink', 'unknown')
        is_premium = owner in svc._premium_users

        logger.info(f"\n[{i}/{len(backlog)}] {owner}/{permlink}  "
                   f"(created {video.get('created')})")
        svc.db.set_processing(owner, permlink, video_type='legacy')
        try:
            if svc.process_video(video, is_premium=is_premium):
                ok += 1
            else:
                failed += 1
                count = svc.db.record_failure(owner, permlink)
                logger.warning(f"Failure #{count}/{svc.max_retries} for {owner}/{permlink}")
        finally:
            svc.db.clear_processing()

        time.sleep(2)

    remaining = len(backlog) - ok - failed
    logger.info(f"\n{'=' * 80}")
    logger.info("Backfill summary:")
    logger.info(f"  Processed: {ok + failed}  |  OK: {ok}  |  Failed: {failed}"
               + (f"  |  Not reached: {remaining}" if remaining else ""))
    logger.info(f"{'=' * 80}\n")
    return 0


if __name__ == '__main__':
    sys.exit(main())
