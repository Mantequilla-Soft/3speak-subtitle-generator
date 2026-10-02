"""
Backfill: cache Hive post metadata (title / body / tags / community) for videos
whose local doc is missing it.

Only ~33% of published embed docs carry hive_body and ~27% hive_title, but the
Hive post always has both. Fetching them once lets the tagger classify title +
body instead of falling back to a transcript sample.

Results go into our own `subtitles-hive-meta` collection. This script never
writes to 3speak's embed-video / videos collections.

Resumable: already-cached videos are skipped, so it is safe to re-run.

    python3 src/backfill_hive_metadata.py --limit 500
    python3 src/backfill_hive_metadata.py --all --sleep 0.3
"""

import argparse
import logging
import os
import sys
import time

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_manager import DatabaseManager  # noqa: E402
from hive_client import fetch_hive_post  # noqa: E402
from video_meta import hive_reference, normalize_video_metadata  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def needs_backfill(video: dict) -> bool:
    meta = normalize_video_metadata(video)
    return not (meta['body'] and meta['hive_tags'])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--limit', type=int, default=200, help='max videos to fetch')
    ap.add_argument('--all', action='store_true', help='ignore --limit')
    ap.add_argument('--sleep', type=float, default=0.2, help='seconds between Hive calls')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    config = yaml.safe_load(open(args.config))
    db = DatabaseManager(config)

    cursor = db.embed_collection.find(
        {'status': 'published'},
        {'owner': 1, 'permlink': 1, 'hive_author': 1, 'hive_permlink': 1,
         'embed_url': 1, 'hive_title': 1, 'hive_body': 1, 'hive_tags': 1,
         'category': 1, 'originalFilename': 1, '_id': 0},
    )

    scanned = fetched = cached = skipped = failed = 0
    for video in cursor:
        scanned += 1
        if not needs_backfill(video):
            skipped += 1
            continue

        author, permlink = hive_reference(video)
        if not author or not permlink:
            skipped += 1
            continue

        if db.get_hive_meta(author, permlink) is not None:
            cached += 1
            continue

        if args.dry_run:
            logger.info(f"would fetch @{author}/{permlink}")
            fetched += 1
        else:
            post = fetch_hive_post(author, permlink)
            if not post:
                failed += 1
                logger.warning(f"no Hive post for @{author}/{permlink}")
            else:
                db.save_hive_meta(author, permlink, post)
                fetched += 1
                logger.info(f"cached @{author}/{permlink} "
                            f"(body={len(post['body'])}c tags={len(post['tags'])})")
            time.sleep(args.sleep)

        if not args.all and fetched >= args.limit:
            logger.info(f"reached --limit {args.limit}, stopping early")
            break

    logger.info(f"\nscanned={scanned} fetched={fetched} already_cached={cached} "
                f"skipped_complete={skipped} failed={failed}")
    if not args.all and fetched >= args.limit:
        logger.info("More videos remain — re-run to continue (safe, resumable).")


if __name__ == '__main__':
    main()
