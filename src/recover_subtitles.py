"""
Recover subtitles that never reached the public gateway.

Pinning to the gateway broke ~2026-04 (a proxy in front of ipfs.3speak.tv timed
out every pin/add), and our local IPFS node has since GC'd the content from its
blockstore — but the .srt files are still on disk. So recovery is:

  1. re-ADD the on-disk .srt to our local node -> restores the exact CID
     (content-addressed) and pins it locally again
  2. PIN that CID on the supernode's API directly -> the supernode fetches it
     over our peered link and its pull-zone serves it publicly

Scoped to the affected window (default: subtitles touched on/after --since) so it
doesn't re-do the older ones that are already fine. Idempotent, parallel,
resumable — a failed CID is logged, not retried forever.

    python3 src/recover_subtitles.py --dry-run --since 2026-03-25
    python3 src/recover_subtitles.py --since 2026-03-25 --workers 12
"""

import argparse
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_manager import DatabaseManager  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

LOCAL_ADD = os.getenv('LOCAL_ADD', 'http://127.0.0.1:5001/api/v0/add')
PIN_API = os.getenv('PIN_API', 'http://65.21.201.94:5002/api/v0/pin/add')
# on the host the files live in the repo; in a container they are at /app/subtitles
SUB_DIR = os.getenv('SUB_DIR', os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), 'subtitles'))

_stop = False


def _handle_stop(*_):
    global _stop
    _stop = True
    logger.info("stop requested — finishing in-flight work")


def recover_one(task):
    """(author, permlink, lang, cid) -> ('ok'|'lost'|'mismatch'|'pinfail', cid)."""
    author, permlink, lang, cid = task
    path = os.path.join(SUB_DIR, author, f"{permlink}.{lang}.srt")
    if not os.path.exists(path):
        return ('lost', cid)
    try:
        # 1. restore to our local node (same CID, re-pinned locally)
        with open(path, 'rb') as fh:
            r = requests.post(f"{LOCAL_ADD}?cid-version=0&pin=true",
                              files={'file': fh}, timeout=30)
        added = r.json().get('Hash')
        if added != cid:
            return ('mismatch', cid)
        # 2. pin on the supernode (it fetches from us now)
        pr = requests.post(f"{PIN_API}?arg={cid}", timeout=60)
        return ('ok' if pr.ok else 'pinfail', cid)
    except Exception:
        return ('pinfail', cid)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--since', default='2026-03-25',
                    help='only subtitles updated on/after this date (YYYY-MM-DD)')
    ap.add_argument('--workers', type=int, default=12)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = yaml.safe_load(open(args.config))
    db = DatabaseManager(config)
    since = datetime.fromisoformat(args.since)

    tasks = []
    for d in db.subtitles_collection.find(
            {'is_alias': {'$ne': True}, 'updated_at': {'$gte': since},
             'subtitles': {'$exists': True}},
            {'author': 1, 'permlink': 1, 'subtitles': 1, '_id': 0}).batch_size(3000):
        for lang, cid in (d.get('subtitles') or {}).items():
            if cid:
                tasks.append((d['author'], d['permlink'], lang, cid))
    if args.limit:
        tasks = tasks[:args.limit]
    logger.info(f"subtitle CIDs to recover (since {args.since}): {len(tasks):,}")
    logger.info(f"files: {SUB_DIR}  |  local add: {LOCAL_ADD}  |  pin: {PIN_API}")
    if args.dry_run:
        present = sum(os.path.exists(os.path.join(SUB_DIR, a, f"{p}.{l}.srt"))
                      for a, p, l, _ in tasks[:200])
        logger.info(f"DRY-RUN — of first 200, {present} .srt files present on disk")
        return 0

    counts = {'ok': 0, 'lost': 0, 'mismatch': 0, 'pinfail': 0}
    with ThreadPoolExecutor(args.workers) as ex:
        futs = [ex.submit(recover_one, t) for t in tasks]
        for n, fut in enumerate(as_completed(futs)):
            status, _cid = fut.result()
            counts[status] += 1
            if n % 500 == 0:
                logger.info(f"[{n}/{len(tasks)}] {counts}")
            if _stop:
                break
    logger.info(f"done: {counts}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
