"""
Recover subtitles that never reached the public gateway.

Pinning to the gateway broke ~2026-04 (a proxy in front of ipfs.3speak.tv timed
out every pin/add), and our local IPFS node has since GC'd the content from its
blockstore — but the .srt files are still on disk. So recovery is:

  1. re-ADD the on-disk .srt to our local node -> restores the exact CID
     (content-addressed) and pins it locally again
  2. ADD the same file on the supernode's API directly -> same CID, pinned
     there, and its pull-zone serves it publicly. (Pin-by-CID made the
     supernode fetch from us over bitswap, which stalls — 2026-10.)

--failed re-pushes only docs flagged remote_pin_failed (any date) and clears
the flag on success.

Scoped to the affected window (default: subtitles touched on/after --since) so it
doesn't re-do the older ones that are already fine. Idempotent, parallel,
resumable — a failed CID is logged, not retried forever.

    python3 src/recover_subtitles.py --dry-run --since 2026-03-25
    python3 src/recover_subtitles.py --since 2026-03-25 --workers 12
    python3 src/recover_subtitles.py --failed --workers 8
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
REMOTE_ADD = os.getenv('REMOTE_ADD', 'http://65.21.201.94:5002/api/v0/add')
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
        # 2. add the same bytes on the supernode (pinned there, same CID)
        with open(path, 'rb') as fh:
            pr = requests.post(f"{REMOTE_ADD}?cid-version=0&pin=true",
                               files={'file': fh}, timeout=60)
        if not pr.ok:
            return ('pinfail', cid)
        return ('ok' if pr.json().get('Hash') == cid else 'mismatch', cid)
    except Exception:
        return ('pinfail', cid)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--since', default='2026-03-25',
                    help='only subtitles updated on/after this date (YYYY-MM-DD)')
    ap.add_argument('--workers', type=int, default=12)
    ap.add_argument('--failed', action='store_true',
                    help='only CIDs flagged remote_pin_failed (ignores --since); clears the flag on success')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = yaml.safe_load(open(args.config))
    db = DatabaseManager(config)
    since = datetime.fromisoformat(args.since)

    tasks = []
    if args.failed:
        for d in db.subtitles_collection.find(
                {'is_alias': {'$ne': True}, 'remote_pin_failed.0': {'$exists': True}},
                {'author': 1, 'permlink': 1, 'remote_pin_failed': 1, '_id': 0}).batch_size(3000):
            for e in d['remote_pin_failed']:
                if e.get('cid'):
                    tasks.append((d['author'], d['permlink'], e['lang'], e['cid']))
    else:
        for d in db.subtitles_collection.find(
                {'is_alias': {'$ne': True}, 'updated_at': {'$gte': since},
                 'subtitles': {'$exists': True}},
                {'author': 1, 'permlink': 1, 'subtitles': 1, '_id': 0}).batch_size(3000):
            for lang, cid in (d.get('subtitles') or {}).items():
                if cid:
                    tasks.append((d['author'], d['permlink'], lang, cid))
    if args.limit:
        tasks = tasks[:args.limit]
    scope = 'flagged remote_pin_failed' if args.failed else f'since {args.since}'
    logger.info(f"subtitle CIDs to recover ({scope}): {len(tasks):,}")
    logger.info(f"files: {SUB_DIR}  |  local add: {LOCAL_ADD}  |  remote add: {REMOTE_ADD}")
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
            if status == 'ok' and args.failed:
                # primary and hive_permlink mirror both carry the flag
                db.subtitles_collection.update_many(
                    {'remote_pin_failed.cid': _cid},
                    {'$pull': {'remote_pin_failed': {'cid': _cid}}})
            if n % 500 == 0:
                logger.info(f"[{n}/{len(tasks)}] {counts}")
            if _stop:
                break
    logger.info(f"done: {counts}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
