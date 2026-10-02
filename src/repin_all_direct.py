"""
Repair the subtitle-pin backlog by pinning missing subtitle CIDs DIRECTLY to the
supernode's IPFS API — bypassing the ipfs.3speak.tv proxy that killed slow
pin/add calls. With our node peered to the supernode, each small file fetches in
under a second and the supernode's pull-zone serves it publicly.

Two phases, both parallel and order-independent (no head-of-line blocking):
  1. CHECK every subtitle CID against the supernode's own gateway (fast, no
     proxy) and keep only the ones it can't already serve.
  2. PIN just those missing CIDs to the supernode API.

Most subtitles pinned fine outside the outage window, so phase 1 keeps this
cheap. A CID that is missing AND no longer on our local node (GC'd) will fail to
pin — those are logged, not retried forever.

    python3 src/repin_all_direct.py --dry-run     # report how many are missing
    python3 src/repin_all_direct.py --workers 24
"""

import argparse
import logging
import os
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_manager import DatabaseManager  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# The supernode's own API + gateway, not the proxied public domain.
PIN_API = os.getenv('PIN_API', 'http://65.21.201.94:5002/api/v0/pin/add')
NODE_GW = os.getenv('NODE_GW', 'http://65.21.201.94:8080/ipfs')

_stop = False


def _handle_stop(*_):
    global _stop
    _stop = True
    logger.info("stop requested — finishing in-flight work")


def served(cid, timeout=10):
    """True if the supernode gateway already serves this CID (fast, no proxy)."""
    try:
        r = requests.get(f"{NODE_GW}/{cid}", headers={'Range': 'bytes=0-64'},
                         timeout=timeout, stream=True)
        return r.status_code in (200, 206) and bool(next(r.iter_content(32), b''))
    except Exception:
        return False


def pin(cid, timeout=40):
    try:
        return requests.post(f"{PIN_API}?arg={cid}", timeout=timeout).ok
    except Exception:
        return False


def _parallel(func, items, workers, label):
    """Run func over items with as_completed; returns {item: result}. Progress logged."""
    out = {}
    with ThreadPoolExecutor(workers) as ex:
        futs = {ex.submit(func, it): it for it in items}
        for n, fut in enumerate(as_completed(futs)):
            out[futs[fut]] = fut.result()
            if n % 1000 == 0:
                logger.info(f"  {label}: {n}/{len(items)}")
            if _stop:
                break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--workers', type=int, default=24)
    ap.add_argument('--check', action='store_true',
                    help='pre-check the gateway and pin only missing CIDs; without '
                         'it, pin everything (faster when most are missing — as here)')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = yaml.safe_load(open(args.config))
    db = DatabaseManager(config)

    seen = set()
    for d in db.subtitles_collection.find(
            {'is_alias': {'$ne': True}}, {'subtitles': 1, '_id': 0}).batch_size(5000):
        for cid in (d.get('subtitles') or {}).values():
            if cid:
                seen.add(cid)
    cids = list(seen)
    logger.info(f"unique subtitle CIDs: {len(cids):,}")

    if args.check:
        logger.info("phase 1: checking which are already served by the supernode...")
        status = _parallel(served, cids, args.workers, "check")
        missing = [c for c, ok in status.items() if not ok]
        logger.info(f"already served: {len(cids) - len(missing):,}  |  MISSING: {len(missing):,}")
    else:
        # ~80% were found missing on inspection, so checking each (10s timeout on
        # a miss) costs more than just pinning it. pin/add is idempotent.
        missing = cids
        logger.info("skipping check — pinning all CIDs directly (idempotent)")

    if args.dry_run:
        logger.info(f"DRY-RUN — would pin {len(missing):,} CIDs")
        return 0
    if not missing or _stop:
        logger.info("nothing to pin.")
        return 0

    logger.info(f"phase 2: pinning {len(missing):,} CIDs to the supernode...")
    res = _parallel(pin, missing, args.workers, "pin")
    ok = sum(1 for v in res.values() if v)
    fail = [c for c, v in res.items() if not v]
    logger.info(f"done: pinned={ok}  failed={len(fail)} (likely GC'd from our node)")
    if fail:
        logger.info(f"first failures: {fail[:10]}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
