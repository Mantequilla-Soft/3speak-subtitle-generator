"""
Re-pin subtitle CIDs to the public IPFS gateway.

Why this exists: a subtitle is added to our LOCAL IPFS node and then pinned on
the public gateway. When that remote pin fails, the CID is local-only — the
language is listed but returns 0 bytes to viewers. The remote pin endpoint has
been seen hanging (accepts the TCP connection, never completes), which silently
half-publishes videos.

Modes:
  --from-failures (default): re-pin every CID a doc recorded in remote_pin_failed
      (main.py writes those). Verifies the gateway now serves it and clears the
      flag. This is the ongoing recovery job — safe to cron.
  --verify [--since YYYY-MM-DD]: probe the gateway for EVERY subtitle CID and,
      for any it cannot serve, attempt a pin and record the outcome. Use once to
      discover the backlog that predates failure tracking.

Idempotent and resumable: already-served CIDs are skipped, successes clear the
flag, and it never touches the video/source collections.

    python3 src/repin_subtitles.py --dry-run
    python3 src/repin_subtitles.py                          # retry known failures
    python3 src/repin_subtitles.py --verify --since 2026-07-01
"""

import argparse
import logging
import os
import signal
import sys
import time

import requests
import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_manager import DatabaseManager  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

_stop = False


def _handle_stop(*_):
    global _stop
    _stop = True
    logger.info("stop requested — finishing current item")


def gateway_serves(gateway: str, cid: str, timeout: int = 12) -> bool:
    """True if the public gateway returns any bytes for this CID."""
    try:
        r = requests.get(f"{gateway.rstrip('/')}/{cid}",
                         headers={'Range': 'bytes=0-256'}, timeout=timeout, stream=True)
        return r.status_code in (200, 206) and bool(next(r.iter_content(64), b''))
    except Exception:
        return False


def remote_pin(url: str, cid: str, timeout: int = 45) -> bool:
    """Ask the remote node to pin the CID. False on any failure/timeout."""
    try:
        r = requests.post(f"{url}?arg={cid}", timeout=timeout)
        return r.ok
    except Exception as e:
        logger.debug(f"pin failed {cid}: {e}")
        return False


def _video_type(doc: dict) -> str:
    return 'audio' if doc.get('isAudio') else 'embed' if doc.get('isEmbed') else 'legacy'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--verify', action='store_true',
                    help='probe every CID and record ones the gateway cannot serve')
    ap.add_argument('--since', default=None, help='verify only docs updated on/after YYYY-MM-DD')
    ap.add_argument('--author', default=None)
    ap.add_argument('--permlink', default=None)
    ap.add_argument('--remote-url',
                    default=os.getenv('REMOTE_PIN_URL', 'https://ipfs.3speak.tv/api/v0/pin/add'))
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--sleep', type=float, default=0.2)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = yaml.safe_load(open(args.config))
    gateway = config['ipfs']['gateways'][0]
    db = DatabaseManager(config)
    S = db.subtitles_collection

    stats = {'checked': 0, 'pinned': 0, 'still_failing': 0, 'already_ok': 0, 'recorded': 0}

    # Build the work list of (author, permlink, video_type, [(lang, cid)]).
    query = {'is_alias': {'$ne': True}}
    if args.author:
        query['author'] = args.author
    if args.permlink:
        query['permlink'] = args.permlink
    if args.verify:
        if args.since:
            from datetime import datetime
            query['updated_at'] = {'$gte': datetime.fromisoformat(args.since)}
    else:
        query['remote_pin_failed.0'] = {'$exists': True}   # has at least one failure

    for doc in S.find(query, {'author': 1, 'permlink': 1, 'subtitles': 1,
                              'remote_pin_failed': 1, 'isEmbed': 1, 'isAudio': 1}):
        if _stop:
            break
        author, permlink = doc['author'], doc['permlink']
        vtype = _video_type(doc)

        if args.verify:
            items = list((doc.get('subtitles') or {}).items())
        else:
            items = [(e['lang'], e['cid']) for e in doc.get('remote_pin_failed') or []]

        for lang, cid in items:
            if _stop:
                break
            stats['checked'] += 1
            if gateway_serves(gateway, cid):
                stats['already_ok'] += 1
                if not args.dry_run and not args.verify:
                    db.record_remote_pin(author, permlink, lang, cid, True, vtype)
                continue
            # not on the gateway -> (re)pin
            if args.dry_run:
                logger.info(f"DRY would pin {author}/{permlink} [{lang}] {cid}")
                stats['recorded'] += 1
                continue
            ok = remote_pin(args.remote_url, cid)
            if ok and gateway_serves(gateway, cid, timeout=20):
                stats['pinned'] += 1
                db.record_remote_pin(author, permlink, lang, cid, True, vtype)
                logger.info(f"pinned {author}/{permlink} [{lang}] {cid}")
            else:
                stats['still_failing'] += 1
                # record the failure so a later run retries (verify mode discovers new ones)
                db.record_remote_pin(author, permlink, lang, cid, False, vtype)
                stats['recorded'] += 1
            if args.sleep:
                time.sleep(args.sleep)

    logger.info(f"done: {stats}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
