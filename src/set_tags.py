"""
Manually set the tags for one video — and lock them so the auto-taggers never
overwrite the correction.

Accepts whatever form of the video you have to hand: a watch URL, an
@author/permlink, or the Hive permlink from the URL (which is usually NOT the
permlink the video is stored under — a-wolverine-302820 is stored as ofj22log).

    python3 src/set_tags.py https://preview.3speak.tv/watch?v=meno/a-wolverine-302820 gaming
    python3 src/set_tags.py meno/ofj22log art,tutorial
    python3 src/set_tags.py @bullravi/bayek-returns-to-yamu-434 gaming --note "clearly AC gameplay"
    python3 src/set_tags.py meno/ofj22log --clear          # set to no tags, still locked
    python3 src/set_tags.py meno/ofj22log --unlock         # hand back to the auto-tagger

Locked docs carry manual=true and tag_model='manual'. retag.py skips them.
"""

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timezone

import yaml
from pymongo import MongoClient

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tag_taxonomy import TAXONOMY, apply_implications  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

MANUAL_MODEL = 'manual'

_URL_RE = re.compile(r'[?&]v=([^&\s]+)')


def parse_ref(ref: str):
    """Pull (author, permlink) out of a watch URL, @author/permlink, or author/permlink."""
    ref = (ref or '').strip()
    m = _URL_RE.search(ref)
    if m:
        ref = m.group(1)
    ref = ref.lstrip('@')
    if '/' not in ref:
        raise ValueError(f"cannot parse video reference: {ref!r}")
    author, permlink = ref.split('/', 1)
    return author.strip(), permlink.strip().strip('/')


def resolve(db, author, permlink):
    """
    Find the video, accepting either its stored permlink or its hive_permlink.

    Returns (author, stored_permlink, collection_name) or (None, None, None).
    """
    for cname in ('embed-video', 'videos', 'embed-audio'):
        col = db[cname]
        v = col.find_one({'owner': author, 'permlink': permlink},
                         {'owner': 1, 'permlink': 1})
        if v:
            return v['owner'], v['permlink'], cname
        # The URL usually carries the Hive permlink, not the stored one.
        v = col.find_one({'owner': author, 'hive_permlink': permlink},
                         {'owner': 1, 'permlink': 1})
        if v:
            return v['owner'], v['permlink'], cname
    return None, None, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('video', help='watch URL, @author/permlink, or author/permlink')
    ap.add_argument('tags', nargs='?', default='',
                    help='comma-separated tags, e.g. gaming,music')
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--note', default='', help='why (stored for future reference)')
    ap.add_argument('--clear', action='store_true', help='set to no tags (still locked)')
    ap.add_argument('--unlock', action='store_true',
                    help='remove the manual lock, letting the auto-tagger own it again')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)
    m = config['mongodb']
    db = MongoClient(m['uri'])[m['database']]

    author, permlink = parse_ref(args.video)
    owner, stored, cname = resolve(db, author, permlink)
    if not owner:
        logger.error(f"video not found: @{author}/{permlink}")
        return 1
    if stored != permlink:
        logger.info(f"resolved hive permlink {permlink!r} -> stored {stored!r}")

    tags_col = db[m['collection_tags']]
    existing = tags_col.find_one({'author': owner, 'permlink': stored}) or {}
    old = existing.get('tags_list') or [
        t for t in (existing.get('tags') or '').split(',') if t]

    if args.unlock:
        if args.dry_run:
            logger.info(f"[dry-run] would unlock @{owner}/{stored}")
            return 0
        tags_col.update_one(
            {'author': owner, 'permlink': stored},
            {'$unset': {'manual': '', 'manual_note': '', 'manual_at': ''},
             '$set': {'tag_model': None}},
        )
        logger.info(f"unlocked @{owner}/{stored} — the auto-tagger may retag it "
                    f"(currently: {', '.join(old) or '(none)'})")
        return 0

    if args.clear:
        new = []
    else:
        raw = [t.strip().lower() for t in args.tags.split(',') if t.strip()]
        if not raw:
            logger.error("no tags given (use --clear to deliberately set none)")
            return 1
        unknown = [t for t in raw if t not in TAXONOMY]
        if unknown:
            logger.error(f"not in the taxonomy: {', '.join(unknown)}")
            logger.error(f"valid tags: {', '.join(sorted(TAXONOMY))}")
            return 1
        # 'tutorial' implies 'education', same as the auto-tagger.
        new = apply_implications(raw)

    logger.info(f"@{owner}/{stored}  [{cname}]")
    logger.info(f"  old: {', '.join(old) or '(none)'}")
    logger.info(f"  new: {', '.join(new) or '(none)'}   [locked: auto-tagger will skip]")

    if args.dry_run:
        logger.info("[dry-run] nothing written")
        return 0

    doc = {
        'author': owner,
        'permlink': stored,
        'tags': ','.join(new),      # legacy comma string — the dashboard reads this
        'tags_list': new,
        'tag_evidence': [],
        'tag_scores': {},
        'tag_model': MANUAL_MODEL,
        'manual': True,             # retag.py skips these
        'manual_note': args.note,
        'manual_at': datetime.now(timezone.utc).replace(tzinfo=None),
    }
    tags_col.update_one({'author': owner, 'permlink': stored},
                        {'$set': doc}, upsert=True)
    logger.info("saved.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
