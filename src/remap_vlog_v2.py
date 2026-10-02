"""
One-off: move migrated v1 `vlog` tags from `lifestyle` back to `vlog`.

When v2 first launched, `vlog` was dropped as a topic (it is a format, not a
subject) and migrate_v1_tags.py sent v1's `vlog` to `lifestyle`. `vlog` has
since been reinstated as an EVIDENCE-ONLY leaf — assigned when an author or a
community declares it, never guessed by a model — so those rows should say
`vlog` again.

Only rows where v1 *itself* said `vlog` are touched, so this cannot invent the
tag. Only the *_v2 fields change: the classic v1 fields (what the frontend
still reads) are never modified, and manual locks are skipped.

    python3 src/remap_vlog_v2.py --dry-run
    python3 src/remap_vlog_v2.py
"""

import argparse
import logging
import sys

import yaml
from pymongo import MongoClient, UpdateOne

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--batch', type=int, default=1000)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))['mongodb']
    col = MongoClient(cfg['uri'], serverSelectionTimeoutMS=8000)[cfg['database']]['subtitles-tags']

    query = {
        'tag_model_v2': 'v1-migrated',   # only rows created by the bulk migration
        'tags_list': 'vlog',             # v1 itself said vlog
        'tags_list_v2': 'lifestyle',     # and we mapped it to lifestyle
        'manual': {'$ne': True},         # never touch hand-corrected rows
    }

    ops, changed, samples = [], 0, []
    for d in col.find(query, {'_id': 1, 'author': 1, 'permlink': 1, 'tags_list_v2': 1}):
        new = ['vlog' if t == 'lifestyle' else t for t in d.get('tags_list_v2') or []]
        # de-dupe in case the row already carried both
        seen, deduped = set(), []
        for t in new:
            if t not in seen:
                seen.add(t)
                deduped.append(t)
        ops.append(UpdateOne({'_id': d['_id']},
                             {'$set': {'tags_list_v2': deduped,
                                       'tags_v2': ','.join(deduped)}}))
        changed += 1
        if len(samples) < 5:
            samples.append(f"{d['author']}/{d['permlink']}: "
                           f"{d.get('tags_list_v2')} -> {deduped}")
        if len(ops) >= args.batch and not args.dry_run:
            col.bulk_write(ops, ordered=False)
            ops = []
    if ops and not args.dry_run:
        col.bulk_write(ops, ordered=False)

    logger.info(f"{'DRY-RUN ' if args.dry_run else ''}rows remapped lifestyle -> vlog: {changed:,}")
    for s in samples:
        logger.info(f"  e.g. {s}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
