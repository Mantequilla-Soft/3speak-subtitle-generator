"""
Backfill tags_*_v2 fields from existing v1 tags — NO re-classification.

v1's vocabulary maps onto v2 almost 1:1, so the bulk of the catalog doesn't
need models at all:

  * identity ........ music, gaming, food, ... (every shared leaf)
  * vlog ............ lifestyle
  * tutorial ........ dropped (v1 always wrote the implied 'education'
                      alongside it; if it was somehow alone -> education)
  * anything else ... dropped (platform noise never made it into v1 anyway)

The classic fields are NEVER touched: the frontend keeps showing v1 tags until
the switch is flipped. Rows already carrying v2 values in the classic fields
(Phase A wrote them there) are copied across so `tags_list_v2` is the one
field a consumer ever needs.

    python3 src/migrate_v1_tags.py --dry-run
    python3 src/migrate_v1_tags.py
"""

import argparse
import logging
import os
import sys
from collections import Counter
from datetime import datetime

import yaml
from pymongo import MongoClient, UpdateOne

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tag_taxonomy_v2 as v2  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

V1_TO_V2 = {'vlog': 'lifestyle'}
DROP = {'tutorial'}          # its implied 'education' partner survives
V2_MODELS = ('v2', 'community-rule-v2')


def map_tags(tags_list):
    out = []
    for t in tags_list:
        t2 = V1_TO_V2.get(t, t)
        if t in DROP:
            continue
        if t2 in v2.TAXONOMY_V2 and t2 not in out:
            out.append(t2)
    if not out and 'tutorial' in tags_list:
        out = ['education']
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--batch', type=int, default=1000)
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))['mongodb']
    col = MongoClient(cfg['uri'], serverSelectionTimeoutMS=8000)[cfg['database']]['subtitles-tags']

    stats = Counter()
    ops = []
    now = datetime.now()

    query = {'tags': {'$nin': [None, '']},            # has v1 (or Phase-A v2) tags
             'tags_list_v2': {'$exists': False},       # not yet migrated
             'manual': {'$ne': True}}                  # hand-locked rows untouched
    for d in col.find(query, {'author': 1, 'permlink': 1, 'tags_list': 1,
                              'tags': 1, 'tag_model': 1}):
        tags_list = d.get('tags_list') or [t for t in (d.get('tags') or '').split(',') if t]
        if d.get('tag_model') in V2_MODELS:
            new, model = tags_list, d['tag_model']      # already v2 values — copy
            stats['copied_v2'] += 1
        else:
            new, model = map_tags(tags_list), 'v1-migrated'
            if not new:
                stats['unmappable'] += 1
                continue
            stats['migrated'] += 1
        ops.append(UpdateOne(
            {'_id': d['_id']},
            {'$set': {'tags_v2': ','.join(new), 'tags_list_v2': new,
                      'tag_model_v2': model, 'tagged_v2_at': now}}))
        for t in new:
            stats[f'tag:{t}'] += 1
        if len(ops) >= args.batch and not args.dry_run:
            col.bulk_write(ops, ordered=False)
            ops = []
            stats['written'] = stats['migrated'] + stats['copied_v2']
            if stats['written'] % 20000 < args.batch:
                logger.info(dict((k, v) for k, v in stats.items() if not k.startswith('tag:')))
    if ops and not args.dry_run:
        col.bulk_write(ops, ordered=False)

    logger.info(f"{'DRY-RUN ' if args.dry_run else ''}summary: "
                f"migrated={stats['migrated']:,} copied_v2={stats['copied_v2']:,} "
                f"unmappable={stats['unmappable']:,}")
    top = Counter({k[4:]: n for k, n in stats.items() if k.startswith('tag:')})
    logger.info(f"v2 tag distribution (top 15): {dict(top.most_common(15))}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
