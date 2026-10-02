"""
Apply the single-topic community rules to videos already in the database.

EXCLUSIVE_COMMUNITY_MAP (see tag_taxonomy.py) says that membership in certain
communities decides a video's tags outright — Music Zone is music, SkateHive is
sports, and so on. New videos get this automatically via the tagger; this script
retrofits the rule onto everything already published.

Hand-corrected videos (manual=true, set by set_tags.py) are never touched.

    python3 src/apply_community_rules.py --dry-run
    python3 src/apply_community_rules.py
"""

import argparse
import logging
import os
import re
import sys
from collections import Counter

import yaml
from pymongo import MongoClient, UpdateOne

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tag_taxonomy import EXCLUSIVE_COMMUNITY_MAP, exclusive_tags_for  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

COMMUNITY_RULE_MODEL = 'community-rule'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--dry-run', action='store_true')
    args = ap.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)
    m = config['mongodb']
    db = MongoClient(m['uri'])[m['database']]
    tags_col = db[m['collection_tags']]

    # The category/community field carries either the hive-XXXX id or the display
    # name — and the name is stored with its original capitalisation ("Music
    # Zone"), while our map keys are lowercase. A plain $in would therefore miss
    # every name-keyed record, so match names case-insensitively.
    keys = []
    for k in sorted(EXCLUSIVE_COMMUNITY_MAP):
        if k.startswith('hive-'):
            keys.append(k)
        else:
            keys.append(re.compile(f'^{re.escape(k)}$', re.IGNORECASE))

    # Videos whose tags were hand-set must not be overwritten by a bulk rule.
    locked = {(d['author'], d['permlink']) for d in
              tags_col.find({'manual': True}, {'author': 1, 'permlink': 1, '_id': 0})}
    if locked:
        logger.info(f"{len(locked)} manually-locked videos will be skipped")

    # Current tags, so we only write where something actually changes.
    current = {}
    for d in tags_col.find({}, {'author': 1, 'permlink': 1, 'tags_list': 1,
                                'tags': 1, '_id': 0}):
        t = d.get('tags_list')
        if t is None:
            t = [x for x in (d.get('tags') or '').split(',') if x]
        current[(d.get('author'), d.get('permlink'))] = t

    ops = []
    changed = unchanged = skipped_locked = 0
    per_rule = Counter()

    sources = [('embed-video', 'category'), ('videos', 'community'),
               ('videos', 'category'), ('embed-audio', 'category')]
    seen = set()

    for cname, field in sources:
        cursor = db[cname].find(
            {'status': 'published', field: {'$in': keys}},
            {'owner': 1, 'permlink': 1, field: 1, '_id': 0},
        )
        for v in cursor:
            key = (v.get('owner'), v.get('permlink'))
            if not key[0] or not key[1] or key in seen:
                continue
            seen.add(key)

            want = exclusive_tags_for(v.get(field))
            if not want:
                continue
            if key in locked:
                skipped_locked += 1
                continue

            have = current.get(key)
            # Compare as sets: only the tag content matters, not the stored order.
            if have is not None and sorted(have) == sorted(want):
                unchanged += 1
                continue

            changed += 1
            per_rule[f"{v.get(field)} -> {','.join(want)}"] += 1
            if changed <= 5:
                logger.info(f"  @{key[0]}/{key[1]}: "
                            f"{', '.join(have) if have else '(none)'} -> {', '.join(want)}")

            ops.append(UpdateOne(
                {'author': key[0], 'permlink': key[1]},
                {'$set': {
                    'author': key[0], 'permlink': key[1],
                    'tags': ','.join(want),
                    'tags_list': want,
                    'tag_evidence': want,
                    'tag_scores': {},
                    'tag_model': COMMUNITY_RULE_MODEL,
                }},
                upsert=True,
            ))

    logger.info("")
    for rule, n in per_rule.most_common():
        logger.info(f"  {rule:<34} {n}")
    logger.info("")
    logger.info(f"matched={len(seen)} changed={changed} already_correct={unchanged} "
                f"skipped_locked={skipped_locked}")

    if args.dry_run:
        logger.info("(DRY RUN — nothing written)")
        return 0

    for i in range(0, len(ops), 1000):
        tags_col.bulk_write(ops[i:i + 1000], ordered=False)
    logger.info(f"wrote {len(ops)} tag docs")
    return 0


if __name__ == '__main__':
    sys.exit(main())
