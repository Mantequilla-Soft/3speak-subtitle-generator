"""
Fit per-label classifier thresholds against real videos.

bart-large-mnli entailment scores are not comparable across labels: 'news'
entails almost any spoken sentence while 'cryptocurrency' entails almost none.
A single global threshold therefore over-fires the easy labels. This script
fits one floor per label.

Ground truth is the authors' own Hive tags mapped onto our taxonomy. Those tags
are withheld from the classifier input, so the model has to recover the topic
from the title and post body alone.

Caveat: authors under-tag. A cooking video tagged only 'food' still counts
'tutorial' as a false positive here, so fitted thresholds skew high. Treat the
output as a starting point, not gospel — and always eyeball `--show-errors`.

    python3 src/calibrate_tags.py --limit 140 --cache /tmp/scores.json
"""

import argparse
import json
import logging
import os
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tag_taxonomy import (  # noqa: E402
    clean_post_body, tags_from_category, tags_from_hive_tags,
)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

GRID = [round(0.30 + 0.025 * i, 3) for i in range(27)]  # 0.300 .. 0.950
MIN_THRESHOLD, MAX_THRESHOLD = 0.45, 0.90


def collect(config, limit, cache_path):
    """Score `limit` videos, caching the result so sweeps are free to re-run."""
    if cache_path and os.path.exists(cache_path):
        logger.info(f"Reusing cached scores: {cache_path}")
        return json.load(open(cache_path))

    import torch
    from pymongo import MongoClient
    from transformers import pipeline

    torch.set_num_threads(max(1, (os.cpu_count() or 4) - 2))

    labels = config['tags']
    template = config['tagging'].get('hypothesis_template', 'This video is about {}.')

    mongo = config['mongodb']
    col = MongoClient(mongo['uri'])[mongo['database']][mongo['collection_embed']]
    cursor = col.find(
        {'status': 'published',
         'hive_body': {'$exists': True, '$nin': [None, '']},
         'hive_tags': {'$exists': True, '$ne': []}},
        {'owner': 1, 'permlink': 1, 'hive_title': 1, 'hive_body': 1,
         'hive_tags': 1, 'category': 1, '_id': 0},
    )

    docs = []
    for d in cursor:
        silver = tags_from_hive_tags(d.get('hive_tags')) | tags_from_category(d.get('category'))
        if not silver:
            continue
        title = (d.get('hive_title') or '').strip()
        body = clean_post_body(d.get('hive_body') or '', config['tagging'].get('body_chars', 1200))
        if len(body) < 80 and not title:
            continue
        docs.append({
            'author': d['owner'], 'permlink': d['permlink'], 'silver': sorted(silver),
            'text': (f"Title: {title}\n" if title else '') + body,
        })
        if len(docs) >= limit:
            break

    logger.info(f"Scoring {len(docs)} videos...")
    model_cfg = config['models']['tagging']
    clf = pipeline('zero-shot-classification', model=model_cfg['model'],
                   device=-1, model_kwargs={'cache_dir': model_cfg.get('cache_dir', '/app/models')})
    for i, d in enumerate(docs):
        r = clf(d.pop('text'), candidate_labels=labels, multi_label=True,
                hypothesis_template=template, batch_size=16)
        d['scores'] = {l: round(s, 5) for l, s in zip(r['labels'], r['scores'])}
        if i and i % 20 == 0:
            logger.info(f"  {i}/{len(docs)}")

    if cache_path:
        json.dump(docs, open(cache_path, 'w'))
    return docs


def f1(tp, fp, fn):
    if tp == 0:
        return 0.0
    precision, recall = tp / (tp + fp), tp / (tp + fn)
    return 2 * precision * recall / (precision + recall)


def fit_label(docs, label):
    """Threshold maximizing this label's F1 against silver truth."""
    positives = sum(1 for d in docs if label in d['silver'])
    if positives < 3:
        # Too few examples to fit. If the label still scores high on videos that
        # are definitely not about it, clamp it shut.
        high = sum(1 for d in docs if d['scores'].get(label, 0) >= 0.7)
        return (MAX_THRESHOLD, positives, 0.0) if high > len(docs) * 0.2 else (None, positives, 0.0)

    best_t, best_f1 = None, -1.0
    for t in GRID:
        tp = sum(1 for d in docs if d['scores'].get(label, 0) >= t and label in d['silver'])
        fp = sum(1 for d in docs if d['scores'].get(label, 0) >= t and label not in d['silver'])
        fn = sum(1 for d in docs if d['scores'].get(label, 0) < t and label in d['silver'])
        score = f1(tp, fp, fn)
        if score > best_f1:
            best_t, best_f1 = t, score
    return min(max(best_t, MIN_THRESHOLD), MAX_THRESHOLD), positives, best_f1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--limit', type=int, default=140)
    ap.add_argument('--cache', default='')
    args = ap.parse_args()

    config = yaml.safe_load(open(args.config))
    docs = collect(config, args.limit, args.cache)
    if not docs:
        logger.error("No videos with silver labels found.")
        return 1

    logger.info(f"\n{'label':<16}{'n':>5}{'thresh':>9}{'F1':>8}")
    fitted = {}
    for label in config['tags']:
        t, n, score = fit_label(docs, label)
        fitted[label] = t
        logger.info(f"{label:<16}{n:>5}{('-' if t is None else f'{t:.3f}'):>9}{score:>8.2f}")

    logger.info("\nPaste into config.yaml under tagging:\n")
    logger.info("  label_thresholds:")
    for label, t in sorted(fitted.items(), key=lambda kv: -(kv[1] or 0)):
        if t is not None:
            logger.info(f"    {label}: {t:.2f}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
