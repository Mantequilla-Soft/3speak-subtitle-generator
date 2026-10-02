"""
Fit per-leaf classifier thresholds for the v2 faceted taxonomy.

Same idea as calibrate_tags.py, adapted to v2:
  * candidate labels are the 25 v2 LEAVES, phrased via tag_taxonomy_v2.LEAF_PROMPTS
    (so the tagger and calibrator score identical text).
  * silver truth is the authors' own Hive tags mapped through v2's richer map,
    withheld from the classifier input.
  * pulled from BOTH `embed-video` and legacy `videos` so the new leaves
    (programming, politics, religion, automotive, ...) actually get positives.
  * F-beta with beta=0.5 (precision-weighted): a wrong tag costs more than a
    missing one — this is what calibrate_tags.py's docstring intended.
  * additionally fits the single category-fallback threshold used by
    tag_taxonomy_v2.resolve_tags (score of a category = its best leaf score).

Two-step so re-fitting is free after one expensive scoring pass:
    # 1. collect + score (needs bart; run in a model container)
    python3 src/calibrate_tags_v2.py --limit 400 --cache /app/models/scores_v2.json
    # 2. re-fit from cache instantly, tweak --beta, eyeball --show-errors
    python3 src/calibrate_tags_v2.py --cache /app/models/scores_v2.json --beta 0.5

Collection alone (no bart) can be sanity-checked anywhere with --collect-only.
"""

import argparse
import json
import logging
import os
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tag_taxonomy_v2 as v2  # noqa: E402
from tag_taxonomy import clean_post_body  # noqa: E402
from video_meta import normalize_video_metadata  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

GRID = [round(0.30 + 0.025 * i, 3) for i in range(27)]  # 0.300 .. 0.950
MIN_THRESHOLD, MAX_THRESHOLD = 0.45, 0.90

# (collection, extra query so the doc has usable text + author tags)
_SOURCES = [
    ('collection_embed', 'embed-video', {
        'status': 'published',
        'hive_body': {'$exists': True, '$nin': [None, '']},
        'hive_tags': {'$exists': True, '$ne': []},
    }),
    ('collection_videos', 'videos', {
        'status': 'published',
        'tags': {'$exists': True, '$nin': [None, '', []]},
    }),
]


def _silver(meta):
    """v2 silver leaves from a normalized doc's author tags + community."""
    return (v2.tags_from_hive_tags(meta['hive_tags'])
            | v2.tags_from_category(meta['category']))


def collect(config, limit, cache_path, collect_only=False):
    """Score up to `limit` videos (half from each source), caching the result."""
    if cache_path and os.path.exists(cache_path) and not collect_only:
        logger.info(f"Reusing cached scores: {cache_path}")
        return json.load(open(cache_path))

    from pymongo import MongoClient
    mongo = config['mongodb']
    db = MongoClient(mongo['uri'], serverSelectionTimeoutMS=8000)[mongo['database']]
    body_chars = config['tagging'].get('body_chars', 1200)

    docs = []
    per_source = max(1, limit // len(_SOURCES))
    for cfg_key, default_name, query in _SOURCES:
        name = mongo.get(cfg_key, default_name)
        got = 0
        for d in db[name].find(query):
            meta = normalize_video_metadata(d)
            silver = _silver(meta)
            if not silver:
                continue
            title, body = meta['title'], clean_post_body(meta['body'], body_chars)
            if len(body) < 80 and not title:
                continue
            docs.append({
                'author': d.get('owner'), 'permlink': d.get('permlink'),
                'source': default_name, 'silver': sorted(silver),
                'text': (f"Title: {title}\n" if title else '') + body,
            })
            got += 1
            if got >= per_source:
                break
        logger.info(f"  collected {got} from {default_name}")

    if collect_only:
        return docs

    import torch
    from transformers import pipeline
    torch.set_num_threads(max(1, (os.cpu_count() or 4) - 2))

    leaves = sorted(v2.LEAVES)
    prompts = [v2.LEAF_PROMPTS[l] for l in leaves]
    prompt_to_leaf = {v2.LEAF_PROMPTS[l]: l for l in leaves}
    template = config['tagging'].get('hypothesis_template', 'This video is about {}.')

    logger.info(f"Scoring {len(docs)} videos over {len(leaves)} leaves...")
    model_cfg = config['models']['tagging']
    # HF_MODEL_CACHE lets a host run point at the local model cache without
    # editing the shared config.yaml the container depends on.
    cache_dir = os.environ.get('HF_MODEL_CACHE') or model_cfg.get('cache_dir', '/app/models')
    clf = pipeline('zero-shot-classification', model=model_cfg['model'], device=-1,
                   model_kwargs={'cache_dir': cache_dir})
    for i, d in enumerate(docs):
        r = clf(d.pop('text'), candidate_labels=prompts, multi_label=True,
                hypothesis_template=template, batch_size=16)
        d['scores'] = {prompt_to_leaf[lab]: round(s, 5)
                       for lab, s in zip(r['labels'], r['scores'])}
        if i and i % 25 == 0:
            logger.info(f"  {i}/{len(docs)}")

    if cache_path:
        json.dump(docs, open(cache_path, 'w'))
        logger.info(f"Cached scores -> {cache_path}")
    return docs


def fbeta(tp, fp, fn, beta):
    if tp == 0:
        return 0.0
    p, r = tp / (tp + fp), tp / (tp + fn)
    if p == 0 and r == 0:
        return 0.0
    b2 = beta * beta
    denom = b2 * p + r
    return (1 + b2) * p * r / denom if denom else 0.0


def fit_label(docs, label, beta):
    """Threshold maximizing this leaf's F-beta against silver truth."""
    positives = sum(1 for d in docs if label in d['silver'])
    if positives < 3:
        high = sum(1 for d in docs if d['scores'].get(label, 0) >= 0.7)
        return (MAX_THRESHOLD, positives, 0.0) if high > len(docs) * 0.2 else (None, positives, 0.0)
    best_t, best = None, -1.0
    for t in GRID:
        tp = sum(1 for d in docs if d['scores'].get(label, 0) >= t and label in d['silver'])
        fp = sum(1 for d in docs if d['scores'].get(label, 0) >= t and label not in d['silver'])
        fn = sum(1 for d in docs if d['scores'].get(label, 0) < t and label in d['silver'])
        s = fbeta(tp, fp, fn, beta)
        if s > best:
            best_t, best = t, s
    return min(max(best_t, MIN_THRESHOLD), MAX_THRESHOLD), positives, best


def fit_category(docs, beta):
    """
    One threshold for the leaf->category fallback. A category is 'present' in
    silver if any of its leaves is; its score is the max score over its leaves.
    """
    def cats(silver):
        return {v2.category_of(l) for l in silver}
    best_t, best = None, -1.0
    for t in GRID:
        tp = fp = fn = 0
        for d in docs:
            truth = cats(d['silver'])
            for cat, leaves in v2.CATEGORY_TREE.items():
                score = max((d['scores'].get(l, 0) for l in leaves), default=0)
                pred, real = score >= t, cat in truth
                tp += pred and real
                fp += pred and not real
                fn += (not pred) and real
        s = fbeta(tp, fp, fn, beta)
        if s > best:
            best_t, best = t, s
    return min(max(best_t, MIN_THRESHOLD), MAX_THRESHOLD), best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--limit', type=int, default=400)
    ap.add_argument('--cache', default='')
    ap.add_argument('--beta', type=float, default=0.5, help='F-beta; <1 favors precision')
    ap.add_argument('--collect-only', action='store_true', help='no bart; dump silver coverage')
    ap.add_argument('--show-errors', type=int, default=0, help='print N worst false positives')
    args = ap.parse_args()

    config = yaml.safe_load(open(args.config))
    docs = collect(config, args.limit, args.cache, args.collect_only)
    if not docs:
        logger.error("No videos with silver labels found.")
        return 1

    if args.collect_only:
        from collections import Counter
        c = Counter(l for d in docs for l in d['silver'])
        logger.info(f"\n{len(docs)} docs. silver positives per leaf:")
        for leaf in sorted(v2.LEAVES):
            logger.info(f"  {leaf:15s}{c.get(leaf, 0):>5}")
        missing = [l for l in v2.LEAVES if c.get(l, 0) < 3]
        if missing:
            logger.info(f"\n<3 positives (will fall back to default): {sorted(missing)}")
        return 0

    logger.info(f"\n(beta={args.beta})\n{'leaf':<16}{'n':>5}{'thresh':>9}{'F':>8}")
    fitted = {}
    for leaf in sorted(v2.LEAVES):
        t, n, score = fit_label(docs, leaf, args.beta)
        fitted[leaf] = t
        logger.info(f"{leaf:<16}{n:>5}{('-' if t is None else f'{t:.3f}'):>9}{score:>8.2f}")

    cat_t, cat_f = fit_category(docs, args.beta)
    logger.info(f"\ncategory-fallback threshold: {cat_t:.3f}  (F={cat_f:.2f})")

    logger.info("\n--- paste into config.yaml (tagging.v2) ---\n")
    logger.info("  label_thresholds_v2:")
    for leaf, t in sorted(fitted.items(), key=lambda kv: -(kv[1] or 0)):
        if t is not None:
            logger.info(f"    {leaf}: {t:.2f}")
    logger.info(f"  category_threshold_v2: {cat_t:.2f}")

    if args.show_errors:
        logger.info(f"\n--- {args.show_errors} highest-scoring silver-negative predictions ---")
        rows = []
        for d in docs:
            for leaf, sc in d.get('scores', {}).items():
                thr = fitted.get(leaf) or MAX_THRESHOLD
                if sc >= thr and leaf not in d['silver']:
                    rows.append((sc, leaf, d['author'], d['permlink'], d['silver']))
        for sc, leaf, a, p, silver in sorted(rows, reverse=True)[:args.show_errors]:
            logger.info(f"  {sc:.2f} {leaf:13s} {a}/{p}  silver={silver}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
