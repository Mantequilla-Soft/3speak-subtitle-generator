"""
Tag videos that were never transcribed, from their Hive metadata alone.

The subtitle pipeline only runs on videos newer than START_DATE, so the entire
pre-cutoff back-catalog (~134k legacy videos) has no tags. But almost all of it
carries a title, a description and Hive tags — enough for the evidence layer plus
a title/body classification, no transcript required.

Design goals:
  * newest-first, in date windows, so the most relevant videos are tagged first
    and the load on Mongo / CPU is spread out in bounded steps rather than one
    giant scan;
  * resumable — already-tagged videos are skipped, so a restart continues where
    it stopped;
  * graceful stop on SIGINT/SIGTERM (finishes the current video first).

    python3 src/tag_metadata.py --dry-run --window-days 30 --max-windows 1
    python3 src/tag_metadata.py --window-days 30 --fetch-missing
    python3 src/tag_metadata.py --until-date 2023-01-01   # stop at a floor
"""

import argparse
import logging
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timedelta

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_manager import DatabaseManager  # noqa: E402
from tag_live_v2 import tag_video_v2  # noqa: E402
from hive_client import fetch_hive_post  # noqa: E402
from tag_taxonomy import (  # noqa: E402
    apply_implications, exclusive_tags_for, tags_from_category, tags_from_hive_tags,
)
from video_meta import hive_reference, merge_hive_post, normalize_video_metadata  # noqa: E402

# Marks a tag doc written without running the classifier, so a later pass can
# find and enrich these if we ever want to.
EVIDENCE_ONLY_MODEL = 'evidence-only'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('tagger').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# (attribute on DatabaseManager, date field, video_type) per source collection.
COLLECTIONS = [
    ('embed_collection', 'createdAt', 'embed'),
    ('embed_audio_collection', 'createdAt', 'audio'),
    ('videos_collection', 'created', 'legacy'),
]

_stop = False


def _request_stop(signum, _frame):
    global _stop
    _stop = True
    logger.warning(f"signal {signum} received — finishing current video, then exiting")


def load_tagged_keys(db):
    """Every (author, permlink) that already has a tag doc. Small relative to the catalog."""
    keys = set()
    for d in db.tags_collection.find({}, {'author': 1, 'permlink': 1, '_id': 0}):
        keys.add((d['author'], d['permlink']))
    return keys


def usable_metadata(meta):
    """True if there is anything worth classifying: mapped evidence, a title, or a body."""
    ev = tags_from_hive_tags(meta['hive_tags']) | tags_from_category(meta['category'])
    return bool(ev or meta['title'] or len(meta['body']) > 50)


def build_metadata(db, video, allow_fetch, fetch_sleep):
    """Normalized metadata, topped up from the Hive post for thin embed docs."""
    meta = normalize_video_metadata(video)
    if meta['title'] and meta['body'] and meta['hive_tags']:
        return meta
    if not allow_fetch:
        return meta
    author, permlink = hive_reference(video)
    if not author or not permlink:
        return meta
    post = db.get_hive_meta(author, permlink)
    if post is None:
        post = fetch_hive_post(author, permlink)
        if post:
            db.save_hive_meta(author, permlink, post)
        time.sleep(fetch_sleep)
    return merge_hive_post(meta, post) if post else meta


def _tag_v2(db, config, tagger, video, meta):
    """
    Write v2 tags alongside the v1 ones (see TAXONOMY_V2.md).

    This path never transcribes and has no local video file, so there is no
    vision here — creator/community rules, the author's Hive tags, and a
    title/body classification only. Anything it cannot decide is left for the
    background tagger-v2 service, which does have vision.

    Strictly additive: any failure is logged and never disturbs v1 tagging.
    """
    try:
        tag_video_v2(
            db, video['owner'], video['permlink'],
            metadata=meta,
            classifier=getattr(tagger, 'classifier', None),
            text=(tagger.build_classifier_input(meta, '') if tagger else ''),
            config=config,
        )
    except Exception as e:
        logger.error(f"  v2 tagging failed (non-fatal): {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--window-days', type=int, default=30,
                    help='size of each date step, newest first')
    ap.add_argument('--start-date', default='',
                    help='newest boundary YYYY-MM-DD (default: today)')
    ap.add_argument('--until-date', default='',
                    help='stop once windows pass below this YYYY-MM-DD (default: all history)')
    ap.add_argument('--max-windows', type=int, default=0,
                    help='process at most this many windows then exit (0 = no cap)')
    ap.add_argument('--collections', default='embed,audio,legacy',
                    help='comma list: embed,audio,legacy')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--fetch-missing', action='store_true',
                    help='fetch Hive posts for embed videos with thin local metadata')
    ap.add_argument('--fetch-sleep', type=float, default=0.2)
    ap.add_argument('--sleep', type=float, default=0.0,
                    help='pause between videos to ease CPU/Mongo load')
    ap.add_argument('--evidence-only', action='store_true',
                    help="tag only from the author's Hive tags / community — no "
                         "classifier, no model load. Orders of magnitude faster. "
                         "Videos with no mappable Hive tags are left untagged for "
                         "a later classifier pass.")
    ap.add_argument('--progress-every', type=int, default=100)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    with open(args.config) as f:
        config = yaml.safe_load(f)
    db = DatabaseManager(config)

    # Import/instantiate the classifier only when it will actually be used —
    # evidence-only mode must not pay the ~30s model load or the memory.
    tagger = None
    max_tags = config['tagging']['max_tags']
    if not args.evidence_only:
        from tagger import ContentTagger
        tagger = ContentTagger(config)

    wanted = {c.strip() for c in args.collections.split(',') if c.strip()}
    sources = [(attr, field, vt) for attr, field, vt in COLLECTIONS if vt in wanted]

    start = (datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date
             else datetime.now())
    until = (datetime.strptime(args.until_date, '%Y-%m-%d') if args.until_date
             else datetime(2016, 1, 1))  # 3Speak predates this; effectively "all"

    logger.info("Loading already-tagged keys...")
    tagged = load_tagged_keys(db)
    logger.info(f"{len(tagged)} videos already tagged; starting at {start.date()}, "
                f"stepping back {args.window_days}d until {until.date()}"
                f"{'  (dry run)' if args.dry_run else ''}")

    before, after = Counter(), Counter()
    processed = tagged_new = skipped_thin = skipped_done = 0
    windows = 0
    run_started = time.time()
    w_end = start

    while w_end > until and not _stop:
        w_start = max(w_end - timedelta(days=args.window_days), until)
        windows += 1
        win_count = 0

        for attr, field, vtype in sources:
            if _stop:
                break
            col = getattr(db, attr)
            cursor = col.find(
                {'status': 'published', field: {'$gte': w_start, '$lt': w_end}},
                {'owner': 1, 'permlink': 1, 'hive_title': 1, 'title': 1,
                 'hive_body': 1, 'description': 1, 'hive_tags': 1, 'tags': 1,
                 'category': 1, 'community': 1, 'hive_author': 1, 'hive_permlink': 1,
                 'embed_url': 1, 'originalFilename': 1, '_id': 0},
            )
            for video in cursor:
                if _stop:
                    break
                key = (video.get('owner'), video.get('permlink'))
                if key in tagged:
                    skipped_done += 1
                    continue
                try:
                    video['_video_type'] = vtype
                    meta = build_metadata(db, video, args.fetch_missing, args.fetch_sleep)

                    if args.evidence_only:
                        # Single-topic community decides outright — same rule the
                        # classifier path applies, so both stay consistent.
                        exclusive = exclusive_tags_for(meta['category'])
                        if exclusive:
                            new_tags = exclusive[:max_tags]
                            result = {'tags': new_tags, 'evidence': new_tags,
                                      'scores': {}, 'model': 'community-rule'}
                            after.update(new_tags)
                            processed += 1
                            win_count += 1
                            tagged.add(key)
                            if not args.dry_run:
                                db.save_tags(video['owner'], video['permlink'],
                                             new_tags, scores={}, evidence=new_tags,
                                             model='community-rule')
                                _tag_v2(db, config, tagger, video, meta)
                                tagged_new += 1
                            continue

                        # Pure dictionary lookup: the author's own Hive tags and
                        # community. No model, no inference.
                        ev = (tags_from_hive_tags(meta['hive_tags'])
                              | tags_from_category(meta['category']))
                        if not ev:
                            # Nothing mappable. Leave it untagged (write nothing) so
                            # the later classifier pass still picks it up.
                            skipped_thin += 1
                            continue
                        new_tags = apply_implications(sorted(ev))[:max_tags]
                        result = {'tags': new_tags, 'evidence': sorted(ev),
                                  'scores': {}, 'model': EVIDENCE_ONLY_MODEL}
                    else:
                        if not usable_metadata(meta):
                            skipped_thin += 1
                            tagged.add(key)  # don't revisit an untaggable video
                            continue
                        result = tagger.generate_tags(metadata=meta, content_text='')
                        new_tags = result['tags']

                    after.update(new_tags)
                    processed += 1
                    win_count += 1
                    tagged.add(key)

                    if not args.dry_run:
                        db.save_tags(video['owner'], video['permlink'], new_tags,
                                     scores=result['scores'], evidence=result['evidence'],
                                     model=result['model'])
                        _tag_v2(db, config, tagger, video, meta)
                        tagged_new += 1

                    if processed % args.progress_every == 0:
                        rate = processed / (time.time() - run_started)
                        logger.info(f"  ...{processed} tagged ({rate * 60:.0f}/min), "
                                    f"window {w_start.date()}..{w_end.date()}")
                    if args.sleep:
                        time.sleep(args.sleep)
                except Exception as e:
                    logger.error(f"@{key[0]}/{key[1]} failed: {e}")
                    continue

        logger.info(f"window {w_start.date()}..{w_end.date()}: tagged {win_count}")
        w_end = w_start
        if args.max_windows and windows >= args.max_windows:
            logger.info(f"reached --max-windows {args.max_windows}, stopping")
            break

    elapsed = (time.time() - run_started) / 60
    logger.info(f"\nwindows={windows} tagged={processed} written={tagged_new} "
                f"skipped_untaggable={skipped_thin} skipped_already_done={skipped_done} "
                f"oldest_window={w_end.date()} elapsed={elapsed:.0f}min"
                f"{'  (DRY RUN — nothing written)' if args.dry_run else ''}"
                f"{'  (STOPPED — rerun to resume; done videos are skipped)' if _stop else ''}")

    if processed:
        logger.info(f"\n{'tag':<16}{'count':>8}{'% of tagged':>13}")
        for tag, n in after.most_common():
            logger.info(f"{tag:<16}{n:>8}{100 * n / processed:>12.1f}%")
    return 0


if __name__ == '__main__':
    sys.exit(main())
