"""
v2 fusion tagger — writes faceted-taxonomy tags for the untagged backlog.

Fuses four layers in trust order (TAXONOMY_V2.md):

  1. EXCLUSIVE community rule ........ decides outright, nothing else runs
  2. evidence (author Hive tags + community) ....... always included
  3. vision (CLIP on 3 frames + format prior) ...... when the file is fetchable
  4. text classifier (bart on title/body) .......... when there is enough text

Eligibility (both enforced here):
  * embed videos without a hive_permlink are orphans -> skipped
  * creators with contentcreators.hidden=true -> skipped

Writes through db_manager.save_tags() with tag_model='v2': manual locks are
respected there, and re-running skips anything that already got v2 tags.
Videos where no layer produces a tag are written as an EMPTY v2 row only with
--mark-empty (default off), so a later pass can retry them cheaply.

    python3 src/tag_videos_v2.py --dry-run --limit 30        # eyeball first
    python3 src/tag_videos_v2.py --limit 500                 # real run, bounded
    python3 src/tag_videos_v2.py --no-vision                 # text/evidence only
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tag_taxonomy_v2 as v2  # noqa: E402
from db_manager import DatabaseManager  # noqa: E402
from tag_taxonomy import clean_post_body  # noqa: E402
from video_meta import normalize_video_metadata  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MODEL_MARKER = 'v2'
GW = 'https://ipfs.3speak.tv/ipfs/'
MAX_TAGS = 5
MIN_TEXT_CHARS = 80
VISION_LEAF_RATIO = 2.0       # accept a leaf when top1/top2 clears this
VISION_CATEGORY_MASS = 0.50   # else accept a category when sibling mass clears this
FORMAT_MIN_PROB = 0.55        # format prior fires only above this

_stop = False


def _handle_stop(signum, frame):
    global _stop
    _stop = True
    logger.info("stop requested — finishing current video")


# ── vision ────────────────────────────────────────────────────────────────────
class VisionTagger:
    """CLIP over 3 frames: topic decision + format prior. Lazy model load."""

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.model = None
        # evidence-only leaves (e.g. 'vlog') are never offered to the model
        self.leaves = sorted(v2.LEAVES - v2.EVIDENCE_ONLY_LEAVES)
        vp = v2.vision_prompts()
        self.topic_prompts = [f"a video about {vp[l]}" for l in self.leaves]
        # Score the FULL format list, not just the 5 mapped ones: without
        # distractors (talking-head, vlog, ...) the softmax renormalizes over
        # too few options and everything clears the floor. Only the mapped
        # formats ever ACT (via FORMAT_TO_TOPIC).
        self.format_prompts = {
            'talking-head': 'a person talking directly into the camera',
            'vlog': 'a handheld personal vlog',
            'interview': 'two or more people in a conversation or interview',
            'documentary': 'documentary footage with narration',
            'tutorial': 'an instructional demonstration with hands and materials',
            'screencast': 'a computer screen recording',
            'presentation': 'a slide presentation or lecture',
            'gameplay': 'a video game screen capture with game graphics',
            'music-performance': 'a person performing music with an instrument or singing',
            'music-video': 'a produced music video',
            'animation': 'an animated cartoon or rendered animation',
        }

    def _load(self):
        # Guard on BOTH handles: if a previous attempt set the model but the
        # processor load then failed, we must retry — not skip and later blow up
        # with AttributeError('proc'). Assign to locals and publish only once
        # both succeed, so the tagger is never left half-initialized.
        if self.model is not None and self.proc is not None:
            return
        from transformers import CLIPModel, CLIPProcessor
        try:
            proc = CLIPProcessor.from_pretrained(
                'openai/clip-vit-base-patch32', cache_dir=self.cache_dir)
            model = CLIPModel.from_pretrained(
                'openai/clip-vit-base-patch32', cache_dir=self.cache_dir)
        except OSError:
            logger.warning("CLIP not loadable from %s — using default HF cache",
                           self.cache_dir)
            proc = CLIPProcessor.from_pretrained('openai/clip-vit-base-patch32')
            model = CLIPModel.from_pretrained('openai/clip-vit-base-patch32')
        model.eval()
        self.proc, self.model = proc, model

    def frames(self, url, workdir):
        """3 frames at 20/50/80%, or None if the file isn't fetchable."""
        try:
            dur = float(subprocess.run(
                ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                 '-of', 'csv=p=0', url],
                capture_output=True, text=True, timeout=30).stdout.strip() or 0)
        except Exception:
            return None, 0
        if dur < 3:
            return None, dur
        paths = []
        for j, frac in enumerate([0.2, 0.5, 0.8]):
            out = os.path.join(workdir, f'f{j}.jpg')
            try:
                r = subprocess.run(
                    ['ffmpeg', '-y', '-v', 'error', '-ss', str(dur * frac), '-i', url,
                     '-frames:v', '1', '-vf', 'scale=336:-1', out],
                    capture_output=True, text=True, timeout=60)
            except Exception:
                return None, dur
            if r.returncode != 0 or not os.path.exists(out) or os.path.getsize(out) < 1000:
                return None, dur
            paths.append(out)
        return paths, dur

    def analyze(self, paths):
        """Returns (topic_tag_or_None, topic_score, format_tag_or_None)."""
        self._load()
        import torch
        from PIL import Image
        imgs = [Image.open(p) for p in paths]

        with torch.no_grad():
            inp = self.proc(text=self.topic_prompts, images=imgs,
                            return_tensors='pt', padding=True)
            mean = self.model(**inp).logits_per_image.softmax(dim=1).mean(dim=0).tolist()
        ranked = sorted(zip(self.leaves, mean), key=lambda kv: -kv[1])
        ratio = ranked[0][1] / max(ranked[1][1], 1e-6)
        topic, score = None, 0.0
        if ratio >= VISION_LEAF_RATIO:
            topic, score = ranked[0][0], round(ratio, 2)
        else:
            mass = {c: sum(p for l, p in zip(self.leaves, mean)
                           if v2.LEAF_TO_CATEGORY[l] == c) for c in v2.CATEGORIES}
            top_cat = max(mass, key=mass.get)
            if mass[top_cat] >= VISION_CATEGORY_MASS:
                topic, score = top_cat, round(mass[top_cat], 3)

        fkeys = list(self.format_prompts)
        with torch.no_grad():
            inp = self.proc(text=list(self.format_prompts.values()), images=imgs,
                            return_tensors='pt', padding=True)
            fmean = self.model(**inp).logits_per_image.softmax(dim=1).mean(dim=0).tolist()
        fbest = max(zip(fkeys, fmean), key=lambda kv: kv[1])
        # only a confidently-detected MAPPED format acts as a topic prior
        fmt = fbest[0] if (fbest[1] >= FORMAT_MIN_PROB
                           and fbest[0] in v2.FORMAT_TO_TOPIC) else None
        return topic, score, fmt


# ── text ──────────────────────────────────────────────────────────────────────
class TextTagger:
    """bart zero-shot over the v2 leaves, calibrated thresholds. Lazy load."""

    def __init__(self, config):
        self.cfg = config
        self.clf = None
        self.leaves = sorted(v2.LEAVES - v2.EVIDENCE_ONLY_LEAVES)
        self.thresholds = config['tagging'].get('label_thresholds_v2', {})

    def _load(self):
        if self.clf is None:
            from transformers import pipeline
            m = self.cfg['models']['tagging']
            cache = os.environ.get('HF_MODEL_CACHE') or m.get('cache_dir', '/app/models')
            self.clf = pipeline('zero-shot-classification', model=m['model'],
                                device=-1, model_kwargs={'cache_dir': cache})

    def analyze(self, text):
        """Leaves clearing their calibrated floor, best first. Max 2."""
        self._load()
        template = self.cfg['tagging'].get('hypothesis_template', 'This video is about {}.')
        prompts = [v2.LEAF_PROMPTS[l] for l in self.leaves]
        inv = {v2.LEAF_PROMPTS[l]: l for l in self.leaves}
        r = self.clf(text[:1500], candidate_labels=prompts, multi_label=True,
                     hypothesis_template=template, batch_size=16)
        out = []
        for lab, s in zip(r['labels'], r['scores']):
            leaf = inv[lab]
            if s >= self.thresholds.get(leaf, 0.9):
                out.append((leaf, round(s, 3)))
        return out[:2]


# ── fusion ────────────────────────────────────────────────────────────────────
def collapse_parents(tags):
    """Drop a category tag when an accepted leaf already belongs to it."""
    leaf_cats = {v2.LEAF_TO_CATEGORY[t] for t in tags if t in v2.LEAVES}
    return [t for t in tags if t not in (leaf_cats & v2.CATEGORIES)]


def fuse(evidence, vision_topic, format_tag, text_tags):
    """
    Analysis first, the author's Hive tags as a fallback.

    Content analysis — image recognition (vision + its format prior) and the
    title/description/transcript classifier — is the primary signal. The
    author's own Hive tags (`evidence`) are only consulted when analysis
    produced NOTHING confident, so a stray reward/curation tag can no longer
    outrank what the video is actually about.

    (Exclusive creator/community rules are decided earlier and never reach here.)
    """
    analysis = []
    if format_tag:
        prior = v2.FORMAT_TO_TOPIC.get(format_tag)
        if prior:
            analysis.append(prior)
    if vision_topic:
        analysis.append(vision_topic)
    analysis.extend(leaf for leaf, _ in text_tags)

    ordered = analysis if analysis else sorted(evidence)   # <-- evidence = fallback
    deduped = list(dict.fromkeys(ordered))                 # preserve order, drop dups
    return collapse_parents(deduped)[:MAX_TAGS]


# ── worklist ──────────────────────────────────────────────────────────────────
def build_worklist(db, embed_only=False, legacy_since=None, watch_mode=False):
    """
    Eligible videos needing v2 tags, newest first, embed before legacy.

    watch_mode=False (backlog): main tags empty AND no v2 fields yet.
    watch_mode=True  (service): no v2 fields yet — main tags don't matter,
        because new videos get v1 tags from the live pipeline AND v2 from us.
    """
    raw = db.db
    state = {}
    for d in db.tags_collection.find(
            {}, {'author': 1, 'permlink': 1, 'tags': 1, 'tags_list_v2': 1, '_id': 0}):
        state[(d.get('author'), d.get('permlink'))] = (
            bool(d.get('tags')), 'tags_list_v2' in d)
    hidden = {d['username'] for d in
              raw['contentcreators'].find({'hidden': True}, {'username': 1, '_id': 0})}
    logger.info(f"hidden creators: {len(hidden)}")

    def needs_v2(key):
        has_v1, has_v2 = state.get(key, (False, False))
        if has_v2:
            return False
        return True if watch_mode else not has_v1

    work = []
    for vdoc in db.embed_collection.find(
            {'status': 'published', 'manifest_cid': {'$nin': [None, '']}}).sort('createdAt', -1):
        if vdoc['owner'] in hidden or (vdoc.get('hive_permlink') or '').strip() == '':
            continue
        if needs_v2((vdoc['owner'], vdoc['permlink'])):
            vdoc['_url'] = f"{GW}{vdoc['manifest_cid']}/manifest.m3u8"
            work.append(vdoc)
    if not embed_only:
        legacy_q = {'status': 'published', 'filename': {'$regex': '^ipfs://'}}
        if legacy_since:
            legacy_q['created'] = {'$gte': legacy_since}
        for vdoc in db.videos_collection.find(legacy_q).sort('created', -1):
            if vdoc['owner'] in hidden:
                continue
            if needs_v2((vdoc['owner'], vdoc['permlink'])):
                vdoc['_url'] = f"{GW}{vdoc['filename'][7:]}"
                work.append(vdoc)
    return work


def process_one(db, args, vision, text, vdoc, stats):
    owner, permlink = vdoc['owner'], vdoc['permlink']
    meta = normalize_video_metadata(vdoc)

    # 0. AI-made flag — independent of topic tagging, from metadata alone, so
    #    it's recorded even when no layer below produces a confident tag.
    if _write_ai_flag(db, args, owner, permlink, meta):
        stats['ai_flagged'] += 1

    # 1. creator rule, then exclusive community rule — either decides outright
    excl, rule_model = v2.exclusive_tags_for_author(owner), 'author-rule-v2'
    if not excl:
        excl, rule_model = v2.exclusive_tags_for(meta['category']), 'community-rule-v2'
    if excl:
        stats['exclusive'] += 1
        _write(db, args, owner, permlink, excl, {}, excl, rule_model)
        stats['tagged'] += 1
        return

    # 2. evidence
    evidence = v2.tags_from_hive_tags(meta['hive_tags']) | v2.tags_from_category(meta['category'])

    # 3. vision
    vision_topic, vscore, fmt = None, 0.0, None
    unavailable = False
    if vision is not None:
        with tempfile.TemporaryDirectory() as wd:
            paths, dur = vision.frames(vdoc['_url'], wd)
            if paths:
                stats['vision_used'] += 1
                vision_topic, vscore, fmt = vision.analyze(paths)
            else:
                # media no longer fetchable from the gateway — record it so a
                # later pass (or the frontend) can act on it, and never retry.
                stats['unfetchable'] += 1
                unavailable = True

    # 4. text
    title, body = meta['title'], clean_post_body(meta['body'], 1200)
    text_tags = []
    if len(body) >= MIN_TEXT_CHARS or title:
        text_tags = text.analyze((f"Title: {title}\n" if title else '') + body)

    tags = fuse(evidence, vision_topic, fmt, text_tags)
    scores = {**{leaf: s for leaf, s in text_tags}}
    if vision_topic:
        scores[vision_topic] = vscore

    if tags:
        _write(db, args, owner, permlink, tags, scores, sorted(evidence),
               MODEL_MARKER, unavailable=unavailable)
        stats['tagged'] += 1
    # Always persist a row for an unavailable video (even without --mark-empty):
    # it carries the flag and stops the endless re-fetch of dead IPFS.
    elif args.mark_empty or unavailable:
        _write(db, args, owner, permlink, [], {}, [], MODEL_MARKER,
               unavailable=unavailable)
        stats['empty'] += 1
    if unavailable:
        stats['unavailable'] += 1


def run_pass(db, args, vision, text):
    # Dual mode = "anything without v2 tags", including videos v1 already tagged.
    # Implied by --watch (the service dual-tags); --dual gets it for a one-shot
    # backfill, where the default is the narrower "no v1 tags either" backlog.
    work = build_worklist(db, embed_only=args.embed_only,
                          legacy_since=args.legacy_since,
                          watch_mode=bool(args.watch) or args.dual)
    logger.info(f"worklist: {len(work):,} videos needing v2 tags")
    if args.limit:
        work = work[:args.limit]
    stats = {'tagged': 0, 'empty': 0, 'exclusive': 0, 'vision_used': 0,
             'unfetchable': 0, 'unavailable': 0, 'ai_flagged': 0}
    for n, vdoc in enumerate(work):
        if _stop:
            break
        process_one(db, args, vision, text, vdoc, stats)
        if n % 25 == 0:
            logger.info(f"[{n}/{len(work)}] {stats}")
        if args.sleep:
            time.sleep(args.sleep)
    logger.info(f"pass done: {stats}")
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=0, help='0 = whole worklist (per pass)')
    ap.add_argument('--no-vision', action='store_true')
    ap.add_argument('--embed-only', action='store_true',
                    help='skip the legacy collection entirely')
    ap.add_argument('--legacy-since', type=lambda s: datetime.fromisoformat(s), default=None,
                    help='only consider legacy videos created on/after this ISO date')
    ap.add_argument('--dual', action='store_true',
                    help='also tag videos that already have v1 tags (implied by --watch)')
    ap.add_argument('--watch', type=int, default=0, metavar='SECONDS',
                    help='service mode: re-scan every N seconds; also tags videos '
                         'that already have v1 tags (dual tagging), v2 fields only '
                         'plus empty-main fill')
    ap.add_argument('--mark-empty', action='store_true',
                    help='write an empty v2 row when no layer fires')
    ap.add_argument('--sleep', type=float, default=0.0)
    args = ap.parse_args()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    config = yaml.safe_load(open(args.config))
    db = DatabaseManager(config)
    hf_cache = os.environ.get('HF_MODEL_CACHE') or \
        config['models']['tagging'].get('cache_dir', '/app/models')
    vision = None if args.no_vision else VisionTagger(hf_cache)
    text = TextTagger(config)

    if not args.watch:
        run_pass(db, args, vision, text)
        return 0

    logger.info(f"watch mode: scanning every {args.watch}s")
    while not _stop:
        try:
            run_pass(db, args, vision, text)
        except Exception:
            logger.exception("pass failed — retrying next cycle")
        for _ in range(args.watch):
            if _stop:
                break
            time.sleep(1)
    return 0


def _write_ai_flag(db, args, owner, permlink, meta):
    """
    Independent side-channel write: is this video AI-made (TAXONOMY_V2.md's
    cheap-facet pattern). Decoupled from the topic-tag fusion below — it only
    needs title/description/Hive tags, so it runs for every eligible video
    regardless of whether any tagging layer below produces a confident tag.

    Idempotent: skips once already set (or manually locked), so re-running a
    pass or --watch cycle doesn't rewrite it every time. Returns True if a
    flag was (or, in --dry-run, would be) written.
    """
    existing = db.tags_collection.find_one(
        {'author': owner, 'permlink': permlink},
        {'manual': 1, 'ai_generated_v2': 1})
    if existing and (existing.get('manual') or 'ai_generated_v2' in existing):
        return False
    is_ai, ai_evidence = v2.detect_ai_generated(
        meta['hive_tags'], meta['title'], meta['body'], author=owner)
    if args.dry_run:
        logger.info(f"DRY ai_generated_v2={is_ai} {owner}/{permlink} {ai_evidence}")
        return True
    db.tags_collection.update_one(
        {'author': owner, 'permlink': permlink},
        {'$set': {
            'author': owner, 'permlink': permlink,
            'ai_generated_v2': is_ai,
            'ai_generated_evidence_v2': ai_evidence,
            'ai_generated_checked_at': datetime.now(),
        }},
        upsert=True)
    if is_ai:
        logger.info(f"ai flag: {owner}/{permlink} -> True {ai_evidence}")
    return True


def _write(db, args, owner, permlink, tags, scores, evidence, model, unavailable=False):
    """
    Backwards-compatible dual write.

    v2 results ALWAYS land in the parallel *_v2 fields. The classic fields
    (tags/tags_list/...) belong to the v1 pipeline and the frontend: they are
    only filled when currently empty (a video v1 never tagged), NEVER replaced.
    `unavailable` stamps unavailableOnTagging so dead-IPFS media is recorded and
    not re-fetched. Manual locks skip everything.
    """
    flag = ' [UNAVAILABLE]' if unavailable else ''
    line = f"{owner}/{permlink} -> {','.join(tags) or '(empty)'} [{model}]{flag}"
    if args.dry_run:
        logger.info(f"DRY {line}")
        return
    existing = db.tags_collection.find_one(
        {'author': owner, 'permlink': permlink}, {'manual': 1, 'tags': 1})
    if existing and existing.get('manual'):
        logger.info(f"skip (manual lock): {owner}/{permlink}")
        return
    now = datetime.now()
    upd = {
        'author': owner, 'permlink': permlink,
        'tags_v2': ','.join(tags), 'tags_list_v2': tags,
        'tag_scores_v2': scores, 'tag_evidence_v2': evidence,
        'tag_model_v2': model, 'tagged_v2_at': now,
    }
    if unavailable:
        upd['unavailableOnTagging'] = True
        upd['unavailableOnTagging_at'] = now
    if not existing or not existing.get('tags'):
        # v1 never tagged this video — filling the classic fields only ADDS
        # frontend coverage; the v1 pipeline may later refine them.
        upd.update({'tags': ','.join(tags), 'tags_list': tags,
                    'tag_scores': scores, 'tag_evidence': evidence,
                    'tag_model': model, 'created_at': now})
    db.tags_collection.update_one({'author': owner, 'permlink': permlink},
                                  {'$set': upd}, upsert=True)
    logger.info(line)


if __name__ == '__main__':
    sys.exit(main())
