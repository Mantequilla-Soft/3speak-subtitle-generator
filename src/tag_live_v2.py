"""
v2 faceted tags for the LIVE transcription pipeline (src/main.py).

Why this exists: the background `tagger-v2` service builds its worklist once per
pass, and a pass of 500 videos takes hours — so a freshly published video can
wait a long time for v2 tags. Hooking in here means a video gets them the moment
it is transcribed, using the transcript (a far stronger signal than metadata
alone) and the classifier main.py has ALREADY loaded (no second bart in memory).

Vision IS run here, on the copy main.py already downloaded for transcription —
so frame extraction is a local ffmpeg seek, not the slow IPFS fetch that
dominates the batch job. This matters for quality, not just speed: once we write
tags the background pass skips the video forever, so if live tagging had no
vision, new videos would permanently get *worse* tags than the back catalogue.
The VisionTagger and fuse() are imported from the batch tagger so live and batch
produce identical results.

Everything here is best-effort: any failure (missing Pillow in an older image,
an unreadable file, a model that won't load) is caught and degrades to
evidence + transcript rather than disturbing transcription.

Writes are the same backwards-compatible dual write the batch tagger uses: v2
values go to the *_v2 fields; the classic v1 fields are only filled when empty,
never overwritten, and manual locks are always respected. When nothing is
confident we write NOTHING, leaving the video for the background pass.
"""

import logging
import os
import sys
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tag_taxonomy_v2 as v2  # noqa: E402

logger = logging.getLogger(__name__)

MAX_TAGS = 5
DEFAULT_THRESHOLD = 0.95   # conservative when a leaf has no calibrated floor

# CLIP is loaded once per process, on first use, and reused for every video.
_vision = None


def _get_vision(config: Dict[str, Any]):
    """Lazily build the shared VisionTagger (adds ~600MB once, then reused)."""
    global _vision
    if _vision is None:
        from tag_videos_v2 import VisionTagger
        cache = (os.environ.get('HF_MODEL_CACHE')
                 or config.get('models', {}).get('tagging', {}).get('cache_dir', '/app/models'))
        _vision = VisionTagger(cache)
    return _vision


def _classify_v2(classifier, text: str, thresholds: Dict[str, float],
                 hypothesis_template: str) -> List[tuple]:
    """Run the pipeline's bart over the v2 leaves; keep leaves clearing their floor."""
    if not text or not text.strip():
        return []
    # evidence-only leaves (e.g. 'vlog') are never offered to the model
    leaves = sorted(v2.LEAVES - v2.EVIDENCE_ONLY_LEAVES)
    prompts = [v2.LEAF_PROMPTS[l] for l in leaves]
    inv = {v2.LEAF_PROMPTS[l]: l for l in leaves}
    result = classifier(
        text,
        candidate_labels=prompts,
        multi_label=True,
        hypothesis_template=hypothesis_template,
        batch_size=16,
    )
    out = []
    for label, score in zip(result['labels'], result['scores']):
        leaf = inv[label]
        if score >= float(thresholds.get(leaf, DEFAULT_THRESHOLD)):
            out.append((leaf, round(float(score), 4)))
    return out[:2]


def _collapse_parents(tags: List[str]) -> List[str]:
    """Drop a category tag when an accepted leaf already covers that category."""
    leaf_cats = {v2.LEAF_TO_CATEGORY[t] for t in tags if t in v2.LEAVES}
    return [t for t in tags if t not in (leaf_cats & v2.CATEGORIES)]


def _save(db, author: str, permlink: str, tags: List[str],
          scores: Dict[str, float], evidence: List[str], model: str) -> None:
    """Dual write: *_v2 always; classic fields only if v1 left them empty."""
    existing = db.tags_collection.find_one(
        {'author': author, 'permlink': permlink}, {'manual': 1, 'tags': 1})
    if existing and existing.get('manual'):
        logger.info(f"  v2: skipping {author}/{permlink} — manually locked")
        return
    now = datetime.now()
    upd = {
        'author': author, 'permlink': permlink,
        'tags_v2': ','.join(tags), 'tags_list_v2': tags,
        'tag_scores_v2': scores, 'tag_evidence_v2': evidence,
        'tag_model_v2': model, 'tagged_v2_at': now,
    }
    if not existing or not existing.get('tags'):
        upd.update({'tags': ','.join(tags), 'tags_list': tags,
                    'tag_scores': scores, 'tag_evidence': evidence,
                    'tag_model': model, 'created_at': now})
    db.tags_collection.update_one({'author': author, 'permlink': permlink},
                                  {'$set': upd}, upsert=True)


def _save_ai_flag(db, author: str, permlink: str, metadata: Dict[str, Any]) -> None:
    """
    Independent side-channel write: is this video AI-made (TAXONOMY_V2.md's
    cheap-facet pattern). Deliberately decoupled from the topic-tag fusion
    above — it needs only title/description/Hive tags, so it is written
    unconditionally, even when nothing else about the video is confident yet
    and _save() is never reached.

    Skips silently once already set, so it never overwrites a later manual
    correction and a re-run stays cheap.
    """
    existing = db.tags_collection.find_one(
        {'author': author, 'permlink': permlink},
        {'manual': 1, 'ai_generated_v2': 1})
    if existing and (existing.get('manual') or 'ai_generated_v2' in existing):
        return
    is_ai, ai_evidence = v2.detect_ai_generated(
        metadata.get('hive_tags'), metadata.get('title'), metadata.get('body'),
        author=author)
    db.tags_collection.update_one(
        {'author': author, 'permlink': permlink},
        {'$set': {
            'author': author, 'permlink': permlink,
            'ai_generated_v2': is_ai,
            'ai_generated_evidence_v2': ai_evidence,
            'ai_generated_checked_at': datetime.now(),
        }},
        upsert=True)
    if is_ai:
        logger.info(f"  v2 ai flag: True {ai_evidence}")


def tag_video_v2(db, author: str, permlink: str, metadata: Dict[str, Any],
                 classifier, text: str, config: Dict[str, Any],
                 video_path: Optional[str] = None) -> List[str]:
    """
    Produce and store v2 tags for a just-transcribed video.

    `video_path` is the local file main.py already downloaded; when given, three
    frames are read from it for the CLIP pass (no IPFS round-trip).

    Returns the tags written ([] if nothing confident — deliberately leaving the
    video for the background vision pass).
    """
    tagging_cfg = config.get('tagging', {})
    thresholds = tagging_cfg.get('label_thresholds_v2', {})
    template = tagging_cfg.get('hypothesis_template', 'This video is about {}.')
    category = metadata.get('category') or ''

    # 0. AI-made flag — independent of topic tagging, from metadata alone.
    try:
        _save_ai_flag(db, author, permlink, metadata)
    except Exception as e:
        logger.warning(f"  v2 ai flag failed: {e}")

    # 1. creator rule, then exclusive community rule — decide outright, no models.
    #    (The community rule alone correctly tags e.g. a Music-community piano
    #    concert that the v1 classifier mislabelled 'technology'.)
    exclusive, rule_model = v2.exclusive_tags_for_author(author), 'author-rule-v2'
    if not exclusive:
        exclusive, rule_model = v2.exclusive_tags_for(category), 'community-rule-v2'
    if exclusive:
        _save(db, author, permlink, exclusive, {}, exclusive, rule_model)
        logger.info(f"  v2 tags: {','.join(exclusive)} [{rule_model}]")
        return exclusive

    # 2. evidence from the author's own Hive tags + community
    evidence = v2.tags_from_hive_tags(metadata.get('hive_tags')) \
        | v2.tags_from_category(category)

    # 3. vision, from the local file — same model and decision rule as the batch
    vision_topic, vscore, fmt = None, 0.0, None
    if video_path and os.path.exists(video_path):
        try:
            vt = _get_vision(config)
            with tempfile.TemporaryDirectory() as wd:
                paths, _dur = vt.frames(video_path, wd)
                if paths:
                    vision_topic, vscore, fmt = vt.analyze(paths)
        except Exception as e:
            logger.warning(f"  v2 vision skipped: {e}")

    # 4. transcript/summary through the already-loaded classifier
    text_tags = []
    try:
        text_tags = _classify_v2(classifier, text, thresholds, template)
    except Exception as e:
        logger.warning(f"  v2 text classification failed: {e}")

    # 5. fuse with the batch tagger's own logic, so live == batch
    try:
        from tag_videos_v2 import fuse
        tags = fuse(evidence, vision_topic, fmt, text_tags)
    except Exception:
        # analysis first, evidence as fallback (mirrors fuse())
        analysis = ([leaf for leaf, _ in text_tags]
                    + ([vision_topic] if vision_topic else []))
        tags = _collapse_parents(analysis or sorted(evidence))[:MAX_TAGS]

    if not tags:
        logger.info("  v2 tags: none confident — left for the background pass")
        return []

    scores = {leaf: s for leaf, s in text_tags}
    if vision_topic:
        scores[vision_topic] = vscore
    _save(db, author, permlink, tags, scores, sorted(evidence), 'v2-live')
    logger.info(f"  v2 tags: {','.join(tags)} [v2-live]"
                + (f" (vision: {vision_topic})" if vision_topic else ''))
    return tags
