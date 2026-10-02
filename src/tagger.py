"""
Content Tagger

Combines two sources of evidence:

  1. Author-supplied Hive tags and the Hive community, mapped onto our taxonomy.
     Human-authored and high precision — treated as fact, never overridden.
  2. Zero-shot classification (bart-large-mnli) over title + summary + post body.

The classifier is only ever allowed to *add* tags. It runs on English text
(the generated summary where available) because bart-large-mnli is English-only,
and its raw scores are uncalibrated, so each label carries its own threshold
plus a relative cutoff against the top-scoring label.
"""

import logging
from typing import Any, Dict, List, Optional

from transformers import pipeline

from tag_taxonomy import (
    apply_implications,
    clean_post_body,
    exclusive_tags_for,
    tags_from_category,
    tags_from_hive_tags,
    topical_hive_tags,
)

logger = logging.getLogger(__name__)

# Marks tags decided outright by a single-topic community, with no classifier.
COMMUNITY_RULE_MODEL = 'community-rule'


class ContentTagger:
    """Video content tagging: Hive metadata evidence + zero-shot classification."""

    def __init__(self, config: dict):
        self.config = config
        self.tags_list = config['tags']

        tagging = config['tagging']
        self.max_tags = tagging['max_tags']
        self.min_confidence = tagging['min_confidence']
        self.relative_ratio = tagging.get('relative_ratio', 0.6)
        self.label_thresholds = tagging.get('label_thresholds', {}) or {}
        self.hypothesis_template = tagging.get(
            'hypothesis_template', 'This video is about {}.'
        )
        self.use_sample = tagging['use_transcript_sample']
        self.sample_duration = tagging['sample_duration']
        self.content_chars = tagging.get('content_chars', 1500)
        self.body_chars = tagging.get('body_chars', 1200)
        self.fallback_confidence = tagging.get('fallback_confidence', 0.5)

        model_cfg = config['models']['tagging']
        self.model_name = model_cfg['model']
        # Overridable so the tagger can be exercised outside the container,
        # where /app/models does not exist.
        self.cache_dir = model_cfg.get('cache_dir', '/app/models')
        self.classifier = None
        self._load_model()

    def _load_model(self):
        try:
            logger.info(f"Loading tagging model: {self.model_name}")
            self.classifier = pipeline(
                "zero-shot-classification",
                model=self.model_name,
                device=-1,  # CPU
                model_kwargs={"cache_dir": self.cache_dir},
            )
            logger.info("Tagging model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load tagging model: {e}")
            raise

    def _threshold_for(self, label: str) -> float:
        """Per-label threshold, falling back to the global minimum."""
        return float(self.label_thresholds.get(label, self.min_confidence))

    def build_classifier_input(
        self,
        metadata: Optional[Dict[str, Any]] = None,
        content_text: str = '',
    ) -> str:
        """
        Assemble the text handed to the classifier.

        bart-large-mnli truncates at 1024 tokens, so the highest-signal fields go
        first: title, then the author's own topical tags, then the summary, and
        finally the post body.
        """
        metadata = metadata or {}
        parts: List[str] = []

        title = (metadata.get('title') or '').strip()
        if title:
            parts.append(f"Title: {title}")

        # Unmappable author tags ("ac_origin", "bayek") are still topical evidence.
        author_tags = topical_hive_tags(metadata.get('hive_tags'))
        if author_tags:
            parts.append(f"Tags: {', '.join(author_tags[:12])}")

        if content_text and content_text.strip():
            parts.append(content_text.strip()[: self.content_chars])

        body = clean_post_body(metadata.get('body') or '', self.body_chars)
        if body:
            parts.append(body)

        return "\n".join(parts).strip()

    def classify(self, text: str) -> Dict[str, float]:
        """Zero-shot scores for every taxonomy label. Empty dict on failure."""
        if not text.strip():
            return {}
        result = self.classifier(
            text,
            candidate_labels=self.tags_list,
            multi_label=True,
            hypothesis_template=self.hypothesis_template,
            batch_size=16,
        )
        return dict(zip(result['labels'], result['scores']))

    def generate_tags(
        self,
        transcript: str = '',
        segments: Optional[List[Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        content_text: str = '',
    ) -> Dict[str, Any]:
        """
        Generate tags for a video.

        Args:
            transcript: Source-language transcript (fallback content only).
            segments: Transcript segments, used to sample the opening minutes.
            metadata: Output of `video_meta.normalize_video_metadata`.
            content_text: Preferred English text (summary_en or translated
                transcript). Falls back to the transcript sample when absent.

        Returns:
            {'tags': [...], 'evidence': [...], 'scores': {...}, 'model': str}
        """
        metadata = metadata or {}

        # 0. Single-topic community: membership decides the tags outright. No
        #    classifier, no transcript, and no extra tags bolted on — these
        #    communities are unambiguous, so anything else is noise.
        exclusive = exclusive_tags_for(metadata.get('category'))
        if exclusive:
            tags = exclusive[: self.max_tags]
            logger.info(f"Tags: {', '.join(tags)} [community rule]")
            return {
                'tags': tags,
                'evidence': tags,
                'scores': {},
                'model': COMMUNITY_RULE_MODEL,
            }

        # 1. Author-supplied evidence. Trusted; never filtered by the classifier.
        evidence = tags_from_hive_tags(metadata.get('hive_tags'))
        evidence |= tags_from_category(metadata.get('category'))

        # 2. Choose the text to classify. English is strongly preferred.
        if not content_text:
            if self.use_sample and segments:
                content_text = self._sample_transcript(segments)
            else:
                content_text = transcript or ''

        text = self.build_classifier_input(metadata, content_text)

        scores: Dict[str, float] = {}
        if text:
            try:
                scores = self.classify(text)
            except Exception as e:
                logger.error(f"Zero-shot classification failed: {e}")
        else:
            logger.info("No text available for classification; using Hive evidence only")

        # 3. Accept a classifier label only if it clears its own threshold *and*
        #    is close enough to the top label. Both guards are needed: the first
        #    kills known attractors ('news'), the second kills the long tail on
        #    videos where one topic clearly dominates.
        accepted: List[str] = []
        if scores:
            top_score = max(scores.values())
            floor = self.relative_ratio * top_score
            ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
            accepted = [
                label
                for label, score in ranked
                if score >= self._threshold_for(label) and score >= floor
            ]

        # 4. Merge: evidence first (ordered by classifier score when we have one),
        #    then accepted classifier labels. No padding to max_tags.
        evidence_ordered = sorted(evidence, key=lambda t: -scores.get(t, 0.0))
        tags = evidence_ordered + [t for t in accepted if t not in evidence]

        # 5. Last resort: a single confident label rather than an empty list.
        if not tags and scores:
            top_label, top_score = max(scores.items(), key=lambda kv: kv[1])
            if top_score >= self.fallback_confidence:
                tags = [top_label]

        # 6. Expand entailed tags ('tutorial' implies 'education') before the cap,
        #    so an implied tag can displace a weaker classifier tag.
        tags = apply_implications(tags)[: self.max_tags]

        logger.info(
            f"Tags: {', '.join(tags) or '(none)'} "
            f"[evidence: {', '.join(sorted(evidence)) or 'none'}]"
        )
        return {
            'tags': tags,
            'evidence': sorted(evidence),
            'scores': {k: round(v, 4) for k, v in scores.items()},
            'model': self.model_name,
        }

    def _sample_transcript(self, segments: List[Any]) -> str:
        """Transcript text from the first `sample_duration` seconds."""
        return ' '.join(
            seg.text for seg in segments if seg.start < self.sample_duration
        )
