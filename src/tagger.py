"""
Content Tagger
Analyzes video transcripts and assigns relevant tags using zero-shot classification
"""

import logging
from typing import List, Dict, Any
from transformers import pipeline
import torch

logger = logging.getLogger(__name__)


class ContentTagger:
    """Handles video content tagging using zero-shot classification"""

    def __init__(self, config: dict):
        """Initialize zero-shot classification model"""
        self.config = config
        self.tags_list = config['tags']
        self.max_tags = config['tagging']['max_tags']
        self.min_confidence = config['tagging']['min_confidence']
        self.use_sample = config['tagging']['use_transcript_sample']
        self.sample_duration = config['tagging']['sample_duration']

        self.classifier = None
        self._load_model()

    def _load_model(self):
        """Load zero-shot classification model"""
        try:
            model_name = self.config['models']['tagging']['model']
            logger.info(f"Loading tagging model: {model_name}")

            self.classifier = pipeline(
                "zero-shot-classification",
                model=model_name,
                device=-1,  # CPU
                model_kwargs={"cache_dir": "/app/models"}
            )

            logger.info("Tagging model loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load tagging model: {e}")
            raise

    def generate_tags(self, transcript: str, segments: List[Any] = None, post_content: str = None) -> List[str]:
        """
        Generate content tags from video transcript and optional Hive post content

        Args:
            transcript: Full transcript text
            segments: Optional list of segments for sampling
            post_content: Optional Hive post body (title + body text)

        Returns:
            List of relevant tags
        """
        try:
            # Sample transcript if needed
            if self.use_sample and segments:
                sampled_text = self._sample_transcript(segments)
            else:
                # Use first 1500 characters to leave room for post content
                sampled_text = transcript[:1500]

            # Combine with Hive post content when available
            if post_content and post_content.strip():
                if sampled_text.strip():
                    # Both sources available — interleave so neither dominates
                    combined_text = f"{post_content[:1500]}\n\n{sampled_text}"
                else:
                    # No speech — rely entirely on the post body
                    combined_text = post_content[:3000]
                    logger.info("Transcript empty; using Hive post content for tagging")
                sampled_text = combined_text
            elif not sampled_text.strip():
                logger.warning("Empty transcript and no post content, returning default tags")
                return ['vlog', 'general']

            logger.info("Analyzing content for tags...")

            # Perform zero-shot classification
            # batch_size batches NLI pairs together: 40 labels / 16 = ~3 forward
            # passes instead of 40, giving ~10x speedup on CPU
            result = self.classifier(
                sampled_text,
                candidate_labels=self.tags_list,
                multi_label=True,
                batch_size=16
            )

            # Filter by confidence and get top tags
            tags = []
            for label, score in zip(result['labels'], result['scores']):
                if score >= self.min_confidence and len(tags) < self.max_tags:
                    tags.append(label)

            # Ensure at least one tag
            if not tags:
                tags = [result['labels'][0]]  # Take highest scoring tag

            logger.info(f"Generated tags: {', '.join(tags)}")
            return tags

        except Exception as e:
            logger.error(f"Tag generation failed: {e}")
            # Return safe default tags
            return ['general', 'video']

    def _sample_transcript(self, segments: List[Any]) -> str:
        """
        Sample transcript from first N seconds

        Args:
            segments: List of transcript segments

        Returns:
            Sampled transcript text
        """
        sampled_segments = [
            seg for seg in segments
            if seg.start < self.sample_duration
        ]

        return ' '.join(seg.text for seg in sampled_segments)

    def get_tag_scores(self, transcript: str) -> Dict[str, float]:
        """
        Get all tags with their confidence scores

        Args:
            transcript: Transcript text

        Returns:
            Dictionary mapping tags to confidence scores
        """
        try:
            sampled_text = transcript[:3000]

            result = self.classifier(
                sampled_text,
                candidate_labels=self.tags_list,
                multi_label=True
            )

            return dict(zip(result['labels'], result['scores']))

        except Exception as e:
            logger.error(f"Failed to get tag scores: {e}")
            return {}

    def add_custom_tags(self, tags: List[str], custom_tags: List[str]) -> List[str]:
        """
        Add custom tags to existing tag list

        Args:
            tags: Existing tags
            custom_tags: Custom tags to add

        Returns:
            Combined tag list
        """
        combined = tags.copy()
        for tag in custom_tags:
            if tag not in combined and len(combined) < self.max_tags + 2:
                combined.append(tag)
        return combined
