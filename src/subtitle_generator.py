"""
Subtitle Generator
Handles SRT subtitle file generation with timestamp preservation
"""

import os
import logging
import re
from typing import List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class SubtitleGenerator:
    """Generates SRT subtitle files from segments"""

    def __init__(self, config: dict):
        """Initialize subtitle generator"""
        self.subtitle_format = config['processing']['subtitle_format']
        self.output_dir = Path('/app/subtitles')
        self.output_dir.mkdir(exist_ok=True)

    def generate_srt(self, segments: List[Dict[str, Any]], output_path: str) -> bool:
        """
        Generate SRT subtitle file from segments

        SRT Format:
        1
        00:00:00,000 --> 00:00:05,000
        Subtitle text here

        Args:
            segments: List of segment dicts with 'start', 'end', 'text'
            output_path: Path to save SRT file

        Returns:
            True if successful
        """
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                for i, segment in enumerate(segments, start=1):
                    # Sequence number
                    f.write(f"{i}\n")

                    # Timestamp line (CRITICAL: Never translate this!)
                    start_time = self._format_timestamp(segment['start'])
                    end_time = self._format_timestamp(segment['end'])
                    f.write(f"{start_time} --> {end_time}\n")

                    # Text content (this is what gets translated)
                    text = segment['text'].strip()
                    f.write(f"{text}\n\n")

            logger.info(f"Generated SRT: {os.path.basename(output_path)}")
            return True

        except Exception as e:
            logger.error(f"Failed to generate SRT: {e}")
            return False

    def _format_timestamp(self, seconds: float) -> str:
        """
        Format seconds to SRT timestamp format: HH:MM:SS,mmm

        Args:
            seconds: Time in seconds

        Returns:
            Formatted timestamp string
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        millis = int((seconds % 1) * 1000)

        return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"

    def parse_srt(self, srt_path: str) -> List[Dict[str, Any]]:
        """
        Parse SRT file back to segments (for validation or modification)

        Args:
            srt_path: Path to SRT file

        Returns:
            List of segment dictionaries
        """
        try:
            with open(srt_path, 'r', encoding='utf-8') as f:
                content = f.read()

            segments = []
            # Regex pattern to match SRT entries
            pattern = r'(\d+)\n(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\n((?:.*\n?)+?)(?:\n\n|\Z)'

            matches = re.findall(pattern, content)

            for match in matches:
                seq_num, start_str, end_str, text = match
                segments.append({
                    'start': self._parse_timestamp(start_str),
                    'end': self._parse_timestamp(end_str),
                    'text': text.strip()
                })

            return segments

        except Exception as e:
            logger.error(f"Failed to parse SRT: {e}")
            return []

    def _parse_timestamp(self, timestamp_str: str) -> float:
        """
        Parse SRT timestamp to seconds

        Args:
            timestamp_str: Timestamp string (HH:MM:SS,mmm)

        Returns:
            Time in seconds
        """
        # Format: HH:MM:SS,mmm
        time_parts, millis = timestamp_str.split(',')
        hours, minutes, seconds = map(int, time_parts.split(':'))

        total_seconds = hours * 3600 + minutes * 60 + seconds + int(millis) / 1000
        return total_seconds

    def validate_srt(self, srt_path: str) -> bool:
        """
        Validate SRT file format

        Args:
            srt_path: Path to SRT file

        Returns:
            True if valid
        """
        try:
            segments = self.parse_srt(srt_path)

            if not segments:
                logger.error(f"No segments found in {srt_path}")
                return False

            # Check timestamp ordering
            for i in range(len(segments) - 1):
                if segments[i]['end'] > segments[i + 1]['start']:
                    logger.warning(f"Overlapping timestamps at segment {i + 1}")

            # Check for empty text
            empty_count = sum(1 for seg in segments if not seg['text'].strip())
            if empty_count > 0:
                logger.warning(f"Found {empty_count} segments with empty text")

            logger.info(f"SRT validation passed: {os.path.basename(srt_path)}")
            return True

        except Exception as e:
            logger.error(f"SRT validation failed: {e}")
            return False

    def create_subtitle_path(self, author: str, permlink: str, language: str) -> str:
        """
        Create standardized subtitle file path

        Args:
            author: Video author
            permlink: Video permlink
            language: Language code

        Returns:
            Full path to subtitle file
        """
        # Create author directory
        author_dir = self.output_dir / author
        author_dir.mkdir(exist_ok=True)

        # Create filename
        filename = f"{permlink}.{language}.srt"
        return str(author_dir / filename)

    def translate_and_preserve_timestamps(self,
                                         original_segments: List[Dict[str, Any]],
                                         translated_texts: List[str]) -> List[Dict[str, Any]]:
        """
        Combine original timestamps with translated text
        CRITICAL: This ensures timestamps are never modified during translation

        Args:
            original_segments: Original segments with timestamps
            translated_texts: Translated text strings (same order)

        Returns:
            New segments with original timestamps and translated text
        """
        if len(original_segments) != len(translated_texts):
            logger.error(
                f"Segment count mismatch: {len(original_segments)} vs {len(translated_texts)}"
            )
            raise ValueError("Segment and translation count must match")

        translated_segments = []
        for orig_seg, translated_text in zip(original_segments, translated_texts):
            translated_segments.append({
                'start': orig_seg['start'],  # PRESERVE original timestamp
                'end': orig_seg['end'],      # PRESERVE original timestamp
                'text': translated_text      # NEW translated text
            })

        return translated_segments
