"""
Whisper Transcriber
Handles speech-to-text transcription using faster-whisper
"""

import logging
from typing import List, Dict, Any, Optional
from faster_whisper import WhisperModel

logger = logging.getLogger(__name__)


class Segment:
    """Represents a transcription segment with timing"""

    def __init__(self, start: float, end: float, text: str):
        self.start = start
        self.end = end
        self.text = text.strip()

    def __repr__(self):
        return f"Segment({self.start:.2f}s - {self.end:.2f}s: {self.text[:30]}...)"


class Transcriber:
    """Handles video transcription using Whisper"""

    def __init__(self, config: dict):
        """Initialize Whisper model"""
        self.config = config['models']['whisper']
        self.model = None
        self._load_model()

    def _load_model(self):
        """Load Whisper model"""
        try:
            logger.info(f"Loading Whisper model: {self.config['model_size']}")
            self.model = WhisperModel(
                self.config['model_size'],
                device=self.config['device'],
                compute_type=self.config['compute_type'],
                download_root="/app/models"
            )
            logger.info("Whisper model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load Whisper model: {e}")
            raise

    def transcribe(self, video_path: str, language: str = None,
                   allowed_languages: set = None) -> tuple[List[Segment], str]:
        """
        Transcribe video audio to text with timestamps

        Args:
            video_path: Path to video file
            language: Force transcription in this language (ISO 639-1), or None for auto-detect
            allowed_languages: Set of configured language codes; if detected language
                               is not in this set, re-run forced as English

        Returns:
            Tuple of (list of segments, detected language)
        """
        try:
            logger.info(f"Starting transcription: {video_path}"
                        + (f" (forced language: {language})" if language else ""))

            # Transcribe with faster-whisper
            transcribe_kwargs = dict(
                beam_size=5,
                vad_filter=True,
                vad_parameters=dict(min_silence_duration_ms=500),
            )
            if language:
                transcribe_kwargs['language'] = language

            segments_iterator, info = self.model.transcribe(
                video_path, **transcribe_kwargs
            )

            detected_language = language or info.language
            logger.info(f"Detected language: {detected_language}"
                        f" (probability: {info.language_probability:.2f})")

            # If detected language isn't in our config, restart as English
            # (checks before consuming the iterator, so no wasted work)
            if not language and allowed_languages and detected_language not in allowed_languages:
                logger.warning(f"Detected '{detected_language}' not in config, re-transcribing as English")
                return self.transcribe(video_path, language='en')

            # Convert to Segment objects
            segments = []
            for segment in segments_iterator:
                seg = Segment(
                    start=segment.start,
                    end=segment.end,
                    text=segment.text
                )
                segments.append(seg)

            logger.info(f"Transcription complete: {len(segments)} segments")
            return segments, detected_language

        except Exception as e:
            logger.error(f"Transcription failed: {e}")
            raise

    def get_transcript_text(self, segments: List[Segment],
                           max_duration: Optional[float] = None) -> str:
        """
        Get full transcript text from segments

        Args:
            segments: List of transcription segments
            max_duration: Optional maximum duration in seconds (for sampling)

        Returns:
            Full transcript as string
        """
        if max_duration:
            # Filter segments within duration
            filtered_segments = [
                s for s in segments if s.start < max_duration
            ]
        else:
            filtered_segments = segments

        return ' '.join(s.text for s in filtered_segments)

    def get_segments_for_language(self, segments: List[Segment]) -> List[Dict[str, Any]]:
        """
        Convert segments to dictionary format for translation

        Args:
            segments: List of Segment objects

        Returns:
            List of segment dictionaries
        """
        return [
            {
                'start': seg.start,
                'end': seg.end,
                'text': seg.text
            }
            for seg in segments
        ]
