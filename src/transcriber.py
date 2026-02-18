"""
Whisper Transcriber
Handles speech-to-text transcription using faster-whisper with batched inference
"""

import re
import logging
from typing import List, Dict, Any, Optional
from faster_whisper import WhisperModel, BatchedInferencePipeline

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
        self.pipeline = None
        self._load_model()

    def _load_model(self):
        """Load Whisper model with batched inference pipeline"""
        try:
            model_size = self.config['model_size']
            logger.info(f"Loading Whisper model: {model_size}")
            self.model = WhisperModel(
                model_size,
                device=self.config['device'],
                compute_type=self.config['compute_type'],
                download_root="/app/models"
            )
            self.pipeline = BatchedInferencePipeline(model=self.model)
            logger.info(f"Whisper model loaded with batched inference pipeline")
        except Exception as e:
            logger.error(f"Failed to load Whisper model: {e}")
            raise

    def transcribe(self, video_path: str, language: str = None,
                   allowed_languages: set = None,
                   hotwords: List[str] = None) -> tuple[List[Segment], str]:
        """
        Transcribe video audio to text with timestamps

        Args:
            video_path: Path to video file
            language: Force transcription in this language (ISO 639-1), or None for auto-detect
            allowed_languages: Set of configured language codes; if detected language
                               is not in this set, re-run forced as English
            hotwords: Platform-specific words to prime the model with

        Returns:
            Tuple of (list of segments, detected language)
        """
        try:
            logger.info(f"Starting transcription: {video_path}"
                        + (f" (forced language: {language})" if language else ""))

            beam_size = self.config.get('beam_size', 3)
            batch_size = self.config.get('batch_size', 16)

            transcribe_kwargs = dict(
                beam_size=beam_size,
                batch_size=batch_size,
                word_timestamps=True,
            )
            if language:
                transcribe_kwargs['language'] = language

            # Prime model with platform-specific vocabulary
            if hotwords:
                transcribe_kwargs['initial_prompt'] = ', '.join(hotwords)
                transcribe_kwargs['hotwords'] = ', '.join(hotwords)
                logger.info(f"Using {len(hotwords)} hotwords for transcription")

            segments_iterator, info = self.pipeline.transcribe(
                video_path, **transcribe_kwargs
            )

            detected_language = language or info.language
            logger.info(f"Detected language: {detected_language}"
                        f" (probability: {info.language_probability:.2f})")

            # If detected language isn't in our config, restart as English
            if not language and allowed_languages and detected_language not in allowed_languages:
                logger.warning(f"Detected '{detected_language}' not in config, re-transcribing as English")
                return self.transcribe(video_path, language='en', hotwords=hotwords)

            # Collect raw segments, then split long ones using word timestamps
            max_words = self.config.get('max_words_per_segment', 8)
            segments = []
            for segment in segments_iterator:
                words = segment.words if segment.words else []
                if len(words) > max_words:
                    segments.extend(self._split_segment(words, max_words))
                else:
                    segments.append(Segment(
                        start=segment.start,
                        end=segment.end,
                        text=segment.text,
                    ))

            logger.info(f"Transcription complete: {len(segments)} segments")
            return segments, detected_language

        except Exception as e:
            logger.error(f"Transcription failed: {e}")
            raise

    @staticmethod
    def _split_segment(words, max_words: int) -> List['Segment']:
        """Split a list of Word objects into shorter Segments.

        Tries to break at sentence-ending punctuation (.!?) first,
        then at clause punctuation (,;:), then at the word limit.
        """
        SENTENCE_END = re.compile(r'[.!?]$')
        CLAUSE_BREAK = re.compile(r'[,;:]$')

        segments = []
        buf = []

        def flush():
            if not buf:
                return
            text = ''.join(w.word for w in buf).strip()
            if text:
                segments.append(Segment(
                    start=buf[0].start,
                    end=buf[-1].end,
                    text=text,
                ))
            buf.clear()

        for word in words:
            buf.append(word)

            at_limit = len(buf) >= max_words
            word_text = word.word.strip()

            if SENTENCE_END.search(word_text):
                flush()
            elif at_limit and CLAUSE_BREAK.search(word_text):
                flush()
            elif at_limit:
                # Look ahead not possible; just cut here
                flush()

        flush()
        return segments

    @staticmethod
    def apply_corrections(segments: List['Segment'],
                          corrections: List[Dict[str, str]]) -> List['Segment']:
        """Apply text corrections (case-insensitive) to all segments."""
        if not corrections:
            return segments
        patterns = [
            (re.compile(re.escape(c['from']), re.IGNORECASE), c['to'])
            for c in corrections
        ]
        for seg in segments:
            for pattern, replacement in patterns:
                seg.text = pattern.sub(replacement, seg.text)
        return segments

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
