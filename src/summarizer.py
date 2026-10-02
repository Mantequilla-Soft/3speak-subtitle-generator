"""
English summarizer (BART-Large-CNN)
Produces coherent paragraph-length summaries. For non-English content the caller
must translate the transcript to English first (NLLB).
"""

import re
import logging
import torch
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

logger = logging.getLogger(__name__)

# Suppress noisy "Token indices sequence length is longer than the specified
# maximum sequence length" warning from the HF tokenizer — we intentionally
# tokenize long transcripts in one shot to compute chunk boundaries.
logging.getLogger("transformers.tokenization_utils_base").setLevel(logging.ERROR)


# bart-large-cnn falls back to its CNN/DailyMail training set when given thin input.
HALLUCINATION_MARKERS = (
    'thank you for reading this article',
    'please share it with your friends',
    'visit cnn.com',
    'cnn.com/soulmate',
    'send your photos and videos',
    'cnn ireport',
    'submit your story to cnn',
    'see cnn.com for more',
)


def is_hallucinated_summary(text: str) -> bool:
    """True if a saved summary looks like a bart-large-cnn fallback hallucination."""
    if not text:
        return False
    low = text.lower()
    return any(m in low for m in HALLUCINATION_MARKERS)

_WS_RE = re.compile(r'\s+')


def _normalize(text: str) -> str:
    return _WS_RE.sub(' ', text.replace('\n', ' ')).strip()


class Summarizer:
    """facebook/bart-large-cnn — paragraph-quality English summarization."""

    MODEL_NAME = 'facebook/bart-large-cnn'
    MAX_INPUT_TOKENS = 1024
    CHUNK_OVERLAP = 64
    MIN_CHUNK_TOKENS = 80
    MIN_INPUT_WORDS = 80  # below this, model just hallucinates CNN-style templates

    # Tuning per generation stage
    CHUNK_MAX_TOKENS = 160   # per-chunk summary (~3-4 sentences)
    CHUNK_MIN_TOKENS = 60
    FINAL_MAX_TOKENS = 280   # final reduce summary (~5-8 sentences)
    FINAL_MIN_TOKENS = 100


    def __init__(self, config: dict):
        cache_dir = '/app/models'
        logger.info(f"Loading summarizer: {self.MODEL_NAME}")
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.MODEL_NAME, cache_dir=cache_dir
        )
        self.model = AutoModelForSeq2SeqLM.from_pretrained(
            self.MODEL_NAME, cache_dir=cache_dir
        )
        self.model.eval()
        logger.info("Summarizer model loaded successfully")

    def summarize(self, text: str) -> str:
        """
        Summarize English text. Short text → single bart pass. Long text →
        chunk-summaries (map) then a final reduce pass for one coherent paragraph.
        Returns '' if transcript is too short or if output looks hallucinated.
        """
        text = _normalize(text)
        if not text:
            return ''

        # Hard guard: bart-large-cnn hallucinates CNN-style templates on thin input.
        word_count = len(text.split())
        if word_count < self.MIN_INPUT_WORDS:
            logger.info(f"Skipping summary: transcript too short ({word_count} words < {self.MIN_INPUT_WORDS})")
            return ''

        token_ids = self.tokenizer(text, add_special_tokens=False)['input_ids']

        # Short video — single pass
        if len(token_ids) <= self.MAX_INPUT_TOKENS:
            return self._discard_if_hallucinated(
                self._generate(text, self.FINAL_MAX_TOKENS, self.FINAL_MIN_TOKENS)
            )

        # Long video — map per chunk
        step = self.MAX_INPUT_TOKENS - self.CHUNK_OVERLAP
        chunk_summaries = []
        for i in range(0, len(token_ids), step):
            chunk_ids = token_ids[i:i + self.MAX_INPUT_TOKENS]
            if len(chunk_ids) < self.MIN_CHUNK_TOKENS:
                break
            chunk_text = self.tokenizer.decode(chunk_ids, skip_special_tokens=True)
            s = self._generate(chunk_text, self.CHUNK_MAX_TOKENS, self.CHUNK_MIN_TOKENS)
            s = self._discard_if_hallucinated(s)
            if s:
                chunk_summaries.append(s)

        if not chunk_summaries:
            return ''

        if len(chunk_summaries) == 1:
            return chunk_summaries[0]

        combined = ' '.join(chunk_summaries)
        combined_token_count = len(
            self.tokenizer(combined, add_special_tokens=False)['input_ids']
        )
        logger.info(
            f"Multi-chunk summary: {len(chunk_summaries)} chunk summaries → reduce"
        )
        if combined_token_count <= self.MAX_INPUT_TOKENS:
            return self._discard_if_hallucinated(
                self._generate(combined, self.FINAL_MAX_TOKENS, self.FINAL_MIN_TOKENS)
            )
        return combined

    def _discard_if_hallucinated(self, summary: str) -> str:
        """Return '' if the output matches a known bart-large-cnn hallucination."""
        if is_hallucinated_summary(summary):
            logger.info(f"Discarding hallucinated summary: {summary[:100]}...")
            return ''
        return summary or ''

    @torch.no_grad()
    def _generate(self, text: str, max_tokens: int, min_tokens: int) -> str:
        inputs = self.tokenizer(
            _normalize(text),
            return_tensors='pt',
            max_length=self.MAX_INPUT_TOKENS,
            truncation=True,
        )
        output_ids = self.model.generate(
            input_ids=inputs['input_ids'],
            attention_mask=inputs['attention_mask'],
            max_length=max_tokens,
            min_length=min_tokens,
            num_beams=4,
            length_penalty=2.0,
            no_repeat_ngram_size=4,
            repetition_penalty=1.3,
            early_stopping=True,
        )[0]
        return self.tokenizer.decode(
            output_ids, skip_special_tokens=True, clean_up_tokenization_spaces=False
        ).strip()
