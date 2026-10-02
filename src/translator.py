"""
NLLB Translator (CTranslate2)
Fast CPU translation using Facebook's NLLB-200 model via CTranslate2 int8 inference
"""

import os
import re
import logging
from typing import List, Dict, Any
import ctranslate2
from transformers import AutoTokenizer

_SENT_SPLIT_RE = re.compile(r'(?<=[.!?。！？])\s+')

logger = logging.getLogger(__name__)


# Language code mapping: ISO 639-1 to NLLB codes
LANGUAGE_MAP = {
    'en': 'eng_Latn',
    'es': 'spa_Latn',
    'fr': 'fra_Latn',
    'de': 'deu_Latn',
    'pt': 'por_Latn',
    'ru': 'rus_Cyrl',
    'ja': 'jpn_Jpan',
    'zh': 'zho_Hans',
    'ar': 'arb_Arab',
    'hi': 'hin_Deva',
    'ko': 'kor_Hang',
    'it': 'ita_Latn',
    'tr': 'tur_Latn',
    'vi': 'vie_Latn',
    'pl': 'pol_Latn',
    'uk': 'ukr_Cyrl',
    'nl': 'nld_Latn',
    'th': 'tha_Thai',
    'id': 'ind_Latn',
    'bn': 'ben_Beng',
    'el': 'ell_Grek',
}


class Translator:
    """Handles text translation using NLLB via CTranslate2"""

    def __init__(self, config: dict):
        self.config = config['models']['translation']
        self.translator = None
        self.tokenizer = None
        self._load_model()

    def _load_model(self):
        model_name = self.config['model']
        ct2_model = self.config.get('ct2_model', 'entai2965/nllb-200-distilled-600M-ctranslate2')
        compute_type = self.config.get('compute_type', 'int8')
        cache_dir = "/app/models"
        ct2_dir = os.path.join(cache_dir, "nllb-ct2")

        # Load tokenizer from original HuggingFace model
        logger.info(f"Loading tokenizer for {model_name}")
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_name, cache_dir=cache_dir
        )

        # Download pre-converted CTranslate2 model on first run
        if not os.path.exists(os.path.join(ct2_dir, "model.bin")):
            from huggingface_hub import snapshot_download
            logger.info(f"Downloading CTranslate2 model: {ct2_model}")
            snapshot_download(ct2_model, local_dir=ct2_dir)
            logger.info("Download complete")

        # Load CTranslate2 translator (quantizes on-the-fly if model was saved as float16)
        logger.info(f"Loading CTranslate2 model ({compute_type})")
        self.translator = ctranslate2.Translator(
            ct2_dir,
            device=self.config.get('device', 'cpu'),
            compute_type=compute_type,
        )
        logger.info("CTranslate2 translation model loaded successfully")

    def translate_text(self, text: str, source_lang: str, target_lang: str) -> str:
        """Translate a single short string (title, summary, etc.)."""
        if not text or not text.strip():
            return ''
        if source_lang == target_lang:
            return text
        out = self.translate_segments(
            [{'start': 0, 'end': 0, 'text': text}],
            source_lang, target_lang,
        )
        return out[0]['text'] if out else ''

    def translate_long_text(self, text: str, source_lang: str, target_lang: str,
                            chunk_chars: int = 400) -> str:
        """
        Translate a long passage by grouping full sentences into ~chunk_chars-sized
        batches before sending to NLLB. Preserves sentence flow so the output reads
        as natural English (instead of the chopped subtitle-style output you get
        from translating per-segment).
        """
        if not text or not text.strip():
            return ''
        if source_lang == target_lang:
            return text

        sentences = [s for s in _SENT_SPLIT_RE.split(text.strip()) if s]
        chunks: List[str] = []
        current = ''
        for s in sentences:
            if not current:
                current = s
            elif len(current) + len(s) + 1 <= chunk_chars:
                current = current + ' ' + s
            else:
                chunks.append(current)
                current = s
        if current:
            chunks.append(current)

        if not chunks:
            return ''

        segments = [{'start': 0, 'end': 0, 'text': c} for c in chunks]
        translated = self.translate_segments(segments, source_lang, target_lang)
        return ' '.join(s.get('text', '') for s in translated).strip()

    def translate_segments(self, segments: List[Dict[str, Any]],
                          source_lang: str, target_lang: str) -> List[Dict[str, Any]]:
        """
        Translate segments to target language while preserving timestamps.

        Args:
            segments: List of segment dicts with 'start', 'end', 'text'
            source_lang: Source language code (ISO 639-1)
            target_lang: Target language code (ISO 639-1)

        Returns:
            List of translated segments with preserved timestamps
        """
        try:
            src_code = LANGUAGE_MAP.get(source_lang, 'eng_Latn')
            tgt_code = LANGUAGE_MAP.get(target_lang, 'eng_Latn')

            logger.info(f"Translating {len(segments)} segments: {source_lang} → {target_lang}")

            # Tokenize all segments
            self.tokenizer.src_lang = src_code
            all_tokens = []
            for seg in segments:
                ids = self.tokenizer.encode(seg['text'])
                tokens = self.tokenizer.convert_ids_to_tokens(ids)
                all_tokens.append(tokens)

            target_prefix = [[tgt_code]] * len(segments)

            # Translate (CT2 handles internal batching via max_batch_size)
            beam_size = self.config.get('beam_size', 1)
            results = self.translator.translate_batch(
                all_tokens,
                target_prefix=target_prefix,
                beam_size=beam_size,
                no_repeat_ngram_size=3,
                repetition_penalty=1.3,
                max_decoding_length=256,
                max_batch_size=32,
            )

            # Decode and build output
            translated_segments = []
            for seg, result in zip(segments, results):
                output_tokens = result.hypotheses[0]
                output_ids = self.tokenizer.convert_tokens_to_ids(output_tokens)
                translation = self.tokenizer.decode(output_ids, skip_special_tokens=True)
                translated_segments.append({
                    'start': seg['start'],
                    'end': seg['end'],
                    'text': translation,
                })

            logger.info(f"Translation complete: {target_lang}")
            return translated_segments

        except Exception as e:
            logger.error(f"Translation failed for {target_lang}: {e}")
            raise
