"""
NLLB Translator
Handles translation using Facebook's NLLB-200 model
"""

import logging
from typing import List, Dict, Any
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
import torch

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
    """Handles text translation using NLLB"""

    def __init__(self, config: dict):
        """Initialize NLLB translation model"""
        self.config = config['models']['translation']
        self.model = None
        self.tokenizer = None
        self._load_model()

    def _load_model(self):
        """Load NLLB model and tokenizer"""
        try:
            logger.info(f"Loading translation model: {self.config['model']}")

            self.tokenizer = AutoTokenizer.from_pretrained(
                self.config['model'],
                cache_dir="/app/models"
            )

            self.model = AutoModelForSeq2SeqLM.from_pretrained(
                self.config['model'],
                cache_dir="/app/models",
                torch_dtype=torch.float32
            )

            # Move to device
            if self.config['device'] == 'cpu':
                self.model = self.model.to('cpu')

            logger.info("Translation model loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load translation model: {e}")
            raise

    def translate_segments(self, segments: List[Dict[str, Any]],
                          source_lang: str, target_lang: str) -> List[Dict[str, Any]]:
        """
        Translate segments to target language while preserving timestamps

        Args:
            segments: List of segment dictionaries with 'start', 'end', 'text'
            source_lang: Source language code (ISO 639-1)
            target_lang: Target language code (ISO 639-1)

        Returns:
            List of translated segments with preserved timestamps
        """
        try:
            # Map language codes
            src_code = LANGUAGE_MAP.get(source_lang, 'eng_Latn')
            tgt_code = LANGUAGE_MAP.get(target_lang, 'eng_Latn')

            logger.info(f"Translating {len(segments)} segments from {source_lang} to {target_lang}")

            translated_segments = []

            # Translate in batches for efficiency
            batch_size = 8
            for i in range(0, len(segments), batch_size):
                batch = segments[i:i + batch_size]
                texts = [seg['text'] for seg in batch]

                # Translate batch
                translated_texts = self._translate_batch(texts, src_code, tgt_code)

                # Create translated segments with original timestamps
                for seg, translated_text in zip(batch, translated_texts):
                    translated_segments.append({
                        'start': seg['start'],
                        'end': seg['end'],
                        'text': translated_text
                    })

            logger.info(f"Translation complete: {target_lang}")
            return translated_segments

        except Exception as e:
            logger.error(f"Translation failed for {target_lang}: {e}")
            raise

    def _translate_batch(self, texts: List[str], src_code: str, tgt_code: str) -> List[str]:
        """
        Translate a batch of texts

        Args:
            texts: List of text strings
            src_code: NLLB source language code
            tgt_code: NLLB target language code

        Returns:
            List of translated strings
        """
        try:
            # Set source language
            self.tokenizer.src_lang = src_code

            # Tokenize
            inputs = self.tokenizer(
                texts,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=512
            ).to(self.model.device)

            # Get target language token
            forced_bos_token_id = self.tokenizer.lang_code_to_id[tgt_code]

            # Estimate a sane output cap: 3× input tokens, min 32, max 128
            input_len = inputs['input_ids'].shape[1]
            max_new_tokens = max(32, min(128, input_len * 3))

            # Generate translations (greedy — num_beams=1 is ~5× faster on CPU
            # with negligible quality loss for subtitle-length sentences)
            with torch.no_grad():
                generated_tokens = self.model.generate(
                    **inputs,
                    forced_bos_token_id=forced_bos_token_id,
                    max_new_tokens=max_new_tokens,
                    num_beams=5,
                    no_repeat_ngram_size=3,      # prevent any 3-gram from repeating
                    repetition_penalty=1.3,       # penalize repeated tokens
                )

            # Decode
            translations = self.tokenizer.batch_decode(
                generated_tokens,
                skip_special_tokens=True
            )

            return translations

        except Exception as e:
            logger.error(f"Batch translation failed: {e}")
            # Return original texts on error
            return texts

    def detect_language(self, text: str) -> str:
        """
        Detect language of text (simplified version)

        Args:
            text: Input text

        Returns:
            ISO 639-1 language code
        """
        # For now, we rely on Whisper's language detection
        # Could add langdetect library if needed
        return 'en'
