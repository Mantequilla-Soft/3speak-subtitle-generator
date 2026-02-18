"""
NLLB Translator (CTranslate2)
Fast CPU translation using Facebook's NLLB-200 model via CTranslate2 int8 inference
"""

import os
import logging
from typing import List, Dict, Any
import ctranslate2
from transformers import AutoTokenizer

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
