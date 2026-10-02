"""
Read the plain-text transcript back out of a locally-saved subtitle file.

The raw transcript is not stored in Mongo — only per-language SRT CIDs and the
optional English summary. But when ENABLE_LOCAL_SAVE is on, every subtitle track
is also written to /app/subtitles/<author>/<permlink>.<lang>.srt. That gives the
re-tagger the same English text the live pipeline had at tagging time, without
re-transcribing or fetching from IPFS.

The English track is preferred: for English-source videos it is the transcript,
and for other languages it is the NLLB translation — either way it is English,
which is what bart-large-mnli expects.
"""

import logging
import os
import re
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_SUBTITLES_DIR = '/app/subtitles'

# SRT cue: an index line, a timestamp line, then one or more text lines.
_TIMESTAMP_RE = re.compile(r'-->')
_INDEX_RE = re.compile(r'^\d+$')


def srt_to_text(srt_content: str, max_chars: int = 6000) -> str:
    """
    Flatten SRT content to plain prose.

    Drops index and timestamp lines, and collapses consecutive duplicate cue
    lines — Whisper commonly repeats a caption across several cues, which would
    otherwise swamp the classifier input.
    """
    lines = []
    prev = None
    for raw in srt_content.splitlines():
        line = raw.strip()
        if not line or _INDEX_RE.match(line) or _TIMESTAMP_RE.search(line):
            continue
        if line == prev:  # de-dupe repeated captions
            continue
        lines.append(line)
        prev = line
    return ' '.join(lines)[:max_chars]


def local_srt_path(author: str, permlink: str, lang: str = 'en',
                   base_dir: str = DEFAULT_SUBTITLES_DIR) -> str:
    return os.path.join(base_dir, author, f"{permlink}.{lang}.srt")


def local_transcript_text(author: str, permlink: str, lang: str = 'en',
                          base_dir: str = DEFAULT_SUBTITLES_DIR,
                          max_chars: int = 6000) -> str:
    """Plain-text transcript from the local SRT, or '' if unavailable."""
    path = local_srt_path(author, permlink, lang, base_dir)
    if not os.path.exists(path):
        return ''
    try:
        with open(path, encoding='utf-8', errors='ignore') as f:
            return srt_to_text(f.read(), max_chars=max_chars)
    except OSError as e:
        logger.warning(f"Could not read SRT {path}: {e}")
        return ''


def word_count(text: Optional[str]) -> int:
    return len(text.split()) if text else 0
