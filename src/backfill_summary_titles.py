"""
Backfill summary_en and title_translations for already-processed videos.

For each subtitle doc missing summary_en or title_translations:
  - Reads the local English SRT (always present after processing)
  - Generates a summary with mT5-XLSum (multilingual, handles English well)
  - Fetches title from the matching video doc (legacy/embed/audio)
  - Detects title language with langdetect and translates to all 15 langs
  - Saves via DatabaseManager (which dual-writes under hive_permlink for embed)

CLI:
  python /app/src/backfill_summary_titles.py [--limit N] [--owner X] [--dry-run] [--skip-summary] [--skip-title]

Idempotent — re-running skips entries already populated.
"""
import argparse
import logging
import os
import sys
from pathlib import Path
import re
import yaml

# Reuse the live components so the dual-write logic stays in one place
sys.path.insert(0, '/app/src')
from db_manager import DatabaseManager
from summarizer import Summarizer, is_hallucinated_summary
from translator import Translator
from subtitle_generator import SubtitleGenerator
from ipfs_fetcher import IPFSFetcher
from hive_client import fetch_hive_title, parse_embed_url, filename_to_title, is_generic_title

from langdetect import detect, DetectorFactory, LangDetectException
DetectorFactory.seed = 0  # deterministic detection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger('backfill')

SUBTITLES_DIR = Path('/app/subtitles')
SRT_TIME_RE = re.compile(r'^\d{2}:\d{2}:\d{2}[,.]\d{3}\s+-->\s+\d{2}:\d{2}:\d{2}[,.]\d{3}')


def read_srt_text(srt_path: Path) -> str:
    """Extract just the spoken text from an SRT file, dropping indices and timestamps."""
    if not srt_path.exists():
        return ''
    out = []
    for line in srt_path.read_text(encoding='utf-8', errors='ignore').splitlines():
        line = line.strip()
        if not line or line.isdigit() or SRT_TIME_RE.match(line):
            continue
        out.append(line)
    return ' '.join(out)


def find_video(db: DatabaseManager, author: str, permlink: str):
    """Return (video_doc, video_type) for a given author/permlink, or (None, None)."""
    v = db.videos_collection.find_one({'owner': author, 'permlink': permlink})
    if v:
        return v, 'legacy'
    v = db.embed_collection.find_one({'owner': author, 'permlink': permlink})
    if v:
        return v, 'embed'
    v = db.embed_audio_collection.find_one({'owner': author, 'permlink': permlink})
    if v:
        return v, 'audio'
    return None, None


def extract_title(video: dict, video_type: str) -> str:
    """Get title from local DB; fall back to Hive / embed_url / filename when missing."""
    owner = video.get('owner', '')

    if video_type == 'legacy':
        title = (video.get('title') or '').strip()
        if title:
            return title
        title = fetch_hive_title(owner, video.get('permlink', ''))
        return title or filename_to_title(video.get('originalFilename', ''))

    if video_type == 'audio':
        title = (video.get('title') or '').strip()
        if title:
            return title
        title = fetch_hive_title(
            video.get('hive_author') or owner,
            video.get('hive_permlink') or '',
        )
        return title or filename_to_title(video.get('originalFilename', ''))

    # embed
    title = (video.get('hive_title') or '').strip()
    if title:
        return title

    embed_title = (video.get('embed_title') or '').strip()
    if embed_title and not is_generic_title(embed_title):
        return embed_title

    # Try Hive by hive_permlink, then by embed_url
    hive_permlink = video.get('hive_permlink') or ''
    if hive_permlink:
        title = fetch_hive_title(video.get('hive_author') or owner, hive_permlink)
        if title:
            return title

    eu_author, eu_permlink = parse_embed_url(video.get('embed_url', ''))
    if eu_author and eu_permlink:
        title = fetch_hive_title(eu_author, eu_permlink)
        if title:
            return title

    # Last resort: clean up original upload filename
    return filename_to_title(video.get('originalFilename', ''))


def detect_lang(text: str, fallback: str = 'en') -> str:
    """langdetect over short strings is noisy — keep simple, fallback on error."""
    try:
        return detect(text) if text and len(text.strip()) >= 3 else fallback
    except LangDetectException:
        return fallback


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=0, help='Process at most N videos that needed work (0 = no limit)')
    parser.add_argument('--recent', type=int, default=0, help='Only consider the N most recently transcribed videos (0 = all)')
    parser.add_argument('--owner', default='', help='Only process this owner')
    parser.add_argument('--permlink', default='', help='Only process this permlink (use with --owner)')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--skip-summary', action='store_true')
    parser.add_argument('--skip-title', action='store_true')
    args = parser.parse_args()

    with open('/app/config.yaml') as f:
        config = yaml.safe_load(f)

    logger.info("Loading models...")
    db = DatabaseManager(config)
    summarizer = None if args.skip_summary else Summarizer(config)
    translator = Translator(config)
    subtitle_gen = SubtitleGenerator(config)
    ipfs = IPFSFetcher(config)
    enable_ipfs = os.getenv('ENABLE_IPFS_PIN', 'true').lower() == 'true'
    enable_remote_pin = os.getenv('ENABLE_REMOTE_PIN', 'false').lower() == 'true'
    remote_pin_url = os.getenv('REMOTE_PIN_URL', 'https://ipfs.3speak.tv/api/v0/pin/add')
    logger.info(f"Models loaded (ipfs={enable_ipfs}, remote_pin={enable_remote_pin})")

    all_title_codes = [l['code'] for l in config['languages']] + \
                      [l['code'] for l in config.get('premium_languages', [])]

    # Find docs needing work. Skip aliases — they get mirrored automatically.
    query = {'is_alias': {'$ne': True}}
    if args.owner:
        query['author'] = args.owner
    if args.permlink:
        query['permlink'] = args.permlink

    cursor = db.subtitles_collection.find(
        query,
        {'author': 1, 'permlink': 1, 'summary_en': 1, 'title_translations': 1, 'meta_cid': 1, '_id': 0},
    )
    if args.recent:
        cursor = cursor.sort('created_at', -1).limit(args.recent)
        logger.info(f"Restricting to the {args.recent} most recently transcribed videos")

    processed = 0
    skipped_done = 0
    skipped_no_srt = 0
    errors = 0

    for doc in cursor:
        author = doc['author']
        permlink = doc['permlink']
        existing_summary = doc.get('summary_en') or ''
        existing_is_bad = bool(existing_summary) and is_hallucinated_summary(existing_summary)

        # Clear hallucinated summaries up front (primary + any hive_permlink mirrors)
        if existing_is_bad:
            logger.info(f"  {author}/{permlink}: clearing hallucinated saved summary")
            if not args.dry_run:
                db.subtitles_collection.update_one(
                    {'author': author, 'permlink': permlink},
                    {'$unset': {'summary_en': ''}},
                )
                db.subtitles_collection.update_many(
                    {'author': author, 'embed_permlink': permlink},
                    {'$unset': {'summary_en': ''}},
                )
            existing_summary = ''

        needs_summary = not args.skip_summary and not existing_summary
        needs_title = not args.skip_title and not doc.get('title_translations')

        video, video_type = find_video(db, author, permlink)
        if not video:
            if needs_summary or needs_title:
                logger.warning(f"  {author}/{permlink}: video doc not found, skipping")
                errors += 1
            else:
                skipped_done += 1
            continue

        # Fast path: nothing to regenerate. Ensure meta.json exists on disk AND
        # is pinned to IPFS with its CID in mongo, then skip.
        meta_path = Path(subtitle_gen.create_meta_path(author, permlink))
        if not needs_summary and not needs_title:
            if not args.dry_run:
                hive_pl = (video.get('hive_permlink') or '') if video_type in ('embed', 'audio') else ''
                wrote = True
                if not meta_path.exists():
                    wrote = subtitle_gen.write_meta(
                        author, permlink,
                        doc.get('summary_en'),
                        doc.get('title_translations'),
                        hive_permlink=hive_pl or None,
                    )
                if wrote and enable_ipfs and not doc.get('meta_cid') and meta_path.exists():
                    meta_cid = ipfs.add_to_ipfs(str(meta_path))
                    if meta_cid:
                        if enable_remote_pin:
                            ipfs.pin_remote(meta_cid, remote_pin_url)
                        db.save_meta_cid(author, permlink, meta_cid, video_type=video_type)
                        logger.info(f"  {author}/{permlink}: meta pinned ({meta_cid})")
            skipped_done += 1
            continue

        if args.limit and processed >= args.limit:
            break

        logger.info(f"[{processed + 1}] {author}/{permlink} [{video_type}]")

        # Detect probable source language from title (used by both summary & title sections)
        title = extract_title(video, video_type)
        src_lang = detect_lang(title, fallback='en') if title else 'en'
        author_dir = SUBTITLES_DIR / author

        # ----- Summary -----
        if needs_summary:
            english_text = ''
            # Prefer the source-language SRT (Whisper original) translated cleanly
            # to English. Falls back to en.srt only when source isn't available.
            source_srt = author_dir / f"{permlink}.{src_lang}.srt"
            if src_lang != 'en' and source_srt.exists():
                source_text = read_srt_text(source_srt)
                if source_text:
                    try:
                        english_text = translator.translate_long_text(
                            source_text, src_lang, 'en'
                        )
                    except Exception as e:
                        logger.warning(f"  source→en translation failed ({src_lang}): {e}")
            if not english_text:
                english_text = read_srt_text(author_dir / f"{permlink}.en.srt")

            if not english_text:
                logger.info(f"  no usable SRT, skipping summary")
                skipped_no_srt += 1
            else:
                try:
                    summary_en = summarizer.summarize(english_text)
                    if summary_en:
                        logger.info(f"  Summary: {summary_en[:160]}{'...' if len(summary_en) > 160 else ''}")
                        if not args.dry_run:
                            db.save_summary(author, permlink, summary_en, video_type=video_type)
                except Exception as e:
                    logger.error(f"  Summary failed: {e}")
                    errors += 1

        # ----- Title translations -----
        if needs_title:
            if not title:
                logger.info(f"  no title, skipping title translations")
            else:
                try:
                    translations = {}
                    for tgt in all_title_codes:
                        if tgt == src_lang:
                            translations[tgt] = title
                        else:
                            translations[tgt] = translator.translate_text(title, src_lang, tgt)
                    if not args.dry_run:
                        db.save_title_translations(
                            author, permlink, translations, video_type=video_type
                        )
                    logger.info(f"  Title ({src_lang}) → {len(translations)} langs")
                except Exception as e:
                    logger.error(f"  Title translation failed: {e}")
                    errors += 1

        # Write .meta.json, pin to IPFS, save meta_cid to mongo for API consumption.
        if not args.dry_run:
            try:
                meta_doc = db.subtitles_collection.find_one(
                    {'author': author, 'permlink': permlink},
                    {'summary_en': 1, 'title_translations': 1, '_id': 0},
                ) or {}
                summary_en = meta_doc.get('summary_en')
                translations = meta_doc.get('title_translations')
                if summary_en or translations:
                    hive_pl = (video.get('hive_permlink') or '') if video_type in ('embed', 'audio') else ''
                    if subtitle_gen.write_meta(
                        author, permlink, summary_en, translations,
                        hive_permlink=hive_pl or None,
                    ) and enable_ipfs:
                        meta_cid = ipfs.add_to_ipfs(str(meta_path))
                        if meta_cid:
                            if enable_remote_pin:
                                ipfs.pin_remote(meta_cid, remote_pin_url)
                            db.save_meta_cid(author, permlink, meta_cid, video_type=video_type)
                            logger.info(f"  Meta pinned: {meta_cid}")
            except Exception as e:
                logger.warning(f"  Could not write/pin meta.json: {e}")

        processed += 1

    logger.info("")
    logger.info(f"Done. processed={processed}, already-done={skipped_done}, "
                f"no-srt={skipped_no_srt}, errors={errors}")


if __name__ == '__main__':
    main()
