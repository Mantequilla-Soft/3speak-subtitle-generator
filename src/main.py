"""
3Speak Subtitle Generator - Main Orchestration
Processes videos from MongoDB, generates subtitles in multiple languages, and tags content
"""

import os
import sys
import logging
import yaml
import time
from datetime import datetime
from typing import Dict, Any, List

# Import our modules
from db_manager import DatabaseManager
from ipfs_fetcher import IPFSFetcher
from transcriber import Transcriber, NoAudioError, NoSpeechError
from translator import Translator
from tagger import ContentTagger
from tag_live_v2 import tag_video_v2
from subtitle_generator import SubtitleGenerator
from summarizer import Summarizer
from hive_client import (
    fetch_hive_post, fetch_hive_title, parse_embed_url, filename_to_title, is_generic_title,
)
from video_meta import hive_reference, merge_hive_post, normalize_video_metadata

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class SubtitleService:
    """Main service orchestrating subtitle generation"""

    def __init__(self, config_path: str = '/app/config.yaml'):
        """Initialize service with configuration"""
        logger.info("Starting 3Speak Subtitle Generator Service")

        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Initialize components
        logger.info("Initializing components...")
        self.db = DatabaseManager(self.config)
        self.ipfs_fetcher = IPFSFetcher(self.config)
        self.transcriber = Transcriber(self.config)
        self.translator = Translator(self.config)
        self.tagger = ContentTagger(self.config)
        self.subtitle_gen = SubtitleGenerator(self.config)
        self.summarizer = Summarizer(self.config)

        # Get language list with duration thresholds
        self.language_configs = self.config['languages']
        self.premium_language_configs = self.config.get('premium_languages', [])
        all_codes = [lang['code'] for lang in self.language_configs]
        premium_codes = [lang['code'] for lang in self.premium_language_configs]
        logger.info(f"Target languages: {', '.join(all_codes)}")
        if premium_codes:
            logger.info(f"Premium-only languages: {', '.join(premium_codes)}")

        # Premium users cache — refreshed every poll cycle
        self._premium_users: set = set()
        self._premium_refresh_counter = 0

        # Feature flags from environment variables
        self.enable_local_save  = os.getenv('ENABLE_LOCAL_SAVE',  'true').lower() == 'true'
        self.enable_ipfs_pin   = os.getenv('ENABLE_IPFS_PIN',   'false').lower() == 'true'
        self.enable_remote_pin = os.getenv('ENABLE_REMOTE_PIN', 'false').lower() == 'true'
        self.enable_mongo_write = os.getenv('ENABLE_MONGO_WRITE', 'false').lower() == 'true'
        self.remote_pin_url    = os.getenv('REMOTE_PIN_URL', 'https://ipfs.3speak.tv/api/v0/pin/add')
        # When set, process only this one video: "owner/permlink"
        self.process_only      = os.getenv('PROCESS_ONLY', '')
        # Start date: only process videos created on or after this date (YYYY-MM-DD)
        start_date_str = os.getenv('START_DATE', '')
        self.start_date = (
            datetime.strptime(start_date_str, '%Y-%m-%d') if start_date_str
            else None
        )
        audio_start_date_str = os.getenv('AUDIO_START_DATE', '')
        self.audio_start_date = (
            datetime.strptime(audio_start_date_str, '%Y-%m-%d') if audio_start_date_str
            else None
        )

        self.max_retries = self.config.get('processing', {}).get('max_retries', 3)

        logger.info(f"ENABLE_LOCAL_SAVE={self.enable_local_save}  "
                    f"ENABLE_IPFS_PIN={self.enable_ipfs_pin}  "
                    f"ENABLE_REMOTE_PIN={self.enable_remote_pin}  "
                    f"ENABLE_MONGO_WRITE={self.enable_mongo_write}")

        logger.info("Service initialized successfully")

    def _extract_title(self, video: Dict[str, Any], video_type: str) -> str:
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

        return filename_to_title(video.get('originalFilename', ''))

    def _video_metadata(self, video: Dict[str, Any]) -> Dict[str, Any]:
        """
        Title / body / tags / community for a video: from the local DB where
        present, from the Hive post where not.

        Only ~33% of embed docs carry hive_body and ~27% hive_title, so without
        this the classifier would fall back to the transcript for most videos.
        Fetched posts are cached in our own collection — we never write back to
        3speak's embed-video.
        """
        meta = normalize_video_metadata(video)
        if meta['body'] and meta['hive_tags']:
            return meta

        author, permlink = hive_reference(video)
        if not author or not permlink:
            return meta

        post = self.db.get_hive_meta(author, permlink)
        if post is None:
            post = fetch_hive_post(author, permlink)
            if not post:
                logger.debug(f"No Hive post found for @{author}/{permlink}")
                return meta
            self.db.save_hive_meta(author, permlink, post)

        return merge_hive_post(meta, post)

    def process_video(self, video: Dict[str, Any], is_premium: bool = False) -> bool:
        """
        Process a single video: transcribe, translate, tag, and save

        Args:
            video: Video document from MongoDB
            is_premium: When True, also generate subtitles for premium_languages

        Returns:
            True if successful
        """
        video_start = time.time()
        try:
            author = video.get('owner', 'unknown')
            permlink = video.get('permlink', 'unknown')
            video_type = video.get('_video_type', 'legacy')

            # Extract CID based on video type
            if video_type == 'audio':
                cid = video.get('audio_cid')
            elif video_type == 'embed':
                cid = video.get('manifest_cid')
            else:
                filename = video.get('filename', '')
                cid = filename.removeprefix('ipfs://') if filename else None

            # For legacy videos, prefer video_v2 HLS manifest (480p) over full file
            video_v2 = video.get('video_v2', '') if video_type == 'legacy' else ''
            hls_cid = None
            if video_v2:
                # Format: ipfs://CID/manifest.m3u8
                hls_cid = video_v2.removeprefix('ipfs://').removesuffix('/manifest.m3u8')

            if not cid and not hls_cid:
                logger.warning(f"Video {author}/{permlink} has no CID, skipping")
                return False

            logger.info(f"\n{'=' * 80}")
            logger.info(f"Processing: {author}/{permlink} [{video_type}]")
            logger.info(f"CID: {hls_cid or cid}")
            logger.info(f"{'=' * 80}\n")

            # Step 1: Download video/audio
            logger.info("Step 1/6: Downloading from IPFS...")
            if video_type == 'embed':
                video_path = self.ipfs_fetcher.download_hls_video(cid, author, permlink)
            elif hls_cid:
                # Legacy with video_v2: use HLS (480p) instead of full file
                video_path = self.ipfs_fetcher.download_hls_video(hls_cid, author, permlink)
            else:
                # Legacy without video_v2, or audio: direct file download
                video_path = self.ipfs_fetcher.download_video(cid, author, permlink)
            if not video_path:
                logger.error("Failed to download video")
                return False

            # Step 2: Transcribe (auto-falls back to English if detected language not in config)
            logger.info("Step 2/6: Transcribing audio...")
            configured_codes = {lang['code'] for lang in self.language_configs}
            hotwords = self.db.get_hotwords()
            start_time = time.time()
            segments, detected_language = self.transcriber.transcribe(
                video_path, allowed_languages=configured_codes, hotwords=hotwords
            )
            transcription_time = time.time() - start_time
            logger.info(f"Transcription completed in {transcription_time:.1f}s")

            # Apply text corrections (e.g. "p.d." -> "PeakD")
            corrections = self.db.get_corrections()
            if corrections:
                segments = self.transcriber.apply_corrections(segments, corrections)
                logger.info(f"Applied {len(corrections)} text corrections")

            # Get full transcript for tagging + source-language segments for translation reuse
            full_transcript = self.transcriber.get_transcript_text(segments)
            segment_dicts = self.transcriber.get_segments_for_language(segments)

            # Calculate video duration from last segment
            video_duration_min = segments[-1].end / 60.0 if segments else 0
            logger.info(f"Video duration: {video_duration_min:.1f} minutes")

            # Generate summary + title translations (independent of premium status,
            # always all 15 langs for title). Skip if already saved on the doc.
            existing_doc = self.db.subtitles_collection.find_one(
                {'author': author, 'permlink': permlink},
                {'summary_en': 1, 'title_translations': 1, '_id': 0},
            ) or {}

            # Step 3: English summary. This runs before tagging because the summary
            # is the best classifier input available: short, on-topic and English.
            # bart-large-mnli scores English labels, so a source-language transcript
            # yields near-random tags.
            logger.info("Step 3/6: Summarizing content...")
            summary_en = existing_doc.get('summary_en') or ''
            english_text = full_transcript if detected_language == 'en' else ''

            if not summary_en:
                try:
                    # bart-large-cnn is English-only. For non-English source,
                    # translate the transcript to English first via NLLB.
                    if detected_language != 'en':
                        logger.info("Translating transcript to English for summarization...")
                        # Use Whisper's flowing transcript (natural sentences),
                        # not subtitle-chopped segments — NLLB produces much better
                        # English when given full-sentence inputs.
                        english_text = self.translator.translate_long_text(
                            full_transcript, detected_language, 'en'
                        )

                    logger.info("Generating English summary (BART-Large-CNN)...")
                    summary_en = self.summarizer.summarize(english_text)
                    if summary_en:
                        logger.info(f"  Summary: {summary_en[:160]}{'...' if len(summary_en) > 160 else ''}")
                        if self.enable_mongo_write:
                            self.db.save_summary(author, permlink, summary_en, video_type=video_type)
                except Exception as e:
                    logger.error(f"  Summary generation failed: {e}")

            # Step 4: Tags, from the author's Hive metadata plus the English content.
            logger.info("Step 4/6: Analyzing content and generating tags...")
            metadata = self._video_metadata(video)
            tag_result = self.tagger.generate_tags(
                full_transcript,
                segments,
                metadata=metadata,
                content_text=summary_en or english_text,
            )
            if self.enable_mongo_write:
                self.db.save_tags(
                    author, permlink, tag_result['tags'],
                    scores=tag_result['scores'],
                    evidence=tag_result['evidence'],
                    model=tag_result['model'],
                    source_lang=detected_language,
                )

                # v2 faceted tags -> the parallel *_v2 fields (see TAXONOMY_V2.md).
                # Reuses the classifier loaded above, so no extra model in memory.
                # Never allowed to break transcription: v2 is strictly additive.
                try:
                    tag_video_v2(
                        self.db, author, permlink,
                        metadata=metadata,
                        classifier=self.tagger.classifier,
                        text=self.tagger.build_classifier_input(
                            metadata, summary_en or english_text),
                        config=self.config,
                        # frames come from the copy downloaded in Step 1
                        video_path=video_path,
                    )
                except Exception as e:
                    logger.error(f"  v2 tagging failed (non-fatal): {e}")

            if not existing_doc.get('title_translations'):
                try:
                    title = self._extract_title(video, video_type)
                    if title:
                        all_title_codes = [l['code'] for l in self.language_configs] + \
                                          [l['code'] for l in self.premium_language_configs]
                        translations = {}
                        for tgt in all_title_codes:
                            if tgt == detected_language:
                                translations[tgt] = title
                            else:
                                translations[tgt] = self.translator.translate_text(
                                    title, detected_language, tgt
                                )
                        if self.enable_mongo_write:
                            self.db.save_title_translations(
                                author, permlink, translations, video_type=video_type
                            )
                        logger.info(f"  Title translated to {len(translations)} languages")
                except Exception as e:
                    logger.error(f"  Title translation failed: {e}")

            # Write .meta.json locally, pin it to IPFS, and store the CID in mongo
            # so the public translate.3speak.tv API can expose it the same way it
            # serves subtitle CIDs.
            try:
                meta_doc = self.db.subtitles_collection.find_one(
                    {'author': author, 'permlink': permlink},
                    {'summary_en': 1, 'title_translations': 1, '_id': 0},
                ) or {}
                summary_en = meta_doc.get('summary_en')
                title_translations = meta_doc.get('title_translations')
                if summary_en or title_translations:
                    hive_pl = video.get('hive_permlink') if video_type in ('embed', 'audio') else None
                    meta_path = self.subtitle_gen.create_meta_path(author, permlink)
                    self.subtitle_gen.write_meta(
                        author, permlink, summary_en, title_translations,
                        hive_permlink=hive_pl,
                    )
                    if self.enable_ipfs_pin:
                        meta_cid = self.ipfs_fetcher.add_to_ipfs(meta_path)
                        if meta_cid and self.enable_remote_pin:
                            self.ipfs_fetcher.pin_remote(meta_cid, self.remote_pin_url, meta_path)
                        if meta_cid and self.enable_mongo_write:
                            self.db.save_meta_cid(author, permlink, meta_cid, video_type=video_type)
                            logger.info(f"  Meta pinned: {meta_cid}")
                    if not self.enable_local_save:
                        try:
                            os.remove(meta_path)
                        except OSError:
                            pass
            except Exception as e:
                logger.warning(f"  Could not pin meta.json: {e}")

            active_lang_configs = list(self.language_configs)
            if is_premium and self.premium_language_configs:
                active_lang_configs += self.premium_language_configs
                logger.info(f"Premium user — adding {len(self.premium_language_configs)} extra languages")

            eligible_languages = [
                lang['code'] for lang in active_lang_configs
                if lang.get('max_duration', 0) == 0
                or video_duration_min <= lang.get('max_duration', 0)
            ]
            skipped = len(active_lang_configs) - len(eligible_languages)
            if skipped:
                logger.info(f"Skipping {skipped} languages (video too long)")

            # Skip languages that already have subtitles in MongoDB
            existing = self.db.get_existing_subtitle_languages(author, permlink)
            if existing:
                eligible_languages = [l for l in eligible_languages if l not in existing]
                logger.info(f"Already processed: {', '.join(existing)}")

            if not eligible_languages:
                logger.info(f"All languages already processed for {author}/{permlink}, skipping")
                self.ipfs_fetcher.cleanup_video(video_path)
                return True

            logger.info(f"Step 5/6: Generating subtitles for {len(eligible_languages)} languages: {', '.join(eligible_languages)}")

            # Process each language
            for lang_code in eligible_languages:
                try:
                    logger.info(f"  Processing language: {lang_code}")

                    # Translate segments
                    if lang_code == detected_language:
                        # Use original transcription for detected language
                        translated_segments = segment_dicts
                    else:
                        # Translate to target language
                        translated_segments = self.translator.translate_segments(
                            segment_dicts,
                            detected_language,
                            lang_code
                        )

                    # Generate SRT file
                    subtitle_path = self.subtitle_gen.create_subtitle_path(
                        author, permlink, lang_code
                    )

                    success = self.subtitle_gen.generate_srt(
                        translated_segments,
                        subtitle_path
                    )

                    if success:
                        # Validate SRT format
                        self.subtitle_gen.validate_srt(subtitle_path)

                        # Add to IPFS and pin
                        subtitle_cid = None
                        remote_ok = None
                        if self.enable_ipfs_pin:
                            subtitle_cid = self.ipfs_fetcher.add_to_ipfs(subtitle_path)
                            if subtitle_cid and self.enable_remote_pin:
                                remote_ok = self.ipfs_fetcher.pin_remote(
                                    subtitle_cid, self.remote_pin_url, subtitle_path)

                        # Save to MongoDB
                        if self.enable_mongo_write and subtitle_cid:
                            self.db.save_subtitle(
                                author, permlink, cid or hls_cid, lang_code, subtitle_cid,
                                video_type=video_type,
                                video_created_at=video.get('created') or video.get('createdAt'),
                            )
                            # Track pin outcome so a retry job can re-pin what the
                            # (currently flaky) remote pin endpoint dropped.
                            if remote_ok is not None:
                                self.db.record_remote_pin(
                                    author, permlink, lang_code, subtitle_cid,
                                    remote_ok, video_type=video_type)

                        # For embed videos cross-posted to Hive, mirror the file
                        # under hive_permlink so URLs like /subtitles/<owner>/3speak-<ts>.<lang>.srt resolve.
                        if self.enable_local_save and video_type in ('embed', 'audio'):
                            hive_permlink = video.get('hive_permlink')
                            if hive_permlink and hive_permlink != permlink:
                                alias_path = self.subtitle_gen.create_subtitle_path(
                                    author, hive_permlink, lang_code
                                )
                                try:
                                    if os.path.lexists(alias_path):
                                        os.remove(alias_path)
                                    os.symlink(os.path.basename(subtitle_path), alias_path)
                                except OSError as e:
                                    logger.warning(f"  Could not create hive_permlink alias: {e}")

                        # Remove local file if local save is disabled
                        if not self.enable_local_save:
                            try:
                                os.remove(subtitle_path)
                            except OSError:
                                pass

                        logger.info(f"  ✓ {lang_code} subtitle saved"
                                    + (f" (IPFS: {subtitle_cid})" if subtitle_cid else ""))
                    else:
                        logger.warning(f"  ✗ Failed to generate {lang_code} subtitle")

                except Exception as e:
                    logger.error(f"  ✗ Error processing {lang_code}: {e}")
                    continue

            # Step 6: Cleanup
            logger.info("Step 6/6: Cleaning up temporary files...")
            if self.config['processing']['cleanup_after_processing']:
                self.ipfs_fetcher.cleanup_video(video_path)

            # Record total processing time and video duration
            processing_seconds = round(time.time() - video_start)
            video_duration_secs = round(segments[-1].end) if segments else 0
            if self.enable_mongo_write:
                self.db.save_processing_time(author, permlink, processing_seconds,
                                             video_duration_seconds=video_duration_secs)
            logger.info(f"\n✓ Successfully processed {author}/{permlink} in {processing_seconds}s\n")
            return True

        except (NoAudioError, NoSpeechError) as e:
            logger.warning(f"Permanently skipping {author}/{permlink}: {e}")
            if 'video_path' in locals() and video_path:
                self.ipfs_fetcher.cleanup_video(video_path)
            # Set failure count to max so this video is never retried
            for _ in range(self.max_retries):
                self.db.record_failure(author, permlink)
            return False

        except Exception as e:
            logger.error(f"Error processing video: {e}", exc_info=True)
            # Cleanup on error
            if 'video_path' in locals() and video_path:
                self.ipfs_fetcher.cleanup_video(video_path)
            return False

    def _poll_once(self) -> int:
        """Run one polling cycle. Returns the number of videos processed."""
        # Refresh premium users every poll cycle
        self._premium_users = self.db.get_premium_users()
        if self._premium_users:
            logger.info(f"Premium users loaded: {len(self._premium_users)}")

        # Check for priority videos first
        processed = 0
        priority = self.db.get_priority_video()
        while priority:
            p_owner = priority.get('owner', 'unknown')
            p_permlink = priority.get('permlink', 'unknown')
            if self.db.is_blacklisted(p_owner, p_permlink):
                logger.info(f"⛔ Skipping blacklisted priority video: {p_owner}/{p_permlink}")
            elif self.db.get_failure_count(p_owner, p_permlink) >= self.max_retries:
                logger.info(f"⏭ Skipping priority video {p_owner}/{p_permlink} (failed {self.max_retries} times)")
            else:
                logger.info(f"\n⚡ Processing PRIORITY video: {p_owner}/{p_permlink}")
                self.db.set_processing(
                    p_owner, p_permlink,
                    video_type=priority.get('_video_type', 'legacy'),
                )
                if self.process_video(priority):
                    processed += 1
                else:
                    count = self.db.record_failure(p_owner, p_permlink)
                    logger.warning(f"Failure #{count}/{self.max_retries} for {p_owner}/{p_permlink}")
                self.db.clear_processing()
            time.sleep(2)
            priority = self.db.get_priority_video()

        # Get videos to process
        if self.process_only:
            owner, permlink = self.process_only.split('/', 1)
            logger.info(f"PROCESS_ONLY mode: fetching {owner}/{permlink}")
            video = self.db.get_video_by_owner_permlink(owner, permlink)
            videos = [video] if video else []
        else:
            if self.start_date:
                logger.info(f"Fetching videos since {self.start_date.date()}...")
                videos = self.db.get_videos_since(self.start_date, audio_start_date=self.audio_start_date)
            else:
                logger.info("Fetching all videos with CIDs...")
                videos = self.db.get_all_videos_with_cids()

        if not videos:
            return processed

        # Sort: priority creators (0) → premium users (1) → regular (2), then oldest first
        priority_creators = self.db.get_priority_creators()
        videos.sort(key=lambda v: (
            0 if v.get('owner') in priority_creators else
            1 if v.get('owner') in self._premium_users else 2,
            v.get('created') or v.get('createdAt') or datetime.min,
        ))

        # Filter out: fully processed, max-failed, blacklisted (single source of truth)
        all_lang_codes = [lang['code'] for lang in self.language_configs]
        fully_done = self.db.get_fully_processed_keys(all_lang_codes)
        max_failed = self.db.get_max_failed_keys(self.max_retries)
        bl_keys = self.db.get_blacklisted_keys()
        bl_authors = self.db.get_blacklisted_authors()
        exclude = fully_done | max_failed | bl_keys
        before = len(videos)
        videos = [
            v for v in videos
            if (v.get('owner', ''), v.get('permlink', '')) not in exclude
            and v.get('owner', '') not in bl_authors
        ]
        logger.info(f"Found {before} videos, {len(fully_done)} complete, {len(max_failed)} max-failed, {len(bl_keys)} blacklisted, {len(bl_authors)} blacklisted authors, {len(videos)} to process")

        if not videos:
            return processed

        # Process each video
        success_count = 0
        failed_count = 0

        for i, video in enumerate(videos, 1):
            # Check for priority videos between regular videos
            priority = self.db.get_priority_video()
            while priority:
                p_owner = priority.get('owner', 'unknown')
                p_permlink = priority.get('permlink', 'unknown')
                if self.db.is_blacklisted(p_owner, p_permlink):
                    logger.info(f"⛔ Skipping blacklisted priority video: {p_owner}/{p_permlink}")
                elif self.db.get_failure_count(p_owner, p_permlink) >= self.max_retries:
                    logger.info(f"⏭ Skipping priority video {p_owner}/{p_permlink} (failed {self.max_retries} times)")
                else:
                    logger.info(f"\n⚡ Processing PRIORITY video: {p_owner}/{p_permlink}")
                    self.db.set_processing(
                        p_owner, p_permlink,
                        video_type=priority.get('_video_type', 'legacy'),
                    )
                    if self.process_video(priority):
                        processed += 1
                    else:
                        count = self.db.record_failure(p_owner, p_permlink)
                        logger.warning(f"Failure #{count}/{self.max_retries} for {p_owner}/{p_permlink}")
                    self.db.clear_processing()
                time.sleep(2)
                priority = self.db.get_priority_video()

            logger.info(f"\nProcessing video {i}/{len(videos)}")

            v_owner = video.get('owner', 'unknown')
            v_permlink = video.get('permlink', 'unknown')
            if self.db.is_blacklisted(v_owner, v_permlink):
                logger.info(f"⛔ Skipping blacklisted video: {v_owner}/{v_permlink}")
                continue

            is_premium = v_owner in self._premium_users
            self.db.set_processing(
                v_owner, v_permlink,
                video_type=video.get('_video_type', 'legacy'),
            )
            if self.process_video(video, is_premium=is_premium):
                success_count += 1
                processed += 1
            else:
                failed_count += 1
                count = self.db.record_failure(v_owner, v_permlink)
                logger.warning(f"Failure #{count}/{self.max_retries} for {v_owner}/{v_permlink}")
            self.db.clear_processing()

            # Small delay between videos
            time.sleep(2)

        # Summary
        logger.info(f"\n{'=' * 80}")
        logger.info("Processing Summary:")
        logger.info(f"  Total videos: {len(videos)}")
        logger.info(f"  Successful: {success_count}")
        logger.info(f"  Failed: {failed_count}")
        logger.info(f"{'=' * 80}\n")

        return processed

    def run(self):
        """Main service loop — polls for new work, sleeps when idle."""
        poll_interval = self.config.get('processing', {}).get('poll_interval', 60)
        logger.info(f"Starting service (poll interval: {poll_interval}s)...")

        try:
            while True:
                count = self._poll_once()
                if count == 0:
                    logger.info(f"No work found, sleeping {poll_interval}s...")
                    time.sleep(poll_interval)
                # If we did work, immediately poll again (new videos may have appeared)
        except KeyboardInterrupt:
            logger.info("\nService interrupted by user")
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")
        self.db.close()
        logger.info("Service shutdown complete")


def main():
    """Entry point"""
    try:
        service = SubtitleService()
        service.run()
    except Exception as e:
        logger.error(f"Failed to start service: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
