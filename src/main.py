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
from transcriber import Transcriber
from translator import Translator
from tagger import ContentTagger
from subtitle_generator import SubtitleGenerator

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

        # Get language list with duration thresholds
        self.language_configs = self.config['languages']
        all_codes = [lang['code'] for lang in self.language_configs]
        logger.info(f"Target languages: {', '.join(all_codes)}")

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

        logger.info(f"ENABLE_LOCAL_SAVE={self.enable_local_save}  "
                    f"ENABLE_IPFS_PIN={self.enable_ipfs_pin}  "
                    f"ENABLE_REMOTE_PIN={self.enable_remote_pin}  "
                    f"ENABLE_MONGO_WRITE={self.enable_mongo_write}")

        logger.info("Service initialized successfully")

    def process_video(self, video: Dict[str, Any]) -> bool:
        """
        Process a single video: transcribe, translate, tag, and save

        Args:
            video: Video document from MongoDB

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

            # Quick check: skip entirely if all languages are already done
            all_lang_codes = [lang['code'] for lang in self.language_configs]
            existing = self.db.get_existing_subtitle_languages(author, permlink)
            if existing and all(code in existing for code in all_lang_codes):
                logger.info(f"All languages already processed for {author}/{permlink}, skipping")
                return True

            logger.info(f"\n{'=' * 80}")
            logger.info(f"Processing: {author}/{permlink} [{video_type}]")
            logger.info(f"CID: {hls_cid or cid}")
            logger.info(f"{'=' * 80}\n")

            # Step 1: Download video/audio
            logger.info("Step 1/5: Downloading from IPFS...")
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
            logger.info("Step 2/5: Transcribing audio...")
            configured_codes = {lang['code'] for lang in self.language_configs}
            hotwords = self.db.get_hotwords()
            start_time = time.time()
            segments, detected_language = self.transcriber.transcribe(
                video_path, allowed_languages=configured_codes, hotwords=hotwords
            )
            transcription_time = time.time() - start_time
            logger.info(f"Transcription completed in {transcription_time:.1f}s")

            if not segments:
                logger.error("No segments generated from transcription")
                self.ipfs_fetcher.cleanup_video(video_path)
                return False

            # Apply text corrections (e.g. "p.d." -> "PeakD")
            corrections = self.db.get_corrections()
            if corrections:
                segments = self.transcriber.apply_corrections(segments, corrections)
                logger.info(f"Applied {len(corrections)} text corrections")

            # Get full transcript for tagging
            full_transcript = self.transcriber.get_transcript_text(segments)

            # Step 3: Generate tags (transcript + video description)
            logger.info("Step 3/5: Analyzing content and generating tags...")
            post_content = video.get('description') or None
            tags = self.tagger.generate_tags(full_transcript, segments, post_content=post_content)
            if self.enable_mongo_write:
                self.db.save_tags(author, permlink, tags)
            logger.info(f"Tags: {', '.join(tags)}")

            # Step 4: Generate subtitles for eligible languages
            # Calculate video duration from last segment
            video_duration_min = segments[-1].end / 60.0 if segments else 0
            logger.info(f"Video duration: {video_duration_min:.1f} minutes")

            # Filter languages by max_duration threshold (0 = no limit, always generate)
            eligible_languages = [
                lang['code'] for lang in self.language_configs
                if lang.get('max_duration', 0) == 0
                or video_duration_min <= lang.get('max_duration', 0)
            ]
            skipped = len(self.language_configs) - len(eligible_languages)
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

            logger.info(f"Step 4/5: Generating subtitles for {len(eligible_languages)} languages: {', '.join(eligible_languages)}")

            # Convert segments to dict format
            segment_dicts = self.transcriber.get_segments_for_language(segments)

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
                        if self.enable_ipfs_pin:
                            subtitle_cid = self.ipfs_fetcher.add_to_ipfs(subtitle_path)
                            if subtitle_cid and self.enable_remote_pin:
                                self.ipfs_fetcher.pin_remote(subtitle_cid, self.remote_pin_url)

                        # Save to MongoDB
                        if self.enable_mongo_write and subtitle_cid:
                            self.db.save_subtitle(
                                author, permlink, cid, lang_code, subtitle_cid,
                                video_type=video_type,
                                video_created_at=video.get('created') or video.get('createdAt'),
                            )

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

            # Step 5: Cleanup
            logger.info("Step 5/5: Cleaning up temporary files...")
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

        except Exception as e:
            logger.error(f"Error processing video: {e}", exc_info=True)
            # Cleanup on error
            if 'video_path' in locals() and video_path:
                self.ipfs_fetcher.cleanup_video(video_path)
            return False

    def run(self):
        """Main service loop"""
        logger.info("Starting processing loop...")

        try:
            # Get videos to process
            if self.process_only:
                owner, permlink = self.process_only.split('/', 1)
                logger.info(f"PROCESS_ONLY mode: fetching {owner}/{permlink}")
                video = self.db.get_video_by_owner_permlink(owner, permlink)
                videos = [video] if video else []
            else:
                # Use the later of START_DATE and last processed video date
                effective_start = self.start_date
                last_processed = self.db.get_last_processed_video_date()
                if last_processed:
                    if effective_start:
                        effective_start = max(effective_start, last_processed)
                    else:
                        effective_start = last_processed

                if effective_start:
                    logger.info(f"Fetching videos since {effective_start}...")
                    videos = self.db.get_videos_since(
                        effective_start, embed_start_date=self.start_date
                    )
                else:
                    logger.info("Fetching all videos with CIDs...")
                    videos = self.db.get_all_videos_with_cids()

            if not videos:
                logger.info("No videos to process")
                return

            # Sort priority: creators > embed > published legacy > scheduled > audio
            priority_creators = self.db.get_priority_creators()
            type_order = {'embed': 0, 'legacy': 1, 'audio': 2}
            videos.sort(key=lambda v: (
                0 if v.get('owner') in priority_creators else 1,
                type_order.get(v.get('_video_type'), 2),
                0 if v.get('status') == 'published' else 1,
            ))

            logger.info(f"Found {len(videos)} videos to process")

            # Process each video
            success_count = 0
            failed_count = 0

            for i, video in enumerate(videos, 1):
                # Check for priority videos before each iteration
                priority = self.db.get_priority_video()
                while priority:
                    p_owner = priority.get('owner', 'unknown')
                    p_permlink = priority.get('permlink', 'unknown')
                    if self.db.is_blacklisted(p_owner, p_permlink):
                        logger.info(f"⛔ Skipping blacklisted priority video: {p_owner}/{p_permlink}")
                    else:
                        logger.info(f"\n⚡ Processing PRIORITY video: {p_owner}/{p_permlink}")
                        self.db.set_processing(
                            p_owner, p_permlink,
                            video_type=priority.get('_video_type', 'legacy'),
                        )
                        self.process_video(priority)
                        self.db.clear_processing()
                    time.sleep(2)
                    priority = self.db.get_priority_video()

                logger.info(f"\nProcessing video {i}/{len(videos)}")

                v_owner = video.get('owner', 'unknown')
                v_permlink = video.get('permlink', 'unknown')
                if self.db.is_blacklisted(v_owner, v_permlink):
                    logger.info(f"⛔ Skipping blacklisted video: {v_owner}/{v_permlink}")
                    continue

                self.db.set_processing(
                    v_owner, v_permlink,
                    video_type=video.get('_video_type', 'legacy'),
                )
                if self.process_video(video):
                    success_count += 1
                else:
                    failed_count += 1
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
