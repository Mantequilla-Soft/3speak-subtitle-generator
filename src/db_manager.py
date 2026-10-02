"""
MongoDB Database Manager
Handles all database operations for videos, subtitles, and tags
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pymongo import MongoClient, errors

logger = logging.getLogger(__name__)

# A legacy video is transcribable if it exposes either a direct file CID
# ('filename', how uploads looked before ~2026-06) or an HLS manifest
# ('video_v2', the only source set on native uploads since). Matching just one
# of the two drops every video published the other way.
_IPFS_URI = {'$exists': True, '$nin': [None, ''], '$regex': '^ipfs://'}
HAS_VIDEO_SOURCE = {'$or': [{'filename': _IPFS_URI}, {'video_v2': _IPFS_URI}]}


class DatabaseManager:
    """Manages MongoDB connections and operations"""

    def __init__(self, config: Dict[str, Any]):
        """Initialize database connection"""
        self.config = config['mongodb']
        self.client = None
        self.db = None
        self.videos_collection = None
        self.embed_collection = None
        self.subtitles_collection = None
        self.tags_collection = None

        self._connect()

    def _connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(
                self.config['uri'],
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000
            )
            # Test connection
            self.client.admin.command('ping')

            self.db = self.client[self.config['database']]
            self.videos_collection = self.db[self.config['collection_videos']]
            self.embed_collection = self.db[self.config['collection_embed']]
            self.embed_audio_collection = self.db[self.config.get('collection_embed_audio', 'embed-audio')]
            self.subtitles_collection = self.db[self.config['collection_subtitles']]
            self.tags_collection = self.db[self.config['collection_tags']]
            self.priority_collection = self.db[self.config.get('collection_priority', 'subtitles-priority')]
            self.status_collection = self.db[self.config.get('collection_status', 'subtitles-status')]
            self.blacklist_collection = self.db[self.config.get('collection_blacklist', 'subtitles-blacklist')]
            self.blacklist_authors_collection = self.db[self.config.get('collection_blacklist_authors', 'subtitles-blacklist-authors')]
            self.priority_creators_collection = self.db[self.config.get('collection_priority_creators', 'subtitles-priority-creators')]
            self.hotwords_collection = self.db[self.config.get('collection_hotwords', 'subtitles-hotwords')]
            # Cache of Hive post metadata. Owned by this project — we never write
            # to 3speak's own embed-video / videos collections.
            self.hive_meta_collection = self.db[self.config.get('collection_hive_meta', 'subtitles-hive-meta')]
            self.corrections_collection = self.db[self.config.get('collection_corrections', 'subtitles-corrections')]
            self.failures_collection = self.db[self.config.get('collection_failures', 'subtitles-failures')]

            logger.info(f"Connected to MongoDB: {self.config['database']}")
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_videos_since(self, start_date: datetime,
                         embed_start_date: Optional[datetime] = None,
                         audio_start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Fetch videos created on or after start_date from all collections.

        Args:
            start_date: Only return legacy videos created on or after this datetime
                        (advances with the processing cursor)
            embed_start_date: Only return embed videos created on or after this
                              datetime (defaults to start_date if not provided)
            audio_start_date: Only return audio videos created on or after this
                              datetime (defaults to embed_since if not provided)

        Returns:
            List of video documents sorted by creation date ascending
        """
        embed_since = embed_start_date or start_date
        audio_since = audio_start_date or embed_since
        try:
            # Legacy collection: uses 'created' field and 'filename' with ipfs:// prefix.
            # Since ~2026-06 the uploader stopped setting 'filename' on native
            # uploads — they now carry only the HLS manifest in 'video_v2' — so
            # requiring 'filename' silently skipped the whole native catalogue.
            # process_video() already prefers video_v2/HLS, so accept either.
            has_cid = HAS_VIDEO_SOURCE
            legacy_query = {
                'created': {'$gte': start_date},
                **has_cid,
                'status': 'published',
            }
            # Also pick up future-scheduled videos so subtitles are ready at publish time
            scheduled_query = {
                **has_cid,
                'status': 'scheduled',
                'publish_data': {'$gt': datetime.now()},
            }
            legacy = list(self.videos_collection.find(
                {'$or': [legacy_query, scheduled_query]}
            ).sort('created', 1))
            for v in legacy:
                v['_video_type'] = 'legacy'

            # Embed and audio use START_DATE (not the cursor) so they aren't
            # skipped when the legacy cursor advances past them.
            embed_query = {
                'createdAt': {'$gte': embed_since},
                'manifest_cid': {'$exists': True, '$nin': [None, '']},
                'status': 'published',
            }
            embed = list(self.embed_collection.find(embed_query).sort('createdAt', 1))
            for v in embed:
                v['_video_type'] = 'embed'

            audio_query = {
                'createdAt': {'$gte': audio_since},
                'audio_cid': {'$exists': True, '$nin': [None, '']},
                'status': 'published',
            }
            audio = list(self.embed_audio_collection.find(audio_query).sort('createdAt', 1))
            for v in audio:
                v['_video_type'] = 'audio'

            videos = sorted(
                legacy + embed + audio,
                key=lambda v: v.get('created') or v.get('createdAt') or datetime.min
            )
            logger.info(f"Found {len(legacy)} legacy (since {start_date.date()}) + {len(embed)} embed (since {embed_since.date()}) + {len(audio)} audio (since {audio_since.date()})")
            return videos

        except Exception as e:
            logger.error(f"Error fetching videos: {e}")
            return []

    def get_all_videos_with_cids(self) -> List[Dict[str, Any]]:
        """
        Fetch all videos from both legacy and embed collections.

        Returns:
            List of video documents sorted by creation date ascending
        """
        try:
            has_cid = HAS_VIDEO_SOURCE
            legacy_query = {**has_cid, 'status': 'published'}
            scheduled_query = {
                **has_cid,
                'status': 'scheduled',
                'publish_data': {'$gt': datetime.now()},
            }
            legacy = list(self.videos_collection.find(
                {'$or': [legacy_query, scheduled_query]}
            ).sort('created', 1))
            for v in legacy:
                v['_video_type'] = 'legacy'

            embed_query = {
                'manifest_cid': {'$exists': True, '$nin': [None, '']},
                'status': 'published'
            }
            embed = list(self.embed_collection.find(embed_query).sort('createdAt', 1))
            for v in embed:
                v['_video_type'] = 'embed'

            audio_query = {
                'audio_cid': {'$exists': True, '$nin': [None, '']},
                'status': 'published'
            }
            audio = list(self.embed_audio_collection.find(audio_query).sort('createdAt', 1))
            for v in audio:
                v['_video_type'] = 'audio'

            videos = sorted(
                legacy + embed + audio,
                key=lambda v: v.get('created') or v.get('createdAt') or datetime.min
            )
            logger.info(f"Found {len(legacy)} legacy + {len(embed)} embed + {len(audio)} audio with CIDs")
            return videos

        except Exception as e:
            logger.error(f"Error fetching videos: {e}")
            return []

    def get_video_by_owner_permlink(self, owner: str, permlink: str) -> Optional[Dict[str, Any]]:
        """Fetch a single video by owner and permlink, checking all collections."""
        try:
            video = self.videos_collection.find_one({'owner': owner, 'permlink': permlink})
            if video:
                video['_video_type'] = 'legacy'
                return video
            video = self.embed_collection.find_one({'owner': owner, 'permlink': permlink})
            if video:
                video['_video_type'] = 'embed'
                return video
            video = self.embed_audio_collection.find_one({'owner': owner, 'permlink': permlink})
            if video:
                video['_video_type'] = 'audio'
            return video
        except Exception as e:
            logger.error(f"Error fetching video {owner}/{permlink}: {e}")
            return None

    def get_priority_video(self) -> Optional[Dict[str, Any]]:
        """Pop the oldest priority request and return the full video document."""
        try:
            entry = self.priority_collection.find_one_and_delete(
                {}, sort=[('requested_at', 1)]
            )
            if not entry:
                return None
            author = entry['author']
            permlink = entry['permlink']
            logger.info(f"Priority video requested: {author}/{permlink}")
            video = self.get_video_by_owner_permlink(author, permlink)
            if not video:
                logger.warning(f"Priority video {author}/{permlink} not found in DB")
            return video
        except Exception as e:
            logger.error(f"Error checking priority queue: {e}")
            return None

    def set_processing(self, author: str, permlink: str, video_type: str = 'legacy'):
        """Mark a video as currently being processed (single-document collection)."""
        try:
            self.status_collection.replace_one(
                {},
                {
                    'author': author,
                    'permlink': permlink,
                    'isEmbed': video_type in ('embed', 'audio'),
                    'isAudio': video_type == 'audio',
                    'started_at': datetime.now(),
                },
                upsert=True,
            )
        except Exception as e:
            logger.error(f"Error setting processing status: {e}")

    def clear_processing(self):
        """Clear the currently-processing status."""
        try:
            self.status_collection.delete_many({})
        except Exception as e:
            logger.error(f"Error clearing processing status: {e}")

    def is_blacklisted(self, author: str, permlink: str) -> bool:
        """Check if a video or its author is blacklisted."""
        try:
            if self.blacklist_authors_collection.find_one({'author': author}, {'_id': 1}):
                return True
            return bool(self.blacklist_collection.find_one(
                {'author': author, 'permlink': permlink}, {'_id': 1}
            ))
        except Exception as e:
            logger.error(f"Error checking blacklist: {e}")
            return False

    def get_priority_creators(self) -> set:
        """Return the set of authors whose videos should be prioritized."""
        try:
            return {
                doc['author']
                for doc in self.priority_creators_collection.find({}, {'author': 1, '_id': 0})
            }
        except Exception as e:
            logger.error(f"Error fetching priority creators: {e}")
            return set()

    def get_last_processed_video_date(self) -> Optional[datetime]:
        """
        Get the video creation date of the most recently processed video.

        Returns:
            The latest video_created_at from the subtitles collection, or None.
        """
        try:
            doc = self.subtitles_collection.find_one(
                {'video_created_at': {'$exists': True}},
                {'video_created_at': 1, '_id': 0},
                sort=[('video_created_at', -1)],
            )
            if doc:
                ts = doc['video_created_at']
                logger.info(f"Last processed video date: {ts}")
                return ts
            return None
        except Exception as e:
            logger.error(f"Error fetching last processed date: {e}")
            return None

    def get_fully_processed_keys(self, language_codes: List[str]) -> set:
        """Return set of (author, permlink) tuples that have all languages done."""
        try:
            query = {f'subtitles.{code}': {'$exists': True} for code in language_codes}
            cursor = self.subtitles_collection.find(
                query, {'author': 1, 'permlink': 1, '_id': 0}
            )
            return {(doc['author'], doc['permlink']) for doc in cursor}
        except Exception as e:
            logger.error(f"Error fetching fully processed keys: {e}")
            return set()

    def get_existing_subtitle_languages(self, author: str, permlink: str) -> List[str]:
        """
        Get list of languages that already have subtitles for a video.

        Returns:
            List of language codes already processed
        """
        try:
            doc = self.subtitles_collection.find_one(
                {'author': author, 'permlink': permlink},
                {'subtitles': 1, '_id': 0}
            )
            if doc and 'subtitles' in doc:
                return list(doc['subtitles'].keys())
            return []
        except Exception as e:
            logger.error(f"Error checking existing subtitles: {e}")
            return []

    def save_subtitle(self, author: str, permlink: str, video_cid: str,
                     language: str, subtitle_cid: str,
                     video_type: str = 'legacy',
                     video_created_at: Optional[datetime] = None) -> bool:
        """
        Save subtitle CID for a language to the video's subtitle document.
        Uses one document per video, with subtitle CIDs keyed by language.

        Args:
            author: Video author
            permlink: Video permlink
            video_cid: Video CID
            language: Subtitle language code
            subtitle_cid: IPFS CID of the subtitle file
            video_type: Source type ('legacy', 'embed', or 'audio')
            video_created_at: Original creation date of the video

        Returns:
            True if successful
        """
        try:
            set_on_insert = {
                'isEmbed': video_type in ('embed', 'audio'),
                'isAudio': video_type == 'audio',
                'created_at': datetime.now(),
            }
            if video_created_at:
                set_on_insert['video_created_at'] = video_created_at

            self.subtitles_collection.update_one(
                {'author': author, 'permlink': permlink},
                {
                    '$set': {
                        'video_cid': video_cid,
                        f'subtitles.{language}': subtitle_cid,
                        'updated_at': datetime.now(),
                    },
                    '$setOnInsert': set_on_insert,
                },
                upsert=True,
            )
            logger.info(f"Saved subtitle CID for {author}/{permlink} [{language}]: {subtitle_cid}")

            # For embed videos cross-posted to Hive, the frontend URL uses
            # hive_permlink (e.g. 3speak-1780432582318), not the embed permlink
            # (e.g. hxo8kawo). Mirror under hive_permlink so the public API resolves.
            if video_type in ('embed', 'audio'):
                col = self.embed_collection if video_type == 'embed' else self.embed_audio_collection
                ev = col.find_one(
                    {'owner': author, 'permlink': permlink},
                    {'hive_permlink': 1, '_id': 0},
                )
                hive_pl = ev.get('hive_permlink') if ev else None
                if hive_pl and hive_pl != permlink:
                    self.subtitles_collection.update_one(
                        {'author': author, 'permlink': hive_pl},
                        {
                            '$set': {
                                'video_cid': video_cid,
                                f'subtitles.{language}': subtitle_cid,
                                'updated_at': datetime.now(),
                                'is_alias': True,
                                'embed_permlink': permlink,
                            },
                            '$setOnInsert': set_on_insert,
                        },
                        upsert=True,
                    )
            return True

        except Exception as e:
            logger.error(f"Error saving subtitle: {e}")
            return False

    def _hive_permlink_for(self, author: str, permlink: str, video_type: str) -> Optional[str]:
        """Resolve the hive_permlink for an embed/audio video, if cross-posted to Hive."""
        if video_type not in ('embed', 'audio'):
            return None
        col = self.embed_collection if video_type == 'embed' else self.embed_audio_collection
        ev = col.find_one(
            {'owner': author, 'permlink': permlink},
            {'hive_permlink': 1, '_id': 0},
        )
        hive_pl = ev.get('hive_permlink') if ev else None
        return hive_pl if hive_pl and hive_pl != permlink else None

    def _mirror_subtitle_doc(self, author: str, hive_permlink: str,
                             embed_permlink: str, update: Dict[str, Any],
                             is_audio: bool) -> None:
        """Write the same $set update to the hive_permlink mirror subtitle doc."""
        self.subtitles_collection.update_one(
            {'author': author, 'permlink': hive_permlink},
            {
                '$set': {**update, 'is_alias': True, 'embed_permlink': embed_permlink},
                '$setOnInsert': {
                    'isEmbed': True,
                    'isAudio': is_audio,
                    'created_at': datetime.now(),
                },
            },
            upsert=True,
        )

    def record_remote_pin(self, author: str, permlink: str, language: str,
                          subtitle_cid: str, ok: bool,
                          video_type: str = 'legacy') -> None:
        """
        Track whether a subtitle's remote pin succeeded.

        A failed pin means the CID is on our local node but not the public
        gateway, so the language is listed but won't load. Recording the failure
        lets repin_subtitles.py find and re-pin it later — instead of the miss
        being a warning line no one ever sees. Cleared on a later success.

        Written to the primary AND the hive_permlink mirror, like save_subtitle.
        """
        entry = {'lang': language, 'cid': subtitle_cid}
        update = ({'$pull': {'remote_pin_failed': {'cid': subtitle_cid}}} if ok
                  else {'$addToSet': {'remote_pin_failed': entry}})
        keys = [permlink]
        hive_pl = self._hive_permlink_for(author, permlink, video_type)
        if hive_pl:
            keys.append(hive_pl)
        try:
            for key in keys:
                self.subtitles_collection.update_one(
                    {'author': author, 'permlink': key}, update)
        except Exception as e:
            logger.error(f"Error recording remote-pin status for {author}/{permlink}: {e}")

    def save_meta_cid(self, author: str, permlink: str, meta_cid: str,
                       video_type: str = 'legacy') -> bool:
        """Save the IPFS CID of the per-video meta.json (summary + title translations)."""
        try:
            update = {'meta_cid': meta_cid, 'updated_at': datetime.now()}
            self.subtitles_collection.update_one(
                {'author': author, 'permlink': permlink},
                {'$set': update},
                upsert=True,
            )
            hive_pl = self._hive_permlink_for(author, permlink, video_type)
            if hive_pl:
                self._mirror_subtitle_doc(
                    author, hive_pl, permlink, update, video_type == 'audio'
                )
            return True
        except Exception as e:
            logger.error(f"Error saving meta_cid: {e}")
            return False

    def save_summary(self, author: str, permlink: str, summary_en: str,
                     video_type: str = 'legacy') -> bool:
        """Save the English summary of the video to the subtitle doc."""
        try:
            update = {'summary_en': summary_en, 'updated_at': datetime.now()}
            self.subtitles_collection.update_one(
                {'author': author, 'permlink': permlink},
                {'$set': update},
                upsert=True,
            )
            hive_pl = self._hive_permlink_for(author, permlink, video_type)
            if hive_pl:
                self._mirror_subtitle_doc(
                    author, hive_pl, permlink, update, video_type == 'audio'
                )
            return True
        except Exception as e:
            logger.error(f"Error saving summary: {e}")
            return False

    def save_title_translations(self, author: str, permlink: str,
                                translations: Dict[str, str],
                                video_type: str = 'legacy') -> bool:
        """Save title translated into multiple languages: {lang_code: translated_title}."""
        try:
            update = {'title_translations': translations, 'updated_at': datetime.now()}
            self.subtitles_collection.update_one(
                {'author': author, 'permlink': permlink},
                {'$set': update},
                upsert=True,
            )
            hive_pl = self._hive_permlink_for(author, permlink, video_type)
            if hive_pl:
                self._mirror_subtitle_doc(
                    author, hive_pl, permlink, update, video_type == 'audio'
                )
            return True
        except Exception as e:
            logger.error(f"Error saving title translations: {e}")
            return False

    def save_processing_time(self, author: str, permlink: str, seconds: int,
                            video_duration_seconds: int = 0) -> bool:
        """Save total processing time and video duration on the subtitle document."""
        try:
            update = {'processing_seconds': seconds}
            if video_duration_seconds:
                update['video_duration_seconds'] = video_duration_seconds
            self.subtitles_collection.update_one(
                {'author': author, 'permlink': permlink},
                {'$set': update},
            )
            return True
        except Exception as e:
            logger.error(f"Error saving processing time: {e}")
            return False

    def save_tags(self, author: str, permlink: str, tags: List[str],
                  scores: Optional[Dict[str, float]] = None,
                  evidence: Optional[List[str]] = None,
                  model: Optional[str] = None,
                  source_lang: Optional[str] = None) -> bool:
        """
        Save video tags to database

        Args:
            author: Video author
            permlink: Video permlink
            tags: List of tags
            scores: Per-label classifier scores, for debugging a bad tag later
            evidence: Tags derived from the author's Hive tags / community
            model: Classifier model name, so a re-tag can target stale rows
            source_lang: Detected transcript language

        Returns:
            True if successful
        """
        try:
            # Hand-corrected tags win over anything automated. The guard lives
            # here rather than in each caller so EVERY path is covered — the live
            # pipeline (a re-queued video), retag.py, and tag_metadata.py.
            # Set/cleared with src/set_tags.py.
            locked = self.tags_collection.find_one(
                {'author': author, 'permlink': permlink, 'manual': True},
                {'_id': 1},
            )
            if locked:
                logger.info(f"Skipping {author}/{permlink}: tags are manually locked")
                return True

            # `tags` stays a comma-separated string: the dashboard splits on ','.
            # `tags_list` is the structured form new consumers should read.
            tags_string = ','.join(tags)

            document = {
                'author': author,
                'permlink': permlink,
                'tags': tags_string,
                'tags_list': tags,
                'tag_evidence': evidence or [],
                'tag_scores': scores or {},
                'created_at': datetime.now()
            }
            # Only written when known. A re-tag has no transcript, so it must not
            # clobber the source_lang recorded by a full pipeline run.
            if model is not None:
                document['tag_model'] = model
            if source_lang is not None:
                document['source_lang'] = source_lang

            # Use update with upsert to avoid duplicates
            self.tags_collection.update_one(
                {'author': author, 'permlink': permlink},
                {'$set': document},
                upsert=True
            )

            logger.info(f"Saved tags for {author}/{permlink}: {tags_string}")
            return True

        except Exception as e:
            logger.error(f"Error saving tags: {e}")
            return False

    def get_hive_meta(self, author: str, permlink: str) -> Optional[Dict[str, Any]]:
        """Cached Hive post metadata (title/body/tags/category), or None."""
        try:
            return self.hive_meta_collection.find_one(
                {'author': author, 'permlink': permlink},
                {'_id': 0, 'title': 1, 'body': 1, 'tags': 1, 'category': 1},
            )
        except Exception as e:
            logger.error(f"Error reading hive meta for {author}/{permlink}: {e}")
            return None

    def save_hive_meta(self, author: str, permlink: str, post: Dict[str, Any]) -> bool:
        """Cache a fetched Hive post so we hit the Hive API at most once per video."""
        try:
            self.hive_meta_collection.update_one(
                {'author': author, 'permlink': permlink},
                {'$set': {
                    'author': author,
                    'permlink': permlink,
                    'title': post.get('title', ''),
                    'body': post.get('body', ''),
                    'tags': post.get('tags', []),
                    'category': post.get('category', ''),
                    'fetched_at': datetime.now(),
                }},
                upsert=True,
            )
            return True
        except Exception as e:
            logger.error(f"Error caching hive meta for {author}/{permlink}: {e}")
            return False

    def get_hotwords(self) -> List[str]:
        """Return all hotwords for transcription prompting."""
        try:
            return [
                doc['word']
                for doc in self.hotwords_collection.find({}, {'word': 1, '_id': 0})
            ]
        except Exception as e:
            logger.error(f"Error fetching hotwords: {e}")
            return []

    def get_corrections(self) -> List[Dict[str, str]]:
        """Return all text corrections (from -> to pairs)."""
        try:
            return [
                {'from': doc['from_text'], 'to': doc['to_text']}
                for doc in self.corrections_collection.find(
                    {}, {'from_text': 1, 'to_text': 1, '_id': 0}
                )
            ]
        except Exception as e:
            logger.error(f"Error fetching corrections: {e}")
            return []

    def get_max_failed_keys(self, max_retries: int) -> set:
        """Return set of (author, permlink) tuples that have failed >= max_retries times."""
        try:
            cursor = self.failures_collection.find(
                {'count': {'$gte': max_retries}},
                {'author': 1, 'permlink': 1, '_id': 0}
            )
            return {(doc['author'], doc['permlink']) for doc in cursor}
        except Exception as e:
            logger.error(f"Error fetching max-failed keys: {e}")
            return set()

    def get_blacklisted_keys(self) -> set:
        """Return set of (author, permlink) tuples for blacklisted videos."""
        try:
            return {
                (d['author'], d['permlink'])
                for d in self.blacklist_collection.find({}, {'author': 1, 'permlink': 1, '_id': 0})
            }
        except Exception as e:
            logger.error(f"Error fetching blacklisted keys: {e}")
            return set()

    def get_blacklisted_authors(self) -> set:
        """Return set of blacklisted author names."""
        try:
            return {
                d['author']
                for d in self.blacklist_authors_collection.find({}, {'author': 1, '_id': 0})
            }
        except Exception as e:
            logger.error(f"Error fetching blacklisted authors: {e}")
            return set()

    def get_premium_users(self) -> set:
        """Return set of usernames that have an active premium status."""
        try:
            col_name = self.config.get('collection_premium', 'premium-users')
            col = self.db[col_name]
            return {
                d['username']
                for d in col.find({'premium': True}, {'username': 1, '_id': 0})
                if d.get('username')
            }
        except Exception as e:
            logger.error(f"Error fetching premium users: {e}")
            return set()

    def record_failure(self, author: str, permlink: str) -> int:
        """Increment failure count for a video and return the new count."""
        try:
            self.failures_collection.update_one(
                {'author': author, 'permlink': permlink},
                {
                    '$inc': {'count': 1},
                    '$set': {'last_failed_at': datetime.now()},
                    '$setOnInsert': {'first_failed_at': datetime.now()},
                },
                upsert=True,
            )
            doc = self.failures_collection.find_one(
                {'author': author, 'permlink': permlink},
                {'count': 1, '_id': 0},
            )
            return doc.get('count', 0) if doc else 0
        except Exception as e:
            logger.error(f"Error recording failure: {e}")
            return 0

    def get_failure_count(self, author: str, permlink: str) -> int:
        """Get the number of times a video has failed processing."""
        try:
            doc = self.failures_collection.find_one(
                {'author': author, 'permlink': permlink},
                {'count': 1, '_id': 0},
            )
            return doc.get('count', 0) if doc else 0
        except Exception as e:
            logger.error(f"Error getting failure count: {e}")
            return 0

    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
