"""
MongoDB Database Manager
Handles all database operations for videos, subtitles, and tags
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pymongo import MongoClient, errors

logger = logging.getLogger(__name__)


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
            self.corrections_collection = self.db[self.config.get('collection_corrections', 'subtitles-corrections')]

            logger.info(f"Connected to MongoDB: {self.config['database']}")
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_videos_since(self, start_date: datetime,
                         embed_start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Fetch videos created on or after start_date from all collections.

        Args:
            start_date: Only return legacy videos created on or after this datetime
                        (advances with the processing cursor)
            embed_start_date: Only return embed/audio videos created on or after this
                              datetime (defaults to start_date if not provided)

        Returns:
            List of video documents sorted by creation date ascending
        """
        embed_since = embed_start_date or start_date
        try:
            # Legacy collection: uses 'created' field and 'filename' with ipfs:// prefix
            has_cid = {'filename': {'$exists': True, '$nin': [None, ''], '$regex': '^ipfs://'}}
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
                'createdAt': {'$gte': embed_since},
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
            logger.info(f"Found {len(legacy)} legacy (since {start_date.date()}) + {len(embed)} embed + {len(audio)} audio (since {embed_since.date()})")
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
            has_cid = {'filename': {'$exists': True, '$nin': [None, ''], '$regex': '^ipfs://'}}
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
            return True

        except Exception as e:
            logger.error(f"Error saving subtitle: {e}")
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

    def save_tags(self, author: str, permlink: str, tags: List[str]) -> bool:
        """
        Save video tags to database

        Args:
            author: Video author
            permlink: Video permlink
            tags: List of tags

        Returns:
            True if successful
        """
        try:
            # Convert tags list to comma-separated string
            tags_string = ','.join(tags)

            document = {
                'author': author,
                'permlink': permlink,
                'tags': tags_string,
                'created_at': datetime.now()
            }

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

    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
