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
            self.subtitles_collection = self.db[self.config['collection_subtitles']]
            self.tags_collection = self.db[self.config['collection_tags']]

            logger.info(f"Connected to MongoDB: {self.config['database']}")
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def get_videos_since(self, start_date: datetime) -> List[Dict[str, Any]]:
        """
        Fetch videos created on or after start_date from both collections.

        Args:
            start_date: Only return videos created on or after this datetime

        Returns:
            List of video documents sorted by creation date ascending
        """
        try:
            # Legacy collection: uses 'created' field and 'filename' with ipfs:// prefix
            legacy_query = {
                'created': {'$gte': start_date},
                'filename': {'$exists': True, '$nin': [None, ''], '$regex': '^ipfs://'},
                'status': 'published'
            }
            legacy = list(self.videos_collection.find(legacy_query).sort('created', 1))
            for v in legacy:
                v['_video_type'] = 'legacy'

            # Embed collection: uses 'createdAt' field and 'manifest_cid'
            embed_query = {
                'createdAt': {'$gte': start_date},
                'manifest_cid': {'$exists': True, '$nin': [None, '']},
                'status': 'published'
            }
            embed = list(self.embed_collection.find(embed_query).sort('createdAt', 1))
            for v in embed:
                v['_video_type'] = 'embed'

            videos = sorted(
                legacy + embed,
                key=lambda v: v.get('created') or v.get('createdAt') or datetime.min
            )
            logger.info(f"Found {len(legacy)} legacy + {len(embed)} embed videos since {start_date.date()}")
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
            legacy_query = {
                'filename': {'$exists': True, '$nin': [None, ''], '$regex': '^ipfs://'},
                'status': 'published'
            }
            legacy = list(self.videos_collection.find(legacy_query).sort('created', 1))
            for v in legacy:
                v['_video_type'] = 'legacy'

            embed_query = {
                'manifest_cid': {'$exists': True, '$nin': [None, '']},
                'status': 'published'
            }
            embed = list(self.embed_collection.find(embed_query).sort('createdAt', 1))
            for v in embed:
                v['_video_type'] = 'embed'

            videos = sorted(
                legacy + embed,
                key=lambda v: v.get('created') or v.get('createdAt') or datetime.min
            )
            logger.info(f"Found {len(legacy)} legacy + {len(embed)} embed videos with CIDs")
            return videos

        except Exception as e:
            logger.error(f"Error fetching videos: {e}")
            return []

    def get_video_by_owner_permlink(self, owner: str, permlink: str) -> Optional[Dict[str, Any]]:
        """Fetch a single video by owner and permlink, checking both collections."""
        try:
            video = self.videos_collection.find_one({'owner': owner, 'permlink': permlink})
            if video:
                video['_video_type'] = 'legacy'
                return video
            video = self.embed_collection.find_one({'owner': owner, 'permlink': permlink})
            if video:
                video['_video_type'] = 'embed'
            return video
        except Exception as e:
            logger.error(f"Error fetching video {owner}/{permlink}: {e}")
            return None

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
                     is_embed: bool = False,
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
            is_embed: Whether the video comes from the embed collection
            video_created_at: Original creation date of the video

        Returns:
            True if successful
        """
        try:
            set_on_insert = {
                'isEmbed': is_embed,
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

    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")
