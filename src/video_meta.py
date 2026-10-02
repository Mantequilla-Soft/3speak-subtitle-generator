"""
Video document normalization.

The three source collections use different field names for the same things:

    videos       (legacy) : title, description, tags (comma string), community
    embed-video          : hive_title, hive_body, hive_tags (array), category
    embed-audio          : title, description, tags

Consumers should never reach for a raw field name. `normalize_video_metadata`
collapses all three shapes into one dict so a missing field is a real absence
rather than a name mismatch.
"""

import logging
from typing import Any, Dict

from hive_client import filename_to_title, is_generic_title

logger = logging.getLogger(__name__)


def _first_str(*values: Any) -> str:
    """First value that is a non-empty string after stripping."""
    for v in values:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ''


def normalize_video_metadata(video: Dict[str, Any]) -> Dict[str, Any]:
    """
    Collapse a video doc from any collection into a uniform metadata dict.

    Returns keys: title, body, hive_tags, category. Values are always the right
    type ('' / [] / '') so callers need no defensive checks.
    """
    if not video:
        return {'title': '', 'body': '', 'hive_tags': [], 'category': ''}

    embed_title = video.get('embed_title')
    if embed_title and is_generic_title(embed_title):
        embed_title = ''

    title = _first_str(
        video.get('hive_title'),
        video.get('title'),
        embed_title,
        filename_to_title(video.get('originalFilename') or ''),
    )

    body = _first_str(video.get('hive_body'), video.get('description'))

    raw_tags = video.get('hive_tags') or video.get('tags') or []
    if isinstance(raw_tags, str):
        raw_tags = [t for t in raw_tags.split(',') if t.strip()]
    elif not isinstance(raw_tags, list):
        raw_tags = []

    category = _first_str(video.get('category'), video.get('community'))

    return {
        'title': title,
        'body': body,
        'hive_tags': raw_tags,
        'category': category,
    }


def merge_hive_post(meta: Dict[str, Any], post: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fill gaps in `meta` from a fetched Hive post. Existing values always win —
    the local DB is authoritative; the Hive post only supplies what is missing.
    """
    if not post:
        return meta

    merged = dict(meta)
    if not merged.get('title'):
        merged['title'] = _first_str(post.get('title'))
    if not merged.get('body'):
        merged['body'] = _first_str(post.get('body'))
    if not merged.get('hive_tags'):
        tags = post.get('tags') or []
        merged['hive_tags'] = list(tags) if isinstance(tags, list) else []
    if not merged.get('category'):
        merged['category'] = _first_str(post.get('category'))
    return merged


def hive_reference(video: Dict[str, Any]) -> tuple:
    """
    The (author, permlink) identifying this video's Hive post, or (None, None).

    Embed docs carry it in hive_author/hive_permlink, sometimes only in the
    embed_url. Legacy docs are posted under their own owner/permlink.
    """
    if not video:
        return None, None

    author = _first_str(video.get('hive_author'), video.get('owner'))
    permlink = _first_str(video.get('hive_permlink'))
    if author and permlink:
        return author, permlink

    from hive_client import parse_embed_url
    eu_author, eu_permlink = parse_embed_url(video.get('embed_url') or '')
    if eu_author and eu_permlink:
        return eu_author, eu_permlink

    # Legacy videos are the Hive post itself.
    if video.get('_video_type', 'legacy') == 'legacy':
        permlink = _first_str(video.get('permlink'))
        if author and permlink:
            return author, permlink

    return None, None
