"""
Faceted taxonomy v2 — the flat-tag namespace with a 2-level category tree.

This is a NEW module, deliberately separate from `tag_taxonomy.py`:

  * v1 (`tag_taxonomy.py`) stays byte-for-byte untouched, so the currently
    running tagger and every existing tag row keep working. This file is not
    imported by any live code path until we choose to wire it in.
  * v2 reuses v1's hand-curated evidence (Hive-tag sources, community maps,
    stopwords, body cleaner) rather than duplicating it — it imports them and
    re-partitions only where the taxonomy changed (splits and new leaves).

Design decisions locked in TAXONOMY_V2.md:
  * Category and topic are ONE flat tag namespace. A leaf ('cryptocurrency') and
    its category slug ('crypto-finance') are both just tags. Prefer the leaf;
    fall back to the category when no leaf is confident.
  * The 2-level tree survives only as a leaf -> category roll-up map (frontend
    grouping + the fallback rule). It is not a second stored field.
  * Format (talking-head / gameplay / ...) is documented here as an INTERNAL
    signal because it steers downstream processing (e.g. gameplay => gaming),
    NOT as a user-facing browsable facet. See FORMAT_LABELS / FORMAT_TO_TOPIC.

Storage stays backwards compatible: v2 writes the same `tags` / `tags_list`
fields, just with richer values, plus a `tag_model='v2'` marker.
"""

import os
import re
import sys
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tag_taxonomy as v1  # noqa: E402  (v1 is only read, never mutated)
from tag_taxonomy import (  # noqa: E402  pure helpers, reused as-is
    normalize_hive_tag,
    clean_post_body,
    HIVE_TAG_STOPWORDS,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# The 2-level tree.  category slug -> ordered list of leaf topics.
# Order matches TAXONOMY_V2.md.  Single-leaf categories are allowed.
# ─────────────────────────────────────────────────────────────────────────────
CATEGORY_TREE: Dict[str, List[str]] = {
    'tech-science':   ['technology', 'programming', 'science', 'education'],
    'crypto-finance': ['cryptocurrency', 'finance', 'business'],
    'entertainment':  ['gaming', 'music', 'vlog', 'comedy', 'lifestyle',
                       'story-time', 'commercial', 'film-tv'],
    'arts-diy':       ['art', 'diy-crafts', 'photography'],
    'food-outdoor':   ['food', 'travel', 'nature', 'gardening', 'pets'],
    'sports-health':  ['sports', 'health', 'fitness'],
    'life-society':   ['news', 'politics', 'spirituality'],
}

# Natural-language phrasing of each leaf for the zero-shot classifier. The bart
# hypothesis template is "This video is about {}.", so these read as noun
# phrases. Shared by the tagger and the calibrator so both score identical text.
LEAF_PROMPTS: Dict[str, str] = {
    'technology':     'technology and gadgets',
    'programming':    'software programming and coding',
    'science':        'science and research',
    'education':      'education and learning',
    'cryptocurrency': 'cryptocurrency and blockchain',
    'finance':        'finance and investing',
    'business':       'business and entrepreneurship',
    'gaming':         'video gaming',
    'music':          'music',
    'comedy':         'comedy and humor',
    'vlog':           'a personal vlog',
    'lifestyle':      'lifestyle and daily life',
    'story-time':     'storytelling and personal stories',
    'commercial':     'a commercial or product advertisement',
    'film-tv':        'movies and TV shows',
    'art':            'art and drawing',
    'diy-crafts':     'DIY and crafts',
    'photography':    'photography',
    'food':           'food and cooking',
    'travel':         'travel',
    'nature':         'nature and wildlife',
    'gardening':      'gardening and plants',
    'pets':           'pets and domestic animals',
    'sports':         'sports',
    'health':         'health and wellness',
    'fitness':        'fitness and exercise',
    'news':           'news and current events',
    'politics':       'politics',
    'spirituality':   'religion and spirituality',
}

# Vision-specific phrasing where CLIP needs different wording than bart.
# LEAF_PROMPTS is calibrated for the text classifier — do NOT edit it for
# vision reasons; override here instead. Fixes from the 10-video regression:
# dessert/restaurant shots lost to 'lifestyle'/'business', fantasy digital art
# read as spiritual imagery.
VISION_PROMPT_OVERRIDES: Dict[str, str] = {
    'food':  'food, cooking, eating, dessert, or a restaurant meal',
    'art':   'art, drawing, digital painting, or fantasy illustration',
    'business': 'business, entrepreneurship, or office work',
    # abstract nouns attract aesthetic imagery (a fountain scored 'spirituality');
    # anchor vision labels to concrete visuals instead.
    'spirituality': 'religious worship, a church or mosque, prayer, or tarot cards',
    'travel': 'travel, tourism, landmarks, fountains, monuments, or city sights',
    'pets':   'cats, dogs, guinea pigs, or other pet animals',
    # birthday candles fooled the spirituality prompt; farm pigs fooled 'pets'
    'lifestyle': 'daily life, a birthday party, or a family celebration',
    'nature': 'nature, wildlife, landscapes, or farm animals',
    'story-time': 'an animated story or someone telling a story',
    'commercial': 'a commercial advertisement or product promo',
    'film-tv': 'a movie scene, film still, or TV show',
}


def vision_prompts() -> Dict[str, str]:
    """LEAF_PROMPTS with the CLIP-specific overrides applied."""
    return {**LEAF_PROMPTS, **VISION_PROMPT_OVERRIDES}


# Human-facing category names, for the frontend.
CATEGORY_NAMES: Dict[str, str] = {
    'tech-science':   'Tech & Science',
    'crypto-finance': 'Crypto & Finance',
    'entertainment':  'Entertainment',
    'arts-diy':       'Arts & DIY',
    'food-outdoor':   'Food & Outdoors',
    'sports-health':  'Sports & Health',
    'life-society':   'Life & Society',
}

# Creator-level rules. Some channels are single-purpose while carrying no usable
# topical metadata — babylady's Hive tags are 'lolz', 'pepe', 'fun', so the
# classifier guessed 'travel'. A creator rule is the most specific signal we
# have, so it is checked BEFORE the community rule and decides outright.
# Keyed by lowercased Hive username.
AUTHOR_RULES: Dict[str, Set[str]] = {
    'babylady': {'story-time'},
}


def exclusive_tags_for_author(author: Any) -> List[str]:
    """Definitive tags for a single-purpose creator, or [] — then run nothing else."""
    if not author or not isinstance(author, str):
        return []
    tags = AUTHOR_RULES.get(author.strip().lower())
    return sorted(tags) if tags else []


# Leaves that may ONLY come from explicit evidence — the author's own Hive tags
# or a community rule — and are never proposed by a model.
#
# 'vlog' is a FORMAT, not a subject: a vlog can be about food, travel, gaming.
# It earns a place in the vocabulary because authors and communities declare it
# constantly and users browse by it, but neither bart nor CLIP can judge it
# reliably (talking-head vs vlog was the weakest distinction in the CLIP trials).
# So we record it when someone tells us, and never guess it.
EVIDENCE_ONLY_LEAVES: Set[str] = {'vlog'}

# Derived: leaf -> category, and the two halves of the flat tag namespace.
LEAF_TO_CATEGORY: Dict[str, str] = {
    leaf: cat for cat, leaves in CATEGORY_TREE.items() for leaf in leaves
}
LEAVES: Set[str] = set(LEAF_TO_CATEGORY)
CATEGORIES: Set[str] = set(CATEGORY_TREE)

# The full valid tag vocabulary = every leaf plus every category slug. Both are
# legal values in `tags` / `tags_list`; the category ones are the fallback tier.
TAXONOMY_V2: Set[str] = LEAVES | CATEGORIES

# Sanity: a leaf must belong to exactly one category, names must be complete.
assert len(LEAF_TO_CATEGORY) == sum(len(v) for v in CATEGORY_TREE.values()), \
    "a leaf appears under more than one category"
assert set(CATEGORY_NAMES) == CATEGORIES, "CATEGORY_NAMES out of sync with tree"
assert set(LEAF_PROMPTS) == LEAVES, "LEAF_PROMPTS out of sync with the leaves"
for _a, _t in AUTHOR_RULES.items():
    if _t - TAXONOMY_V2:
        raise ValueError(f"author rule {_a!r} maps to unknown tag {_t - TAXONOMY_V2}")
assert set(VISION_PROMPT_OVERRIDES) <= LEAVES, "vision override for a non-leaf"


# ─────────────────────────────────────────────────────────────────────────────
# Hive-tag -> leaf evidence.  Built from v1's curated source lists, re-partitioned
# for the taxonomy changes:
#   * v1 formats ('vlog', 'tutorial') are no longer topics — their author-tag
#     evidence is redirected to the nearest v2 leaf ('lifestyle', 'diy-crafts').
#   * split leaves (programming<-technology, politics<-news, gardening<-nature,
#     fitness<-health, diy-crafts<-art) take specific terms from their v1 parent.
#   * brand-new leaves get fresh source lists.
# ─────────────────────────────────────────────────────────────────────────────

# Start from v1's sources. 'vlog' survives as an evidence-only leaf, so its
# author-tag sources carry over unchanged; only 'tutorial' (a pure format with
# no v2 home) is dropped.
_SOURCES: Dict[str, List[str]] = {
    leaf: list(terms) for leaf, terms in v1._CANONICAL_SOURCES.items()
    if leaf != 'tutorial'
}
_SOURCES.setdefault('vlog', []).extend(['vlogger', 'dailyvlogs', 'myvlog'])
_SOURCES.setdefault('diy-crafts', []).extend(
    t for t in v1._CANONICAL_SOURCES.get('tutorial', [])
    if t in ('diy', 'diyhub')  # only the craft-y ones; 'howto'/'guide' are format cues
)
# 'chessbrothers' is a curation/reward community name spray-tagged onto unrelated
# posts (seen on a TV-series review), so as a bare TAG it is noise, not sports.
# Genuine chess content still carries 'chess'; the chess COMMUNITY maps below.
_SOURCES['sports'] = [t for t in _SOURCES.get('sports', []) if t != 'chessbrothers']

# (child_leaf, parent_leaf, terms) — move terms from the broad v1 parent to the
# more specific v2 leaf so the specific one wins.
_RETARGET = [
    ('programming', 'technology', ['programming', 'coding', 'software']),
    ('politics',    'news',       ['politics', 'geopolitics']),
    ('gardening',   'nature',     ['garden', 'gardening']),
    ('fitness',     'health',     ['workout', 'gym']),
    ('diy-crafts',  'art',        ['craft', 'crafts', 'manualidad', 'manualidades',
                                   'artesania', 'yeso', 'diy', 'diyhub', 'handmade',
                                   'needlework', 'pottery', 'sculpture']),
]
for _child, _parent, _terms in _RETARGET:
    if _parent in _SOURCES:
        _SOURCES[_parent] = [t for t in _SOURCES[_parent] if t not in _terms]
    _SOURCES.setdefault(_child, []).extend(_terms)

# Fresh evidence for the brand-new leaves.
_NEW_SOURCES: Dict[str, List[str]] = {
    'programming': ['dev', 'developer', 'developers', 'programmer', 'coder',
                    'webdev', 'javascript', 'typescript', 'python', 'rust',
                    'html', 'css', 'reactjs', 'nodejs', 'sql', 'devops',
                    'github', 'opensource', 'devlife', 'softwaredevelopment',
                    '100daysofcode', 'codenewbie'],
    'business':    ['business', 'entrepreneur', 'entrepreneurship', 'startup',
                    'marketing', 'ecommerce', 'sales', 'smallbusiness'],
    'comedy':      ['comedy', 'funny', 'humor', 'humour', 'standup', 'satire',
                    'parody', 'joke', 'jokes'],
    'lifestyle':   ['daily', 'routine', 'family', 'parenting',
                    'motherhood', 'selfimprovement', 'productivity'],
    'photography': ['photography', 'photo', 'photographer', 'camera', 'lightroom',
                    'streetphotography', 'portrait'],
    'spirituality': ['religion', 'religious', 'christian', 'christianity', 'bible',
                     'jesus', 'god', 'faith', 'islam', 'muslim', 'buddhism',
                     'spirituality', 'spiritual', 'church', 'prayer', 'gospel',
                     # esoteric corner — recurring blind spot in the untagged pile
                     'tarot', 'oracle', 'astrology', 'horoscope', 'esoteric'],
    'gardening':   ['homestead', 'homesteading', 'permaculture', 'plants',
                    'vegetable', 'harvest', 'cannabis', 'weed', 'grow'],
    'pets':        ['pets', 'pet', 'cat', 'cats', 'dog', 'dogs', 'puppy', 'kitten',
                    'caturday', 'guineapig', 'hamster', 'rabbit', 'hivepets',
                    'petlovers', 'dogsofhive', 'catsofhive'],
    'story-time':  ['story', 'storytime', 'stories', 'storytelling', 'anecdote',
                    'cuento', 'relato'],
    'commercial':  ['commercial', 'advertisement', 'advert', 'promo', 'trailer'],
    'diy-crafts':  ['woodworking', 'upcycling', 'sewing', 'knitting', 'crochet',
                    'restoration', 'papercraft', 'origami'],
    # Political/current-events content is common on 3Speak but the classifier's
    # news/politics floors are 0.95 (attractor labels), so it almost never fires
    # from the transcript — leaving political videos untagged. Author-declared
    # topic words are a far more reliable signal, same lever as 'programming'.
    # Commentary/figures/process -> politics; events/conflict -> news.
    'politics':    ['trump', 'biden', 'kamala', 'obama', 'election', 'elections',
                    'president', 'presidential', 'government', 'senate', 'congress',
                    'democrat', 'democrats', 'republican', 'republicans',
                    'conservative', 'liberal', 'maga', 'politician', 'political',
                    'activism', 'freedomofspeech', 'globalist', 'deepstate'],
    'news':        ['war', 'warcrimes', 'genocide', 'military', 'conflict',
                    'geopolitical', 'breakingnews', 'crisis', 'protest', 'protests',
                    'terrorism', 'immigration', 'censorship', 'propaganda',
                    'currentevents', 'worldnews', 'nwo'],
}
for _leaf, _terms in _NEW_SOURCES.items():
    _SOURCES.setdefault(_leaf, []).extend(_terms)

# De-dupe each source list, preserving order.
for _leaf in list(_SOURCES):
    _seen: Set[str] = set()
    _SOURCES[_leaf] = [t for t in _SOURCES[_leaf] if not (t in _seen or _seen.add(t))]

# Every source key must be a real leaf.
for _leaf in _SOURCES:
    if _leaf not in LEAVES:
        raise ValueError(f"source list targets non-leaf {_leaf!r}")

# Invert to Hive-tag -> {leaves}.
HIVE_TAG_MAP: Dict[str, Set[str]] = {}
for _leaf, _terms in _SOURCES.items():
    for _t in _terms:
        HIVE_TAG_MAP.setdefault(_t, set()).add(_leaf)

# Hive tags that map to a CATEGORY rather than a leaf. Movie/TV review content
# (CineTV, Movies & TV Shows) has no dedicated leaf but clearly belongs in
# Entertainment. Values may be categories or leaves — validated against the full
# tag namespace, not just LEAVES.
EXTRA_HIVE_TAG_MAP: Dict[str, Set[str]] = {}
for _t in ('cinetv', 'movie', 'movies', 'moviereview', 'moviereviews',
           'film', 'films', 'filmreview', 'tvshow', 'tvshows', 'tvseries',
           'cinema', 'boxoffice', 'netflix', 'seriesreview', 'series', 'serie'):
    EXTRA_HIVE_TAG_MAP[_t] = {'film-tv'}
for _t, _tags in EXTRA_HIVE_TAG_MAP.items():
    if _tags - TAXONOMY_V2:
        raise ValueError(f"EXTRA_HIVE_TAG_MAP {_t!r} -> unknown {_tags - TAXONOMY_V2}")


# ─────────────────────────────────────────────────────────────────────────────
# Community maps.  Reuse v1's, remapping only the leaves that no longer exist as
# topics: v1 'tutorial' communities (DIYHub, Hive Diy) are craft communities ->
# 'diy-crafts'; v1 'vlog' communities -> 'lifestyle'.
# ─────────────────────────────────────────────────────────────────────────────
# 'vlog' is a real v2 leaf again, so vlog communities keep mapping to it.
_LEAF_OVERRIDE = {'tutorial': 'diy-crafts'}


def _remap(leafset: Set[str]) -> Set[str]:
    return {_LEAF_OVERRIDE.get(l, l) for l in leafset}


EXCLUSIVE_COMMUNITY_MAP: Dict[str, Set[str]] = {
    k: _remap(v) for k, v in v1.EXCLUSIVE_COMMUNITY_MAP.items()
}
COMMUNITY_MAP: Dict[str, Set[str]] = {
    k: _remap(v) for k, v in v1.COMMUNITY_MAP.items()
}

# ── v2-only community rules ──────────────────────────────────────────────────
# Rules that only make sense against v2 leaves — mostly the dev communities
# that motivated the 'programming' leaf. Author Hive tags almost never carry it
# (1 silver example in 800), so community membership is its main evidence.
# Keyed by hive-id AND lowercased display name, same convention as v1: legacy
# `videos.community` holds display names, embed `category` holds hive-ids.
# Ids resolved via bridge.list_communities, 2026-07-17.
_V2_EXCLUSIVE_ADDITIONS: Dict[str, Set[str]] = {
    # --- programming ---------------------------------------------------------
    'hive-139531': {'programming'}, 'hivedevs': {'programming'},
    'hive-169321': {'programming'}, 'programming & dev': {'programming'},
    'hive-102677': {'programming'}, 'devtalk': {'programming'},
    'hive-154226': {'programming'}, 'develop spanish': {'programming'},
    'hive-129924': {'programming'}, 'python': {'programming'},
    'hive-164872': {'programming'}, 'learning python': {'programming'},
    'hive-146513': {'programming'}, 'hivesql': {'programming'},
    'hive-128612': {'programming'}, 'hivemind': {'programming'},
    'programming/dev': {'programming'},
    'web development': {'programming'},
    'mobile app developer': {'programming'},
    'ios development': {'programming'},
    'flutter devs': {'programming'},
    # --- game development: both sides of the fence ---------------------------
    'hive-176981': {'programming', 'gaming'}, 'game development': {'programming', 'gaming'},
    'hive-195736': {'programming', 'gaming'}, 'game developers mx': {'programming', 'gaming'},
    'hive-150008': {'programming', 'gaming'}, 'game dev 🕹️': {'programming', 'gaming'},
    'game developers': {'programming', 'gaming'},
    # --- single-topic communities with a category-level home ------------------
    # movie/TV reviews: vision misreads cinematic stills (a film scene scored
    # 'spirituality'), so membership decides outright.
    'hive-166847': {'film-tv'}, 'movies & tv shows': {'film-tv'},
    'hive-121744': {'film-tv'}, 'cinetv': {'film-tv'},
}
EXCLUSIVE_COMMUNITY_MAP.update(_V2_EXCLUSIVE_ADDITIONS)

# Evidence only (classifier still runs): broader tech/science communities.
# NOTE: 'Personal Development' is deliberately lifestyle, NOT programming.
# Values may be leaves OR category slugs (flat namespace) — v1 had to exclude
# Movies & TV Shows entirely; v2 files it as 'entertainment' (see exclusive map).
_V2_EVIDENCE_ADDITIONS: Dict[str, Set[str]] = {
    'hive-196387': {'science'}, 'stemsocial': {'science'},
    'hive-163521': {'science'}, 'stemgeeks': {'science'},
    'stemspace': {'science'},
    'hive-127817': {'science', 'technology'}, 'science & technology': {'science', 'technology'},
    'hive-152724': {'technology'}, 'linux': {'technology'},
    'hive-116823': {'technology'}, 'linux&softwarelibre': {'technology'},
    'hive-132127': {'technology'}, 'infotech': {'technology'},
    'hive-178653': {'technology', 'programming'}, 'nerds of hive': {'technology', 'programming'},
    'technology': {'technology'}, 'tech lovers': {'technology'},
    'technical analysis': {'finance'},
    'hive-113523': {'lifestyle'}, 'personal development': {'lifestyle'},
    # chess community — real chess content, kept after dropping the noisy
    # 'chessbrothers' tag; classifier still runs on top.
    'hive-157286': {'sports'}, 'the chess community': {'sports'},
}
COMMUNITY_MAP.update(_V2_EVIDENCE_ADDITIONS)

for _cmap in (EXCLUSIVE_COMMUNITY_MAP, COMMUNITY_MAP):
    for _k, _v in _cmap.items():
        bad = _v - TAXONOMY_V2   # leaves and category slugs are both valid tags
        if bad:
            raise ValueError(f"community {_k!r} maps to unknown tag {bad}")


# ─────────────────────────────────────────────────────────────────────────────
# Facet 3 — conditional sub-genres.  Documented; not run until built.
# ─────────────────────────────────────────────────────────────────────────────
GENRE_TREES: Dict[str, List[str]] = {
    'music': ['rock', 'pop', 'hip-hop', 'electronic', 'jazz', 'classical',
              'folk', 'latin', 'reggae', 'metal', 'country', 'rnb-soul',
              'gospel', 'afrobeat', 'world'],
    'diy-crafts': ['woodworking', 'building', 'crafting', 'upcycling',
                   'sewing', 'electronics-making', 'home-improvement', 'restoration'],
    'gaming': ['fps', 'rpg', 'strategy', 'simulation', 'retro', 'mmo',
               'mobile', 'sandbox'],
}

# ─────────────────────────────────────────────────────────────────────────────
# Facet 1 — FORMAT.  Kept as documentation + an internal signal: format steers
# downstream processing (a gameplay clip is a gaming video; a music performance
# is a music video). Not a user-facing browsable facet. FORMAT_TO_TOPIC is the
# only part that feeds tagging today.
# ─────────────────────────────────────────────────────────────────────────────
FORMAT_LABELS: List[str] = [
    'talking-head', 'vlog', 'interview', 'documentary', 'tutorial', 'screencast',
    'presentation', 'gameplay', 'music-performance', 'music-video',
    'livestream-rec', 'short', 'animation', 'compilation',
]

# Formats that are effectively a topic answer. Used as a prior / direct evidence.
# Values may be a leaf OR a category slug (flat namespace): 'animation' can't
# pick a leaf, but animated story content belongs in Entertainment — and its
# rendered look otherwise fools CLIP into 'technology'.
FORMAT_TO_TOPIC: Dict[str, str] = {
    'gameplay':          'gaming',
    'screencast':        'programming',
    'music-performance': 'music',
    'music-video':       'music',
    'animation':         'story-time',   # animated content here is story clips
}

# ─────────────────────────────────────────────────────────────────────────────
# Cheap facets stored as side-channels (backwards-compatible optional keys).
# ─────────────────────────────────────────────────────────────────────────────
DURATION_BUCKETS = ('short', 'standard', 'long')       # <60s / 60s-20m / >20m
CONTENT_FLAGS = ('has-speech', 'no-speech', 'music-only',
                 'sensitive:political', 'sensitive:mature', 'sensitive:graphic')


def duration_bucket(seconds: Optional[float]) -> Optional[str]:
    """Map a duration in seconds to a bucket, or None if unknown."""
    if seconds is None:
        return None
    if seconds < 60:
        return 'short'
    if seconds <= 1200:
        return 'standard'
    return 'long'


# ─────────────────────────────────────────────────────────────────────────────
# AI-generated content — cheap side-channel facet, metadata only (no model).
# Two signals, both text-shape and available before any transcode/transcribe:
#   1. the author's own Hive tags naming an AI tool/label
#   2. the title/description explicitly saying the content is AI-made
# The description is the stronger signal in practice — creators often reuse
# the same generic Hive tags (nature/life/village/...) across every upload
# regardless of whether that particular one is AI-made, and only say so in
# the body text (e.g. "Scary AI generated", "AI video that looks so real").
# ─────────────────────────────────────────────────────────────────────────────
AI_HIVE_TAGS: Set[str] = {
    'ai', 'aiart', 'aivideo', 'aishorts', 'aianimation', 'aigenerated',
    'ai-generated', 'aicreated', 'aigenerativeart', 'artificialintelligence',
    'genai', 'generativeai', 'midjourney', 'stablediffusion', 'stable-diffusion',
    'dalle', 'dalle2', 'dalle3', 'chatgpt', 'sora', 'soraai', 'runway',
    'runwayml', 'kling', 'klingai', 'veo', 'pika', 'pikalabs', 'luma', 'lumaai',
    'heygen', 'synthesia', 'sunoai', 'suno', 'invideoai', 'leonardoai',
    'deepfake', 'aimusic',
    # Generic AI-content labels.
    'aivideos', 'aishort', 'aireels', 'aifilm', 'aimovie', 'aicinema',
    'aistory', 'aistories', 'aiimage', 'aiimages', 'aiphoto', 'aiportrait',
    'aiartwork', 'aiartist', 'aiartcommunity', 'aicharacter', 'aigirl',
    'aiinfluencer', 'aimodel', 'aibaby', 'aianimals', 'aifantasy', 'aihorror',
    'aiscifi', 'aisong', 'aicover', 'aivoice', 'aivoiceover', 'aigenerate',
    'aigen', 'aicontent', 'aiedit', 'madewithai', 'createdwithai',
    'generatedwithai', 'generatedbyai', 'texttovideo', 'text2video',
    'imagetovideo', 'img2vid', 'deepfakes', 'faceswap', 'voiceclone',
    # Video generators.
    'sora2', 'openaisora', 'veo2', 'veo3', 'googleveo', 'kling2', 'klingai2',
    'runwaygen3', 'runwaygen4', 'dreammachine', 'hailuo',
    'hailuoai', 'minimax', 'minimaxai', 'pixverse', 'pixverseai', 'vidu',
    'viduai', 'seedance', 'dreamina', 'jimeng', 'hunyuan', 'hunyuanvideo',
    'wanai', 'wan2', 'wan21', 'wan22', 'higgsfield', 'higgsfieldai', 'hedra',
    'viggle', 'viggleai', 'domoai', 'kaiber', 'kaiberai', 'genmo', 'ltxvideo',
    'ltxstudio', 'animatediff', 'deforum', 'polloai', 'pikaai', 'grokimagine',
    'metaai', 'moviegen', 'pictory', 'fliki', 'deevid', 'krea',
    'kreaai', 'vizuraai', 'vizura',
    # Image generators.
    'fluxai', 'flux1', 'ideogram', 'imagen3', 'googleimagen', 'adobefirefly',
    'fireflyai', 'nanobanana', 'openart', 'nightcafe', 'playgroundai',
    'leonardo_ai', 'comfyui', 'automatic1111', 'sdxl', 'bingimagecreator',
    'dalle-3', 'gptimage', 'chatgptimage',
    # Voice / music / avatar generators.
    'udio', 'udioai', 'elevenlabs', 'heygenai', 'captionsai',
}

# Creators who only publish AI-generated content: every video of theirs is
# flagged regardless of metadata (evidence "creator:<name>"). Hive usernames,
# lowercase.
AI_CREATORS: Set[str] = {
    'zoorisas-ia',
    'mdmilon12',
}

# Phrases that assert the CONTENT was AI-made, not merely a video "about AI".
# Deliberately excludes bare tool-name mentions (chatgpt/midjourney/...): those
# fire on incidental tool credits — "thumbnail edited by ChatGPT" on an
# otherwise normal filmed video — without saying the video itself is AI-made.
# The `(?!\s+generation\b)` guard on the video/short/... alternative keeps
# "AI video generation" (a mention of the technology/field) from matching —
# only a direct "AI video" / "AI short" self-description counts.
_AI_TEXT_PATTERNS = [
    r'\bai[\s-]?generated\b',
    r'\bai[\s-]?created\b',
    r'\bai[\s-]?made\b',
    r'\bgenerated (?:by|using|with)\s+ai\b',
    r'\b(?:made|created|produced)\s+(?:by|using|with)\s+(?:ai|artificial intelligence)\b',
    r'\bpowered by ai\b',
    r'\b100%\s*ai\b',
    # "AI video", "Epic AI Movie", "AI sci-fi animation", "AI creature battle
    # animation" — up to two descriptive words between "AI" and the noun, but
    # never a function word ("AI tools for video" is about AI, not AI-made),
    # and never followed by a tooling noun ("AI video generator/editor").
    r'\bai[ \t]+(?:(?!(?:for|to|and|or|in|on|of|the|a|an|is|are|will|can|with|about|vs|news'
    r'|this|that|these|those|it|its|my|our|your|his|her|their|i|we|you)\b)[\w-]+[ \t]+){0,2}'
    r'(?:video|videos|short|shorts|animation|animations|art|artwork|image|images|clip|clips'
    r'|movie|movies|film|films|cinema|story|stories|fantasy|horror|music video|song|cover'
    r'|trailer|character|characters|creature|creatures|monster|monsters|battle|scene)\b'
    r'(?!\s+(?:generation|generator|generators|tool|tools|editor|editing|app|apps|tutorial|course|prompt|prompts)\b)',
    r'\b(?:realistic|hyper[\s-]?realistic|cinematic)\s+ai\b',
    r'\bartificial intelligence\b(?:[^.]{0,20})\b(?:generated|created|made)\b',
    # A named generator tied to a generation verb or credit line — a bare
    # tool mention still doesn't count (see note above). Plain "InVideo" is
    # left out: it's also a template editor ("made in InVideo" on gameplay).
    r'\b(?:made|created|generated|animated|rendered|produced)\s+(?:by|using|with|in|on)\s+'
    r'(?:sora|veo\s?\d?|kling|runway|pika|luma|dream machine|hailuo|minimax|pixverse|vidu'
    r'|seedance|dreamina|hunyuan|higgsfield|hedra|viggle|domo\s?ai|kaiber|midjourney'
    r'|stable diffusion|dall-?e\s?\d?|flux|ideogram|leonardo\s?ai|grok imagine|invideo\s?ai'
    r'|suno|udio|heygen|synthesia|elevenlabs)\b',
    # Channels that only publish AI-generated content; their name alone (as a
    # "Credit: VIZURAAI" line) marks the video.
    r'\bvizura\s?ai\b',
    # This channel's clickbait framing for AI content: "AI or real?" / "real
    # or AI?" — a rhetorical hook, not a factual claim, but specific enough
    # (unlike a bare tool-name mention) that using it is itself the signal.
    r'\bai or (?:real|fake)\b',
    r'\breal or ai\b',
]
AI_TEXT_RE = re.compile('|'.join(_AI_TEXT_PATTERNS), re.IGNORECASE)
MAX_AI_EVIDENCE = 5

# Creators often duplicate their tags as inline #hashtags in the description
# rather than (or in addition to) the formal hive_tags array — scan for those
# too and match them against the same AI_HIVE_TAGS vocabulary.
_HASHTAG_RE = re.compile(r'#(\w+)')

# A match is discarded when one of these immediately precedes it (within a
# short window): they mark the sentence as describing THIRD-PARTY content
# ("the White House posted an AI-generated video") rather than this video.
_AI_REPORTING_CUES = (
    'posted', 'shared', 'released', 'announced', 'uploaded', 'published',
    'went viral', 'reportedly', 'according to',
)
_CUE_WINDOW = 60


def detect_ai_generated(hive_tags: Any, title: str = '', body: str = '',
                        author: str = '') -> Tuple[bool, List[str]]:
    """
    Best-effort "was this AI-made" flag from metadata alone — no transcript,
    no model. Checked in order: the creator (AI_CREATORS), the author's Hive
    tags, then title/description text. Returns (is_ai, evidence); evidence lists the matched tag(s) and/or
    phrase(s) (capped) so a bad call is auditable later.
    """
    evidence: List[str] = []

    if author and author.strip().lower() in AI_CREATORS:
        evidence.append(f"creator:{author.strip().lower()}")

    tags = hive_tags or []
    if isinstance(tags, str):
        tags = tags.split(',')
    for raw in tags:
        tag = normalize_hive_tag(raw) if raw else ''
        if tag and tag in AI_HIVE_TAGS:
            marker = f"tag:{tag}"
            if marker not in evidence:
                evidence.append(marker)

    text = f"{title or ''} {body or ''}"

    for m in _HASHTAG_RE.finditer(text):
        tag = normalize_hive_tag(m.group(1))
        if tag and tag in AI_HIVE_TAGS:
            marker = f"hashtag:{tag}"
            if marker not in evidence:
                evidence.append(marker)

    for m in AI_TEXT_RE.finditer(text):
        window = text[max(0, m.start() - _CUE_WINDOW):m.start()].lower()
        if any(cue in window for cue in _AI_REPORTING_CUES):
            continue
        marker = f"text:{m.group(0).strip().lower()}"
        if marker not in evidence:
            evidence.append(marker)
        if len(evidence) >= MAX_AI_EVIDENCE:
            break

    return bool(evidence), evidence[:MAX_AI_EVIDENCE]


# ─────────────────────────────────────────────────────────────────────────────
# Lookups + resolution.
# ─────────────────────────────────────────────────────────────────────────────
def category_of(tag: str) -> Optional[str]:
    """The category slug for a leaf; the slug itself if `tag` is a category; else None."""
    if tag in CATEGORIES:
        return tag
    return LEAF_TO_CATEGORY.get(tag)


def with_categories(tags: List[str]) -> List[str]:
    """Append each leaf's parent category (roll-up), preserving order, no dupes."""
    out: List[str] = []
    for t in tags:
        if t not in out:
            out.append(t)
    for t in list(out):
        cat = LEAF_TO_CATEGORY.get(t)
        if cat and cat not in out:
            out.append(cat)
    return out


def tags_from_hive_tags(hive_tags: Any) -> Set[str]:
    """Map author-supplied Hive tags onto v2 leaves, dropping platform noise."""
    if not hive_tags:
        return set()
    if isinstance(hive_tags, str):
        hive_tags = hive_tags.split(',')
    result: Set[str] = set()
    for raw in hive_tags:
        tag = normalize_hive_tag(raw)
        if not tag or tag in HIVE_TAG_STOPWORDS:
            continue
        if tag.startswith('hive-'):
            result |= COMMUNITY_MAP.get(tag, set())
            continue
        result |= HIVE_TAG_MAP.get(tag, set())
        result |= EXTRA_HIVE_TAG_MAP.get(tag, set())   # movie/TV -> entertainment
        if tag in TAXONOMY_V2:     # author used a leaf/category word directly
            result.add(tag)
    return result


def tags_from_category(category: Any) -> Set[str]:
    """Map a Hive community id / category string onto v2 leaves (evidence)."""
    if not category or not isinstance(category, str):
        return set()
    return set(COMMUNITY_MAP.get(category.strip().lower(), set()))


def exclusive_tags_for(category: Any) -> List[str]:
    """Definitive tags for a single-topic community, or [] — then run nothing else."""
    if not category or not isinstance(category, str):
        return []
    tags = EXCLUSIVE_COMMUNITY_MAP.get(category.strip().lower())
    return sorted(tags) if tags else []


def resolve_tags(leaf_scores: Dict[str, float],
                 leaf_thresholds: Optional[Dict[str, float]] = None,
                 default_threshold: float = 0.55,
                 category_threshold: float = 0.45,
                 max_tags: int = 5) -> List[str]:
    """
    Turn per-leaf classifier scores into a flat tag list, with the leaf->category
    fallback.

      * a leaf that clears its threshold is emitted (specific tag).
      * for a category with NO qualifying leaf, if its best leaf score still
        clears `category_threshold`, the CATEGORY slug is emitted instead — a
        coarser but correct tag ("clearly Tech & Science, can't split the leaf").

    Returns leaves first (highest score first), then fallback categories, capped
    at `max_tags`.
    """
    leaf_thresholds = leaf_thresholds or {}
    chosen = [(l, s) for l, s in leaf_scores.items()
              if l in LEAVES and s >= leaf_thresholds.get(l, default_threshold)]
    chosen.sort(key=lambda ls: ls[1], reverse=True)
    tags: List[str] = [l for l, _ in chosen]
    covered = {category_of(l) for l, _ in chosen}

    for cat, leaves in CATEGORY_TREE.items():
        if cat in covered:
            continue
        best = max((leaf_scores.get(l, 0.0) for l in leaves), default=0.0)
        if best >= category_threshold:
            tags.append(cat)
            covered.add(cat)

    return tags[:max_tags]


if __name__ == '__main__':
    # Quick self-check / vocabulary dump.
    print(f"categories: {len(CATEGORIES)}   leaves: {len(LEAVES)}   "
          f"total tags: {len(TAXONOMY_V2)}")
    for cat, leaves in CATEGORY_TREE.items():
        print(f"  {cat:15s} ({CATEGORY_NAMES[cat]}): {', '.join(leaves)}")
    print(f"\nhive-tag map entries: {len(HIVE_TAG_MAP)}")
    print("sample resolutions:")
    for label, scores in [
        ('clear gaming', {'gaming': 0.9, 'technology': 0.2}),
        ('ambiguous tech', {'programming': 0.5, 'technology': 0.5, 'science': 0.48}),
        ('nothing', {'food': 0.3, 'travel': 0.2}),
    ]:
        print(f"  {label:16s} -> {resolve_tags(scores)}")
