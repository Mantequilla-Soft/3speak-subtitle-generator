"""
Tag taxonomy: maps author-supplied Hive tags and Hive community IDs onto the
project's fixed tag vocabulary.

Hive tags are human-authored, so when one maps cleanly onto our taxonomy it is
far more reliable than a zero-shot guess over a transcript. A large share of
Hive tags are platform/curation noise ("3speak", "ecency", "neoxian") and carry
no topical signal at all, so they are dropped rather than fed to the classifier.

The maps below were derived from every Hive tag used at least 12 times across
published embed videos, plus the top ~30 communities by video count.
"""

import re
import logging
from typing import Any, Dict, List, Set

logger = logging.getLogger(__name__)

# The fixed output vocabulary. Must stay in sync with `tags:` in config.yaml.
TAXONOMY: Set[str] = {
    'food', 'travel', 'vlog', 'tutorial', 'education', 'technology',
    'gaming', 'music', 'art', 'health', 'news', 'sports', 'nature',
    'science', 'cryptocurrency', 'finance',
}

# Platform, reward-token, curation-trail, language and community-membership tags.
# These are the highest-frequency tags in the corpus and mean nothing topically.
HIVE_TAG_STOPWORDS: Set[str] = {
    # platform / frontend
    '3speak', 'threespeak', '3shorts', 'three', 'speak', 'video', 'hive',
    'ecency', 'peakd', 'dbuzz', 'inleo', 'leofinance', 'waivio', 'waves',
    'short', 'shorts', 'snaps', 'podcast', 'test', 'originalcontent',
    'enlace', 'embed', 'reel', 'reels',
    # reward tokens / curation trails
    'neoxian', 'pob', 'proofofbrain', 'palnet', 'archon', 'creativecoin',
    'appreciator', 'qurator', 'ocd', 'ocdb', 'curie', 'curangel', 'gems',
    'bbh', 'waiv', 'pimp', 'vyb', 'cent', 'bilpcoin', 'slothbuzz', 'tribes',
    'hive-engine', 'alive', 'hmf', 'dew', 'splash', 'oneup', 'entropia',
    'splintertalks', 'vdc', 'hivegc', 'arcadecolony', 'ctp', 'lassecash',
    # community / regional membership
    'indiaunited', 'teamuk', 'hivebr', 'hivepakistan', 'india', 'indonesia',
    'cuba', 'venezuela', 'japan', 'tokyo', 'akihabara', 'asean', 'aseanhive',
    # languages
    'english', 'spanish', 'deutsch', 'espanol', 'português', 'portugues',
    # generic filler
    'life', 'love', 'enjoy', 'hot', 'kiss', 'daily', 'contest', 'meme',
    'funny', 'romance', 'romantic', 'blog', 'content', 'post', 'new',
}

# Canonical tag -> Hive tags that imply it.
_CANONICAL_SOURCES: Dict[str, List[str]] = {
    'gaming': [
        'gaming', 'gameplay', 'videogames', 'videogame', 'games', 'game',
        'gamers', 'gamer', 'hivegaming', 'retrogaming', 'pcgames', 'pcgaming',
        'gamingphotography', 'gamingphotocontest', 'splinterlands', 'play2earn',
        'roguelike', 'cavesofqud', 'rpg', 'roleplay', 'gta', 'minecraft',
        'steam', 'nintendo', 'playstation', 'xbox', 'speedrun', 'letsplay',
    ],
    'music': [
        'music', 'musica', 'openmic', 'coversong', 'cover', 'instrumental',
        'guitar', 'guitarra', 'musicforlife', 'sound', 'sounds', 'song',
        'singing', 'singer', 'piano', 'drums', 'band', 'afri-tunes',
        'afritunes', 'musiczone', 'hivemusic', 'rap', 'jazz', 'rock',
    ],
    'art': [
        'art', 'drawing', 'draw', 'painting', 'paint', 'ink', 'pen',
        'watercolor', 'gouache', 'abstract', 'sketchbook', 'sketch',
        'creative', 'illustration', 'artwork', 'digitalart', 'craft',
        'crafts', 'manualidad', 'manualidades', 'artesania', 'yeso',
        'diy', 'diyhub', 'handmade', 'needlework', 'pottery', 'sculpture',
    ],
    'tutorial': [
        'tutorial', 'tutoriales', 'howto', 'how-to', 'diy', 'diyhub',
        'stepbystep', 'guide', 'lesson', 'walkthrough', 'recipe', 'receta',
    ],
    'education': [
        'education', 'educacion', 'learning', 'teaching', 'school',
        'university', 'study', 'math', 'maths', 'mathematics', 'calculus',
        'physics', 'electromagnetism', 'chemistry', 'biology', 'history',
    ],
    'science': [
        'science', 'ciencia', 'physics', 'math', 'maths', 'mathematics',
        'calculus', 'electromagnetism', 'chemistry', 'biology', 'astronomy',
        'research', 'space',
    ],
    'technology': [
        'technology', 'tech', 'ai', 'artificialintelligence', 'web3',
        'software', 'programming', 'coding', 'computer', 'linux',
        'opensource', 'gadgets', 'electronics',
    ],
    'cryptocurrency': [
        'crypto', 'cryptocurrency', 'bitcoin', 'btc', 'ethereum', 'eth',
        'altcoin', 'blockchain', 'defi', 'nft', 'mining', 'hodl',
    ],
    'finance': [
        'finance', 'investing', 'investment', 'trading', 'stocks', 'money',
        'economy', 'economics', 'markets', 'business',
    ],
    'food': [
        'food', 'hivefood', 'cooking', 'cook', 'recipe', 'receta', 'comida',
        'baking', 'kitchen', 'foodie', 'vegan', 'restaurant', 'meal',
    ],
    'travel': [
        'travel', 'travelfeed', 'worldmappin', 'trip', 'tourism', 'viaje',
        'adventure', 'wanderlust', 'roadtrip', 'backpacking',
    ],
    'nature': [
        'nature', 'naturaleza', 'wildlife', 'plants', 'garden', 'gardening',
        'forest', 'mountain', 'beach', 'ocean', 'animals', 'birds', 'rain',
        'landscape',
    ],
    'sports': [
        'sports', 'sport', 'deportes', 'fulldeportes', 'football', 'soccer',
        'worldcup', 'basketball', 'chess', 'chessbrothers', 'fitness',
        'running', 'cycling', 'boxing', 'mma',
    ],
    'health': [
        'health', 'salud', 'wellness', 'mentalhealth', 'medicine',
        'nutrition', 'yoga', 'meditation', 'workout', 'gym',
    ],
    'news': [
        'news', 'noticias', 'politics', 'current-events', 'conspiracy',
        'psyop', '911truth', 'deepdives', 'informationwar', 'geopolitics',
    ],
    'vlog': [
        'vlog', 'vlogging', 'lifestyle', 'dailyvlog', 'personal',
    ],
}

# Hive tag -> set of taxonomy tags (inverted from _CANONICAL_SOURCES).
HIVE_TAG_MAP: Dict[str, Set[str]] = {}
for _canonical, _sources in _CANONICAL_SOURCES.items():
    if _canonical not in TAXONOMY:
        raise ValueError(f"canonical tag {_canonical!r} is not in TAXONOMY")
    for _src in _sources:
        HIVE_TAG_MAP.setdefault(_src, set()).add(_canonical)

# Communities whose topic is not in doubt: membership alone DECIDES the tags.
# The classifier is skipped entirely — no extra tags are added, no transcript is
# consulted. Use this only where the community is unambiguously single-topic.
#
# Keyed by BOTH the hive-XXXX id and the display name (lowercased), because the
# category/community field in Mongo carries either form depending on the record.
# Deliberately EXCLUDED — reviewed and judged NOT single-topic, so they still go
# through the classifier:
#     Geek Zone (hive-106817), Deep Dives (hive-122315)
#     Hive Learners (hive-153850) — a learning community, but the videos span
#         many topics, so 'education' alone was too blunt. Classifier decides.
#     Music Technology (hive-111482), Dance and music (hive-118409)
#     Q Inspired-by-Music (hive-192806), Gaming Music (CC-BY)
#     Hive Music School, Music Theory
#     'general' (128k videos — the default category, no topic)
#     Threespeak / Threeshorts / Ecency / GEMS — platform containers
#     Movies & TV Shows, SciFi Multiverse, Spooky Zone, Motherhood — no tag in
#         the taxonomy fits these; mapping them would be worse than leaving them.
EXCLUSIVE_COMMUNITY_MAP: Dict[str, Set[str]] = {
    # --- music ---------------------------------------------------------------
    'hive-193816': {'music'}, 'music': {'music'},                    # Music
    'hive-120026': {'music'}, 'music zone': {'music'},               # Music Zone
    'hive-105786': {'music'}, 'hive open mic': {'music'},            # Hive Open Mic
    'hive-195772': {'music'}, 'afri-tunes': {'music'},               # AFRI-TUNES
    'hive-148115': {'music'}, 'sound music': {'music'},              # Sound Music
    'hive-141487': {'music'}, 'hive music': {'music'},               # Hive Music
    'hive-175836': {'music'}, 'musicforlife': {'music'},             # Musicforlife
    'hive-168505': {'music'}, 'hivemusic': {'music'},                # HiveMusic
    'hive-129959': {'music'}, 'dsound': {'music'},                   # DSound
    'blocktunes': {'music'},
    'electronic music': {'music'},
    'experimental music': {'music'},
    'classical music': {'music'},
    'original music': {'music'},
    'indiemusic': {'music'},
    'musichive': {'music'},
    'musicbulgaria': {'music'},
    'world of music': {'music'},
    'music everywhere': {'music'},
    'ear candy music': {'music'},
    'music on vinyl': {'music'},
    'music on leo': {'music'},
    'hive music italia': {'music'},
    'music 4 peace': {'music'},
    'stick up music': {'music'},
    'audio hive': {'music'},

    'hive-140169': {'music'}, 'vibes': {'music'},                     # Vibes

    # --- gaming ---------------------------------------------------------------
    'hive-140217': {'gaming'}, 'hive gaming': {'gaming'},            # Hive Gaming
    'hive-13323': {'gaming'}, 'splinterlands': {'gaming'},           # Splinterlands
    'hive-185676': {'gaming'}, 'gaming photography': {'gaming'},     # Gaming Photography
    'hive-195370': {'gaming'}, 'rising star game': {'gaming'},       # Rising Star Game

    # --- sports ---------------------------------------------------------------
    'hive-173115': {'sports'}, 'skatehive': {'sports'},              # SkateHive
    'hive-189157': {'sports'}, 'full deportes': {'sports'},          # Full Deportes
    'hive-101690': {'sports'}, 'sports talk social': {'sports'},     # Sports Talk Social

    # --- vlog -----------------------------------------------------------------
    'hive-155221': {'vlog'}, 'we are alive tribe': {'vlog'},         # We Are Alive Tribe
    'hive-145796': {'vlog'}, 'espavlog': {'vlog'},                   # EspaVlog
    'hive-11800': {'vlog'}, 'hive sucre': {'vlog'},                  # Hive Sucre
    'hive-187189': {'vlog'}, 'lifestyle': {'vlog'},                  # Lifestyle

    # --- food -----------------------------------------------------------------
    'hive-120586': {'food'}, 'foodies bee hive': {'food'},           # Foodies Bee Hive
    'hive-100067': {'food'}, 'hive food': {'food'},                  # Hive Food

    # --- art ------------------------------------------------------------------
    'hive-158694': {'art'}, 'alien art hive': {'art'},               # Alien Art Hive

    # --- nature ---------------------------------------------------------------
    'hive-150210': {'nature'}, 'clean planet': {'nature'},           # CLEAN PLANET
    'hive-114308': {'nature'}, 'homesteading': {'nature'},           # Homesteading
    'hive-140635': {'nature'}, 'hivegarden': {'nature'},             # HiveGarden
    'hive-147663': {'nature'}, 'hive gardening': {'nature'},         # Hive Gardening

    # --- science / education --------------------------------------------------
    'hive-128780': {'science'}, 'mes science': {'science'},          # MES Science

    # --- news -----------------------------------------------------------------
    'hive-109255': {'news'}, 'news & views': {'news'},               # News & Views

    # --- travel ---------------------------------------------------------------
    'hive-163772': {'travel'}, 'worldmappin': {'travel'},            # Worldmappin

    # --- finance / crypto -----------------------------------------------------
    'hive-167922': {'finance'}, 'leofinance': {'finance'},           # LeoFinance
    'hive-106130': {'cryptocurrency'}, 'spendhbd': {'cryptocurrency'},  # SpendHBD

    # --- tutorial -------------------------------------------------------------
    'hive-189641': {'tutorial'}, 'diyhub': {'tutorial'},             # DIYHub
    'hive-130560': {'tutorial'}, 'hive diy': {'tutorial'},           # Hive Diy

    # --- plain category values ------------------------------------------------
    # Not Hive communities: raw category strings on older videos. They map just
    # as cleanly, so they get the same treatment.
    'crypto': {'cryptocurrency'},
    'politics': {'news'},
    'documentary': {'news'},
    'gaming': {'gaming'},
    'vlog': {'vlog'},
    'art': {'art'},
}


def exclusive_tags_for(category: Any) -> List[str]:
    """
    The definitive tags for a single-topic community, or [] if it isn't one.

    A non-empty result means: use exactly these tags and run nothing else — no
    classifier, no transcript, no extra topics.

    Implications still apply, so DIYHub ('tutorial') also yields 'education',
    exactly as a tutorial tagged any other way would. The primary tag stays
    first; implied tags follow.
    """
    if not category or not isinstance(category, str):
        return []
    tags = EXCLUSIVE_COMMUNITY_MAP.get(category.strip().lower())
    if not tags:
        return []
    return apply_implications(sorted(tags))


# Hive community ID -> taxonomy tags. Communities absent from this map (Ecency,
# Threespeak, GEMS, 3speak TEST, ...) are generic containers with no topic.
# These are *evidence* — the classifier may still add tags on top. For
# communities that should decide the tags outright, use EXCLUSIVE_COMMUNITY_MAP.
COMMUNITY_MAP: Dict[str, Set[str]] = {
    'hive-185676': {'gaming'},              # Gaming Photography
    'hive-140217': {'gaming'},              # Hive Gaming
    'hive-13323':  {'gaming'},              # Splinterlands
    'hive-102963': {'gaming'},              # The PEPT Game
    'hive-105786': {'music'},               # Hive Open Mic
    'hive-193816': {'music'},               # Music
    'hive-120026': {'music'},               # Music Zone
    'hive-195772': {'music'},               # AFRI-TUNES
    'hive-174301': {'art'},                 # Sketchbook
    'hive-158694': {'art'},                 # Alien Art Hive
    'hive-189641': {'art', 'tutorial'},     # DIYHub
    'hive-128780': {'science', 'education'},  # MES Science
    'hive-113182': {'news'},                # MES 9/11 Truth
    'hive-106474': {'news'},                # MES Conspiracy
    'hive-100067': {'food'},                # Hive Food
    'hive-189157': {'sports'},              # Full Deportes
    'hive-163772': {'travel'},              # Worldmappin
}

# Taxonomy tags that entail another tag. A how-to video is a teaching video, but
# the classifier scores the two labels independently and 'education' rarely
# clears its floor on its own, so the relationship is asserted rather than
# learned. Applied to the final tag list, whatever produced it.
TAG_IMPLICATIONS: Dict[str, Set[str]] = {
    'tutorial': {'education'},
}

_TAG_CLEAN_RE = re.compile(r'^[#\s]+|[\s,.;:]+$')

# Markdown / URL / HTML stripping for Hive post bodies.
_IMG_RE = re.compile(r'!\[[^\]]*\]\([^)]+\)')
_LINK_RE = re.compile(r'\[([^\]]+)\]\([^)]+\)')
_HTML_RE = re.compile(r'<[^>]+>')
_URL_RE = re.compile(r'https?://\S+')
_MD_SYNTAX_RE = re.compile(r'[*_>#`~|-]{1,}')
_WS_RE = re.compile(r'\s+')


def normalize_hive_tag(tag: str) -> str:
    """Lowercase and strip leading '#' / trailing punctuation (corpus has 'threespeak,')."""
    if not tag:
        return ''
    return _TAG_CLEAN_RE.sub('', str(tag).strip().lower())


def tags_from_hive_tags(hive_tags: Any) -> Set[str]:
    """Map author-supplied Hive tags onto the taxonomy, dropping platform noise."""
    if not hive_tags:
        return set()
    if isinstance(hive_tags, str):
        hive_tags = hive_tags.split(',')

    result: Set[str] = set()
    for raw in hive_tags:
        tag = normalize_hive_tag(raw)
        if not tag or tag in HIVE_TAG_STOPWORDS:
            continue
        # A bare community id in the tag list — resolve it as a community.
        if tag.startswith('hive-'):
            result |= COMMUNITY_MAP.get(tag, set())
            continue
        result |= HIVE_TAG_MAP.get(tag, set())
        # An author using a taxonomy word directly is the strongest signal there is.
        if tag in TAXONOMY:
            result.add(tag)
    return result


def tags_from_category(category: Any) -> Set[str]:
    """Map a Hive community ID onto the taxonomy. Generic communities yield nothing."""
    if not category or not isinstance(category, str):
        return set()
    return set(COMMUNITY_MAP.get(category.strip().lower(), set()))


def topical_hive_tags(hive_tags: Any) -> List[str]:
    """
    Author tags with platform noise removed, preserving order.

    These are fed to the classifier as extra text: even a tag we cannot map
    ("ac_origin", "bayek") is topical evidence the transcript may not carry.
    """
    if not hive_tags:
        return []
    if isinstance(hive_tags, str):
        hive_tags = hive_tags.split(',')

    out: List[str] = []
    for raw in hive_tags:
        tag = normalize_hive_tag(raw)
        if not tag or tag in HIVE_TAG_STOPWORDS or tag.startswith('hive-'):
            continue
        if tag not in out:
            out.append(tag)
    return out


def apply_implications(tags: List[str]) -> List[str]:
    """
    Expand a tag list with the tags it entails, e.g. 'tutorial' adds 'education'.

    Implied tags are inserted directly after the tag that triggered them, so the
    strongest signal stays first and a later truncation to max_tags drops the
    weakest tags rather than the implied ones.
    """
    out: List[str] = []
    for tag in tags:
        if tag not in out:
            out.append(tag)
        for implied in sorted(TAG_IMPLICATIONS.get(tag, ())):
            if implied not in out:
                out.append(implied)
    return out


def clean_post_body(body: str, max_chars: int = 1200) -> str:
    """Strip markdown, HTML and URLs from a Hive post body down to plain prose."""
    if not body:
        return ''
    text = _IMG_RE.sub(' ', body)
    text = _LINK_RE.sub(r'\1', text)
    text = _HTML_RE.sub(' ', text)
    text = _URL_RE.sub(' ', text)
    text = _MD_SYNTAX_RE.sub(' ', text)
    text = _WS_RE.sub(' ', text).strip()
    return text[:max_chars]
