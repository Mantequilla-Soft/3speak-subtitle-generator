"""
Unit tests for the deterministic half of tagging: Hive-tag/community mapping and
video-document normalization. No model required.

    python3 -m unittest discover -s tests -v
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from tag_taxonomy import (  # noqa: E402
    TAG_IMPLICATIONS,
    TAXONOMY,
    apply_implications,
    clean_post_body,
    normalize_hive_tag,
    tags_from_category,
    tags_from_hive_tags,
    topical_hive_tags,
)
from video_meta import normalize_video_metadata  # noqa: E402
from transcript_source import srt_to_text, word_count  # noqa: E402


class TestNormalizeHiveTag(unittest.TestCase):
    def test_strips_hash_case_and_trailing_punctuation(self):
        self.assertEqual(normalize_hive_tag('#3Speak'), '3speak')
        # The corpus really does contain a tag literally spelled "threespeak,".
        self.assertEqual(normalize_hive_tag('threespeak,'), 'threespeak')
        self.assertEqual(normalize_hive_tag('  GAMING '), 'gaming')

    def test_empty(self):
        self.assertEqual(normalize_hive_tag(''), '')


class TestTagsFromHiveTags(unittest.TestCase):
    def test_platform_noise_yields_nothing(self):
        noise = ['3speak', 'threespeak', 'ecency', 'neoxian', 'pob', 'ocd',
                 'qurator', 'hive', 'video', 'waves', 'english', 'spanish']
        self.assertEqual(tags_from_hive_tags(noise), set())

    def test_bullravi_gaming_video(self):
        """The real tags on bullravi/t0jwlsyl, which we previously tagged 'news'."""
        hive_tags = ['hive-185676', 'gaming', 'gamers', 'ac_origin',
                     'indiaunited', 'videogames', 'neoxian']
        self.assertEqual(tags_from_hive_tags(hive_tags), {'gaming'})

    def test_dubisortiz_craft_video(self):
        """dubisortiz/ljiskt3i: 'diy' implies both art and tutorial."""
        self.assertEqual(tags_from_hive_tags(['3speak', 'yeso', 'diy']),
                         {'art', 'tutorial'})

    def test_accepts_comma_string_legacy_shape(self):
        self.assertEqual(tags_from_hive_tags('gaming,3speak,music'),
                         {'gaming', 'music'})

    def test_embedded_community_id_resolves(self):
        self.assertEqual(tags_from_hive_tags(['hive-100067']), {'food'})

    def test_unknown_community_id_is_ignored_not_treated_as_word(self):
        self.assertEqual(tags_from_hive_tags(['hive-999999']), set())

    def test_all_outputs_are_in_taxonomy(self):
        every = ['gaming', 'diy', 'yeso', 'openmic', 'calculus', 'bitcoin',
                 'worldmappin', 'chessbrothers', 'conspiracy', 'lifestyle']
        self.assertTrue(tags_from_hive_tags(every) <= TAXONOMY)

    def test_none_and_empty(self):
        self.assertEqual(tags_from_hive_tags(None), set())
        self.assertEqual(tags_from_hive_tags([]), set())


class TestTopicalHiveTags(unittest.TestCase):
    def test_keeps_unmappable_topical_tags_drops_noise_and_ids(self):
        tags = ['hive-185676', 'gaming', 'ac_origin', 'neoxian', '3speak']
        self.assertEqual(topical_hive_tags(tags), ['gaming', 'ac_origin'])

    def test_deduplicates_preserving_order(self):
        self.assertEqual(topical_hive_tags(['bayek', 'bayek', 'yamu']),
                         ['bayek', 'yamu'])


class TestTagsFromCategory(unittest.TestCase):
    def test_known_topical_communities(self):
        self.assertEqual(tags_from_category('hive-185676'), {'gaming'})   # Gaming Photography
        self.assertEqual(tags_from_category('hive-189641'), {'art', 'tutorial'})  # DIYHub
        self.assertEqual(tags_from_category('hive-105786'), {'music'})    # Hive Open Mic

    def test_generic_containers_yield_nothing(self):
        # Ecency, Threespeak and 3speak TEST are the three largest communities
        # by video count and carry no topical meaning whatsoever.
        for generic in ('hive-125125', 'hive-181335', 'hive-146528'):
            self.assertEqual(tags_from_category(generic), set(), generic)

    def test_non_string_and_empty(self):
        self.assertEqual(tags_from_category(None), set())
        self.assertEqual(tags_from_category(''), set())
        self.assertEqual(tags_from_category(123), set())

    def test_returns_a_copy_not_the_shared_map_value(self):
        got = tags_from_category('hive-185676')
        got.add('mutated')
        self.assertEqual(tags_from_category('hive-185676'), {'gaming'})


class TestCleanPostBody(unittest.TestCase):
    def test_strips_markdown_images_links_html_and_urls(self):
        body = ('https://play.3speak.tv/embed?v=a/b\n\n---\n---\n\n'
                '* **Hello everyone.**\n![img](https://x/y.png)\n'
                '[PeakD](https://peakd.com) is a frontend.<br/>')
        out = clean_post_body(body)
        self.assertNotIn('http', out)
        self.assertNotIn('![', out)
        self.assertNotIn('<br', out)
        self.assertIn('Hello everyone.', out)
        self.assertIn('PeakD', out)  # link text survives, target does not

    def test_truncates_to_max_chars(self):
        self.assertEqual(len(clean_post_body('word ' * 2000, max_chars=100)), 100)

    def test_empty(self):
        self.assertEqual(clean_post_body(''), '')
        self.assertEqual(clean_post_body(None), '')


class TestNormalizeVideoMetadata(unittest.TestCase):
    def test_embed_video_shape(self):
        doc = {'hive_title': 'Bayek Returns to Yamu.', 'hive_body': 'After the mission...',
               'hive_tags': ['gaming', '3speak'], 'category': 'hive-185676'}
        meta = normalize_video_metadata(doc)
        self.assertEqual(meta['title'], 'Bayek Returns to Yamu.')
        self.assertEqual(meta['body'], 'After the mission...')
        self.assertEqual(meta['hive_tags'], ['gaming', '3speak'])
        self.assertEqual(meta['category'], 'hive-185676')

    def test_legacy_shape_with_comma_string_tags(self):
        doc = {'title': 'Old video', 'description': 'A description',
               'tags': 'oracle-d,steem,steemfest', 'community': 'hive-100067'}
        meta = normalize_video_metadata(doc)
        self.assertEqual(meta['title'], 'Old video')
        self.assertEqual(meta['body'], 'A description')
        self.assertEqual(meta['hive_tags'], ['oracle-d', 'steem', 'steemfest'])
        self.assertEqual(meta['category'], 'hive-100067')

    def test_embed_audio_shape(self):
        doc = {'title': 'An audio post', 'description': 'Body text', 'tags': ['music']}
        meta = normalize_video_metadata(doc)
        self.assertEqual(meta['title'], 'An audio post')
        self.assertEqual(meta['hive_tags'], ['music'])

    def test_hive_title_wins_over_generic_embed_title(self):
        doc = {'embed_title': 'Comment', 'hive_title': 'Real Title'}
        self.assertEqual(normalize_video_metadata(doc)['title'], 'Real Title')

    def test_generic_embed_title_is_discarded(self):
        doc = {'embed_title': 'Comment', 'originalFilename': 'My Cool Video.mp4'}
        self.assertEqual(normalize_video_metadata(doc)['title'], 'My Cool Video')

    def test_missing_fields_give_typed_empties(self):
        meta = normalize_video_metadata({})
        self.assertEqual(meta, {'title': '', 'body': '', 'hive_tags': [], 'category': ''})
        self.assertEqual(normalize_video_metadata(None)['hive_tags'], [])

    def test_whitespace_only_title_is_not_a_title(self):
        doc = {'hive_title': '   ', 'title': 'Fallback'}
        self.assertEqual(normalize_video_metadata(doc)['title'], 'Fallback')


class TestRegressionOnRealVideos(unittest.TestCase):
    """Both videos the reported bug was filed against."""

    def test_gaming_video_now_yields_gaming(self):
        doc = {'hive_title': 'Bayek Returns to Yamu.',
               'hive_tags': ['hive-185676', 'gaming', 'gamers', 'ac_origin',
                             'indiaunited', 'videogames', 'neoxian'],
               'category': 'hive-185676'}
        meta = normalize_video_metadata(doc)
        evidence = tags_from_hive_tags(meta['hive_tags']) | tags_from_category(meta['category'])
        self.assertEqual(evidence, {'gaming'})
        # The old output was news,travel,health,art,music — none of it survives.
        self.assertFalse(evidence & {'news', 'travel', 'health', 'music'})

    def test_craft_video_yields_art_and_tutorial(self):
        doc = {'hive_title': 'Manzana decorativa en yeso | Manualidad fácil y elegante',
               'hive_tags': ['3speak', 'yeso', 'diy'],
               'category': 'hive-181335'}  # Threespeak: generic, no signal
        meta = normalize_video_metadata(doc)
        evidence = tags_from_hive_tags(meta['hive_tags']) | tags_from_category(meta['category'])
        self.assertEqual(evidence, {'art', 'tutorial'})
        self.assertFalse(evidence & {'health', 'travel', 'technology'})


class TestApplyImplications(unittest.TestCase):
    def test_tutorial_implies_education(self):
        self.assertEqual(apply_implications(['tutorial']), ['tutorial', 'education'])

    def test_implied_tag_is_inserted_after_its_trigger(self):
        # 'art' came first (higher score), so it must stay first.
        self.assertEqual(apply_implications(['art', 'tutorial']),
                         ['art', 'tutorial', 'education'])

    def test_no_duplicate_when_education_already_present(self):
        self.assertEqual(apply_implications(['tutorial', 'education']),
                         ['tutorial', 'education'])
        self.assertEqual(apply_implications(['education', 'tutorial']),
                         ['education', 'tutorial'])

    def test_untriggered_tags_pass_through_unchanged(self):
        self.assertEqual(apply_implications(['gaming']), ['gaming'])
        self.assertEqual(apply_implications([]), [])

    def test_deduplicates_input(self):
        self.assertEqual(apply_implications(['gaming', 'gaming']), ['gaming'])

    def test_implication_targets_are_in_taxonomy(self):
        for trigger, implied in TAG_IMPLICATIONS.items():
            self.assertIn(trigger, TAXONOMY)
            self.assertTrue(implied <= TAXONOMY, f"{trigger} -> {implied - TAXONOMY}")

    def test_craft_video_now_yields_art_tutorial_education(self):
        """dubisortiz: the reporter expected art, education and tutorial."""
        evidence = tags_from_hive_tags(['3speak', 'yeso', 'diy'])
        tags = apply_implications(sorted(evidence))
        self.assertEqual(set(tags), {'art', 'tutorial', 'education'})


class TestSrtToText(unittest.TestCase):
    SRT = (
        "1\n00:00:00,000 --> 00:00:02,000\nHello everyone\n\n"
        "2\n00:00:02,000 --> 00:00:04,000\nwelcome to my cooking channel\n\n"
        "3\n00:00:04,000 --> 00:00:06,000\nwelcome to my cooking channel\n\n"  # dup
        "4\n00:00:06,000 --> 00:00:08,000\ntoday we make bread\n"
    )

    def test_strips_indices_and_timestamps(self):
        out = srt_to_text(self.SRT)
        self.assertNotIn('-->', out)
        self.assertNotIn('00:00', out)
        self.assertFalse(any(tok.isdigit() for tok in out.split()))

    def test_dedupes_consecutive_repeats(self):
        # The duplicated caption appears once, not twice.
        self.assertEqual(srt_to_text(self.SRT).count('welcome to my cooking channel'), 1)

    def test_produces_clean_prose(self):
        self.assertEqual(
            srt_to_text(self.SRT),
            'Hello everyone welcome to my cooking channel today we make bread')

    def test_empty_and_caps_length(self):
        self.assertEqual(srt_to_text(''), '')
        self.assertEqual(srt_to_text('1\n00:00:00,000 --> 00:00:01,000\n' + 'x ' * 5000,
                                     max_chars=100).__len__(), 100)

    def test_word_count(self):
        self.assertEqual(word_count('one two three'), 3)
        self.assertEqual(word_count(''), 0)
        self.assertEqual(word_count(None), 0)


class TestExclusiveCommunities(unittest.TestCase):
    """Single-topic communities decide the tags outright — no classifier."""

    def test_music_communities(self):
        from tag_taxonomy import exclusive_tags_for
        for cat in ('hive-193816', 'Music',                      # Music
                    'hive-120026', 'Music Zone', 'music zone',   # Music Zone
                    'hive-105786', 'Hive Open Mic',              # Hive Open Mic
                    'hive-195772', 'AFRI-TUNES',                 # AFRI-TUNES
                    'hive-148115', 'Sound Music',                # Sound Music
                    'hive-141487', 'Hive Music',                 # Hive Music
                    'hive-168505', 'HiveMusic',                  # HiveMusic
                    'hive-175836',                               # Musicforlife (emoji name)
                    'hive-129959', 'DSound',                     # DSound
                    'BlockTunes', 'Electronic Music'):
            self.assertEqual(exclusive_tags_for(cat), ['music'], cat)

    def test_music_adjacent_communities_are_NOT_exclusive(self):
        """
        Only purely-music communities short-circuit the classifier. A gaming
        music channel is not simply 'music', so these must still be classified.
        """
        from tag_taxonomy import exclusive_tags_for
        for cat in ('Gaming Music (CC-BY)',
                    'hive-111482', 'Music Technology',
                    'hive-118409', 'Dance and music',
                    'hive-192806', 'Q Inspired-by-Music',
                    'Hive Music School', 'Music Theory'):
            self.assertEqual(exclusive_tags_for(cat), [], cat)

    def test_skatehive_is_sports(self):
        from tag_taxonomy import exclusive_tags_for
        self.assertEqual(exclusive_tags_for('hive-173115'), ['sports'])
        self.assertEqual(exclusive_tags_for('SkateHive'), ['sports'])

    def test_clean_planet_is_nature(self):
        from tag_taxonomy import exclusive_tags_for
        self.assertEqual(exclusive_tags_for('hive-150210'), ['nature'])
        self.assertEqual(exclusive_tags_for('CLEAN PLANET'), ['nature'])

    def test_homesteading_and_garden_communities_are_nature(self):
        from tag_taxonomy import exclusive_tags_for
        for cat in ('hive-114308', 'Homesteading',      # Homesteading
                    'hive-140635', 'HiveGarden',        # HiveGarden
                    'hive-147663', 'Hive Gardening'):   # Hive Gardening
            self.assertEqual(exclusive_tags_for(cat), ['nature'], cat)

    def test_lookalike_planet_communities_are_not_matched(self):
        """Matching is exact, not substring — these are unrelated communities."""
        from tag_taxonomy import exclusive_tags_for
        for cat in ('Planetauto', 'PlanetTammie', 'Flat Earth',
                    'BioHack the Planet!', 'Environment'):
            self.assertEqual(exclusive_tags_for(cat), [], cat)

    def test_diyhub_is_tutorial_plus_implied_education(self):
        from tag_taxonomy import exclusive_tags_for
        # 'only tutorial', but the tutorial->education implication still applies,
        # so DIYHub matches how a tutorial is tagged anywhere else.
        self.assertEqual(exclusive_tags_for('hive-189641'), ['tutorial', 'education'])
        self.assertEqual(exclusive_tags_for('DIYHub'), ['tutorial', 'education'])

    def test_matching_is_case_insensitive(self):
        from tag_taxonomy import exclusive_tags_for
        # The DB stores the display name with its original capitalisation.
        self.assertEqual(exclusive_tags_for('SKATEHIVE'), ['sports'])
        self.assertEqual(exclusive_tags_for('  diyhub  '), ['tutorial', 'education'])

    def test_reviewed_community_mappings(self):
        """The full mapping tibfox reviewed in COMMUNITY_TAG_MAPPING.md."""
        from tag_taxonomy import exclusive_tags_for
        cases = {
            'hive-140217': ['gaming'],        # Hive Gaming
            'hive-13323': ['gaming'],         # Splinterlands
            'hive-185676': ['gaming'],        # Gaming Photography
            'hive-195370': ['gaming'],        # Rising Star Game
            'hive-189157': ['sports'],        # Full Deportes
            'hive-101690': ['sports'],        # Sports Talk Social
            'hive-155221': ['vlog'],          # We Are Alive Tribe
            'hive-145796': ['vlog'],          # EspaVlog
            'hive-11800': ['vlog'],           # Hive Sucre
            'hive-187189': ['vlog'],          # Lifestyle
            'hive-140169': ['music'],         # Vibes
            'hive-100067': ['food'],          # Hive Food
            'hive-158694': ['art'],           # Alien Art Hive
            'hive-128780': ['science'],       # MES Science
            'hive-109255': ['news'],          # News & Views
            'hive-163772': ['travel'],        # Worldmappin
            'hive-167922': ['finance'],       # LeoFinance
            'hive-106130': ['cryptocurrency'],  # SpendHBD
        }
        for cid, want in cases.items():
            self.assertEqual(exclusive_tags_for(cid), want, cid)

    def test_hive_diy_gets_implied_education(self):
        from tag_taxonomy import exclusive_tags_for
        self.assertEqual(exclusive_tags_for('hive-130560'), ['tutorial', 'education'])

    def test_plain_category_values(self):
        """Raw category strings on older videos, not Hive communities."""
        from tag_taxonomy import exclusive_tags_for
        self.assertEqual(exclusive_tags_for('crypto'), ['cryptocurrency'])
        self.assertEqual(exclusive_tags_for('politics'), ['news'])
        self.assertEqual(exclusive_tags_for('documentary'), ['news'])
        self.assertEqual(exclusive_tags_for('gaming'), ['gaming'])
        self.assertEqual(exclusive_tags_for('vlog'), ['vlog'])
        self.assertEqual(exclusive_tags_for('art'), ['art'])

    def test_reviewed_rejections_stay_unmapped(self):
        """Communities explicitly rejected during review must NOT be exclusive."""
        from tag_taxonomy import exclusive_tags_for
        for cat in ('hive-106817', 'Geek Zone',       # rejected
                    'hive-122315', 'Deep Dives',      # rejected
                    'hive-153850', 'Hive Learners',   # rejected: too broad a topic mix
                    'general',                        # default category
                    'hive-111000', 'Threespeak',      # platform
                    'hive-166847', 'Movies & TV Shows',  # no tag fits
                    'hive-165757', 'Motherhood'):        # no tag fits
            self.assertEqual(exclusive_tags_for(cat), [], cat)

    def test_non_exclusive_community_yields_nothing(self):
        from tag_taxonomy import exclusive_tags_for
        self.assertEqual(exclusive_tags_for('hive-125125'), [])   # Ecency
        self.assertEqual(exclusive_tags_for('hive-181335'), [])   # Threespeak
        self.assertEqual(exclusive_tags_for(''), [])
        self.assertEqual(exclusive_tags_for(None), [])

    def test_exclusive_targets_are_valid_taxonomy_tags(self):
        from tag_taxonomy import EXCLUSIVE_COMMUNITY_MAP, exclusive_tags_for
        for key in EXCLUSIVE_COMMUNITY_MAP:
            self.assertTrue(set(exclusive_tags_for(key)) <= TAXONOMY, key)


class TestConfigConsistency(unittest.TestCase):
    """config.yaml and the taxonomy module must not drift apart."""

    @classmethod
    def setUpClass(cls):
        import yaml
        path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
        with open(path) as f:
            cls.config = yaml.safe_load(f)

    def test_config_tags_match_taxonomy(self):
        self.assertEqual(set(self.config['tags']), TAXONOMY)

    def test_every_label_has_an_explicit_threshold(self):
        """
        A label omitted from label_thresholds falls back to min_confidence,
        which is far too permissive for the attractor labels ('news', 'travel').
        Listing every label makes that failure mode impossible.
        """
        thresholds = self.config['tagging']['label_thresholds']
        self.assertEqual(set(thresholds), TAXONOMY,
                         f"labels without a threshold: {TAXONOMY - set(thresholds)}")

    def test_attractor_labels_are_held_to_a_high_bar(self):
        thresholds = self.config['tagging']['label_thresholds']
        for attractor in ('news', 'travel', 'health', 'technology'):
            self.assertGreaterEqual(thresholds[attractor], 0.9, attractor)

    def test_taxonomy_values_of_maps_are_valid(self):
        from tag_taxonomy import COMMUNITY_MAP, HIVE_TAG_MAP
        for src, targets in HIVE_TAG_MAP.items():
            self.assertTrue(targets <= TAXONOMY, f"{src} -> {targets - TAXONOMY}")
        for cid, targets in COMMUNITY_MAP.items():
            self.assertTrue(targets <= TAXONOMY, f"{cid} -> {targets - TAXONOMY}")

    def test_no_stopword_is_also_a_mapped_tag(self):
        from tag_taxonomy import HIVE_TAG_MAP, HIVE_TAG_STOPWORDS
        overlap = HIVE_TAG_STOPWORDS & set(HIVE_TAG_MAP)
        self.assertEqual(overlap, set(), f"tags both dropped and mapped: {overlap}")


if __name__ == '__main__':
    unittest.main()
