"""
Unit tests for the story columns of the leaderboard rollup. No Mongo required.

    python3 -m unittest discover -s tests -v
"""

import os
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.dirname(__file__))

from leaderboard_stats import METRICS, collect, longest_streak  # noqa: E402
from test_leaderboard_stream_stats import FakeDB  # noqa: E402

DAY1 = datetime(2026, 10, 25)
DAY2 = datetime(2026, 10, 26)


def collect_stories(videos):
    """Run collect() over embed-video rows alone and return the {(user, day): row} map."""
    stats, _topics, _topics_v2 = collect(FakeDB({'embed-video': list(videos)}), None)
    return stats


def story(owner, posted_at, **extra):
    return {'owner': owner, 'permlink': f'p{posted_at.timestamp()}', 'status': 'published',
            'short': True, 'createdAt': posted_at, 'story_posted_at': posted_at, **extra}


class TestLongestStreak(unittest.TestCase):
    def days(self, *offsets):
        return [DAY1 + timedelta(days=o) for o in offsets]

    def test_empty_is_zero(self):
        self.assertEqual(longest_streak([]), 0)

    def test_single_day(self):
        self.assertEqual(longest_streak(self.days(0)), 1)

    def test_gap_breaks_the_run(self):
        self.assertEqual(longest_streak(self.days(0, 1, 2, 4, 5)), 3)

    def test_longest_run_wins_wherever_it_sits(self):
        self.assertEqual(longest_streak(self.days(0, 2, 3, 4, 5, 7)), 4)

    def test_order_and_duplicates_do_not_matter(self):
        self.assertEqual(longest_streak(self.days(2, 0, 1, 1, 0)), 3)


class TestStoryUploads(unittest.TestCase):
    def test_story_is_metric_on_board(self):
        self.assertIn('story_uploads', METRICS)

    def test_buckets_on_posted_day_not_created_day(self):
        stats = collect_stories([story('alice', DAY2 + timedelta(hours=3),
                                       createdAt=DAY1 + timedelta(hours=20))])
        self.assertEqual(stats[('alice', DAY2)]['story_uploads'], 1)
        self.assertEqual(stats[('alice', DAY1)]['story_uploads'], 0)

    def test_kept_as_short_still_counts(self):
        stats = collect_stories([story('alice', DAY1, story=False)])
        self.assertEqual(stats[('alice', DAY1)]['story_uploads'], 1)

    def test_story_also_counts_as_a_short(self):
        stats = collect_stories([story('alice', DAY1)])
        self.assertEqual(stats[('alice', DAY1)]['short_uploads'], 1)

    def test_several_stories_one_day(self):
        stats = collect_stories([story('alice', DAY1 + timedelta(hours=h)) for h in (1, 5, 9)])
        self.assertEqual(stats[('alice', DAY1)]['story_uploads'], 3)


if __name__ == '__main__':
    unittest.main()
