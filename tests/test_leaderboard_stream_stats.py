"""
Unit tests for the livestream / boost half of the leaderboard rollup. No Mongo
required — collect() is driven through a fake DB that hands back canned docs.

    python3 -m unittest discover -s tests -v
"""

import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from leaderboard_stats import (  # noqa: E402
    MAX_METRICS,
    METRICS,
    _as_float,
    _as_int,
    agg_op,
    collect,
    norm_user,
)

D1 = datetime(2026, 7, 24, 6, 0)
D2 = datetime(2026, 7, 25, 6, 0)
DAY1 = datetime(2026, 7, 24)
DAY2 = datetime(2026, 7, 25)


class _FakeCollection:
    """find() ignores the filter — tests drive collect() in --full mode, where
    the stream/boost filters are `{}` anyway."""

    def __init__(self, docs):
        self._docs = docs

    def find(self, query=None, projection=None):
        return iter(list(self._docs))


class FakeDB:
    def __init__(self, data):
        self._data = data

    def __getitem__(self, name):
        return _FakeCollection(self._data.get(name, []))


def collect_streams(streams=(), boosts=()):
    """Run collect() over stream data alone and return the {(user, day): row} map."""
    db = FakeDB({'stream-stats': list(streams), 'stream-boosts': list(boosts)})
    stats, _topics, _topics_v2 = collect(db, None)
    return stats


class TestCoercion(unittest.TestCase):
    def test_as_int_rejects_null_and_junk(self):
        for bad in (None, '', 'abc', {}, []):
            self.assertEqual(_as_int(bad), 0)

    def test_as_int_floors_negatives_to_zero(self):
        self.assertEqual(_as_int(-5), 0)
        self.assertEqual(_as_int(12), 12)

    def test_as_float_coalesces_null_amount(self):
        self.assertEqual(_as_float(None), 0.0)
        self.assertEqual(_as_float('2.5'), 2.5)
        self.assertEqual(_as_float(-1), 0.0)

    def test_norm_user_folds_case_and_blanks(self):
        self.assertEqual(norm_user('  ButtCoins '), 'buttcoins')
        self.assertIsNone(norm_user('   '))
        self.assertIsNone(norm_user(None))


class TestRollupOperators(unittest.TestCase):
    def test_peak_viewers_rolls_up_as_max(self):
        # Summing a daily high-water mark across days would invent concurrency.
        self.assertEqual(agg_op('stream_peak_viewers'), '$max')

    def test_everything_else_sums(self):
        for m in METRICS:
            if m not in MAX_METRICS:
                self.assertEqual(agg_op(m), '$sum', m)

    def test_max_metrics_are_real_metrics(self):
        self.assertTrue(MAX_METRICS.issubset(set(METRICS)))


class TestStreamStats(unittest.TestCase):
    def test_two_streams_one_day_max_peak_but_summed_duration(self):
        stats = collect_streams([
            {'_id': 's1', 'host': 'buttcoins', 'startedAt': D1, 'endedAt': D1,
             'durationSec': 5400, 'peakViewers': 12, 'totalViewers': 3},
            {'_id': 's2', 'host': 'buttcoins', 'startedAt': D1, 'endedAt': D1,
             'durationSec': 600, 'peakViewers': 9, 'totalViewers': 2},
        ])
        row = stats[('buttcoins', DAY1)]
        self.assertEqual(row['streams'], 2)
        self.assertEqual(row['stream_secs'], 6000)
        self.assertEqual(row['stream_peak_viewers'], 12)   # max, not 21
        self.assertEqual(row['stream_viewers'], 5)         # join volume, summed

    def test_live_stream_counts_viewers_but_not_duration(self):
        stats = collect_streams([
            {'_id': 's1', 'host': 'alice', 'startedAt': D2,
             'peakViewers': 7, 'totalViewers': 2},
        ])
        row = stats[('alice', DAY2)]
        self.assertEqual(row['streams'], 1)
        self.assertEqual(row['stream_secs'], 0)
        self.assertEqual(row['stream_peak_viewers'], 7)

    def test_absurd_duration_is_skipped_but_stream_still_counts(self):
        stats = collect_streams([
            {'_id': 's1', 'host': 'alice', 'startedAt': D2, 'endedAt': D2,
             'durationSec': 99 * 3600, 'peakViewers': 1, 'totalViewers': 1},
        ])
        row = stats[('alice', DAY2)]
        self.assertEqual(row['streams'], 1)
        self.assertEqual(row['stream_secs'], 0)

    def test_bucketed_on_start_day_not_end_day(self):
        # An overnight stream lands whole on the day it started.
        stats = collect_streams([
            {'_id': 's1', 'host': 'alice', 'startedAt': datetime(2026, 7, 24, 23, 0),
             'endedAt': datetime(2026, 7, 25, 1, 0), 'durationSec': 7200},
        ])
        self.assertEqual(stats[('alice', DAY1)]['stream_secs'], 7200)
        self.assertNotIn(('alice', DAY2), stats)

    def test_mixed_case_host_folds_into_one_user(self):
        stats = collect_streams([
            {'_id': 's1', 'host': 'Alice', 'startedAt': D2, 'endedAt': D2,
             'durationSec': 60},
            {'_id': 's2', 'host': 'alice', 'startedAt': D2, 'endedAt': D2,
             'durationSec': 40},
        ])
        self.assertEqual(stats[('alice', DAY2)]['stream_secs'], 100)

    def test_stream_without_host_or_start_is_ignored(self):
        stats = collect_streams([
            {'_id': 's1', 'host': None, 'startedAt': D2, 'durationSec': 60},
            {'_id': 's2', 'host': 'alice', 'startedAt': None, 'durationSec': 60},
        ])
        self.assertEqual(dict(stats), {})


class TestBoosts(unittest.TestCase):
    STREAMS = [{'_id': 's1', 'host': 'buttcoins', 'startedAt': D1, 'endedAt': D1,
                'durationSec': 10}]

    def test_credits_receiver_and_sender(self):
        stats = collect_streams(self.STREAMS, [
            {'streamId': 's1', 'host': 'buttcoins', 'sender': 'alice',
             'amount': 5, 'createdAt': D1},
        ])
        self.assertEqual(stats[('buttcoins', DAY1)]['boosts_received'], 1)
        self.assertEqual(stats[('buttcoins', DAY1)]['boost_amount_received'], 5)
        self.assertEqual(stats[('alice', DAY1)]['boosts_given'], 1)
        self.assertEqual(stats[('alice', DAY1)]['boost_amount_given'], 5)

    def test_null_amount_still_counts_as_a_boost(self):
        stats = collect_streams(self.STREAMS, [
            {'streamId': 's1', 'host': 'buttcoins', 'sender': 'bob',
             'amount': None, 'createdAt': D1},
        ])
        self.assertEqual(stats[('buttcoins', DAY1)]['boosts_received'], 1)
        self.assertEqual(stats[('buttcoins', DAY1)]['boost_amount_received'], 0)

    def test_null_host_resolves_through_stream_id(self):
        stats = collect_streams(self.STREAMS, [
            {'streamId': 's1', 'host': None, 'sender': 'bob',
             'amount': 2, 'createdAt': D1},
        ])
        self.assertEqual(stats[('buttcoins', DAY1)]['boosts_received'], 1)

    def test_self_boost_is_excluded_from_both_sides(self):
        stats = collect_streams(self.STREAMS, [
            {'streamId': 's1', 'host': 'buttcoins', 'sender': 'buttcoins',
             'amount': 99, 'createdAt': D1},
        ])
        row = stats[('buttcoins', DAY1)]
        self.assertEqual(row['boosts_received'], 0)
        self.assertEqual(row['boosts_given'], 0)
        self.assertEqual(row['boost_amount_received'], 0)

    def test_unresolvable_host_still_credits_the_sender(self):
        stats = collect_streams(self.STREAMS, [
            {'streamId': 'ghost', 'host': None, 'sender': 'carol',
             'amount': 3, 'createdAt': D1},
        ])
        self.assertEqual(stats[('carol', DAY1)]['boosts_given'], 1)

    def test_boost_buckets_on_its_own_day_not_the_streams(self):
        stats = collect_streams(self.STREAMS, [
            {'streamId': 's1', 'host': 'buttcoins', 'sender': 'alice',
             'amount': 1, 'createdAt': D2},
        ])
        self.assertEqual(stats[('buttcoins', DAY2)]['boosts_received'], 1)
        self.assertEqual(stats[('buttcoins', DAY1)]['boosts_received'], 0)


if __name__ == '__main__':
    unittest.main()
