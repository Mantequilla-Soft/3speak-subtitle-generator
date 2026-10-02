"""
Leaderboard stats aggregator.

Computes, per user per UTC day:

    video_uploads          videos published to Hive (excluding shorts)
    short_uploads          shorts / reels published to Hive
    video_watch_secs       seconds their videos were watched
    short_watch_secs       seconds their shorts were watched
    tags_given             viewer-tags they added to OTHER creators' videos
    streams                stream sessions they started
    stream_secs            seconds they spent live (ended streams only)
    stream_peak_viewers    best concurrent-viewer count — a MAX, not a sum
    stream_viewers         viewer-join events across their streams (join volume,
                           NOT unique reach — the same person rejoining counts twice)
    boosts_received        boosts sent TO them on their streams
    boost_amount_received  summed amount of those boosts
    boosts_given           boosts they sent to OTHER streamers
    boost_amount_given     summed amount of those

Written to two collections, both owned by this project (we never write to
3Speak's own collections):

    leaderboard-daily   one doc per (user, day) — the incremental source of truth
    leaderboard         one doc per (window, user) — totals for 7d/30d/365d/all,
                        so the UI can sort by any metric without re-aggregating

Daily buckets mean any time window is just a sum over a date range: recomputing
a day is idempotent — it is upserted AND emptied rows are pruned, so a count that
drops to zero actually drops — and an hourly run only has to redo the last days.

IMPORTANT — watch-time coverage: `view-durations` (the only source of watched
seconds) only starts 2026-07-06. The 7.2M-row `views` collection records view
events with NO duration, so it cannot supply historical watch time. Watch-time
columns for the 30d/365d/all windows are therefore only as deep as that data
goes, and will fill in naturally over the coming months.

Same caveat, harder, for the stream columns: `stream-stats` / `stream-boosts` are
written by the 3speakchecks /stream-stats/* endpoints, which nothing calls yet.
Both collections are empty, so every stream metric reads 0 until that service is
wired up. The columns are computed and indexed regardless, so they populate on
the next hourly run once data starts arriving — no code change needed. The old
`livestreams` / `liveviews` collections are the previous channel/streamkey
system and are deliberately NOT read here.

    python3 src/leaderboard_stats.py --full          # rebuild everything
    python3 src/leaderboard_stats.py --days 7        # refresh recent days (hourly)
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import yaml
from pymongo import ASCENDING, DESCENDING, MongoClient, UpdateOne

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

METRICS = ('video_uploads', 'short_uploads',
           'video_watch_secs', 'short_watch_secs', 'tags_given',
           'streams', 'stream_secs', 'stream_peak_viewers', 'stream_viewers',
           'boosts_received', 'boost_amount_received',
           'boosts_given', 'boost_amount_given')

# Metrics that roll up with $max instead of $sum. `peakViewers` is a
# high-water mark: a creator who peaked at 12 on Monday and 9 on Tuesday peaked
# at 12, not 21. Summing it would invent concurrency that never happened.
MAX_METRICS = frozenset({'stream_peak_viewers'})


def agg_op(metric):
    """The $group operator that rolls this metric up across days."""
    return '$max' if metric in MAX_METRICS else '$sum'

# Per-topic board: how much a creator published in a topic, and how long it was
# watched. Topics are the auto-generated tags (see config.yaml `tags`).
# Videos and shorts are tracked separately — shorts get topics too, and a shorts
# creator shouldn't be buried under a long-form one. `uploads` / `watch_secs` are
# the combined totals, kept alongside so the UI can rank either way.
TOPIC_METRICS = ('video_uploads', 'short_uploads', 'uploads',
                 'video_watch_secs', 'short_watch_secs', 'watch_secs')

# window key -> lookback in days (None = all time)
WINDOWS = {'7d': 7, '30d': 30, '365d': 365, 'all': None}

# A single view-duration longer than this is treated as bad data (idle tab,
# clock skew) rather than real watch time.
MAX_SANE_WATCH_SECS = 6 * 3600

# Same idea for a single stream. Generous on purpose — 24/7 rebroadcast channels
# are a real thing, and this only exists to catch a durationSec computed against
# a bogus startedAt. Over-long streams are skipped and counted, not clamped, so
# the log says so rather than quietly inventing a plausible number.
MAX_SANE_STREAM_SECS = 48 * 3600

# An upload only counts once it actually reached the chain.
#
# `status: 'published'` on an embed doc means the ENCODER finished, not that a
# Hive post exists: a bulk upload leaves one doc per file at encodingProgress
# 100 with hive_permlink/hive_title null and 0 views, even if the creator only
# ever posted one of them. Counting those put steemseph at #1 on the 7d board
# with 16 uploads when 15 were orphans. `hive_permlink` is the signal the tagger
# already uses to skip orphans (see tag_videos_v2.py).
#
# Applied only from this date on: 3Speak did not populate hive_permlink before
# ~Feb 2026 (zero coverage Nov 2025 - Jan 2026, ~30% through May), so gating all
# history would erase real pre-June uploads from the 365d/all boards. Older days
# keep counting as they always have.
HIVE_GATE_FROM = datetime(2026, 6, 1)


def utcnow():
    """
    Naive UTC 'now'. datetime.utcnow() is deprecated, but the day buckets we
    store are naive, so a tz-aware value here would not compare against them.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _new_row():
    return {m: 0 for m in METRICS}


def _new_topic_row():
    return {m: 0 for m in TOPIC_METRICS}


def load_tag_maps(db):
    """
    Two maps of (author, permlink) -> [topics]: v1 and v2.

    Both are read in a single pass over subtitles-tags. v1 (`tags_list`) is what
    the production frontend reads today; v2 (`tags_list_v2`) is the faceted
    taxonomy (see TAXONOMY_V2.md) and is published to parallel `*-v2`
    collections so the frontend can switch over without a flag day.

    Note the tag doc's own created_at is when WE tagged it, not when the video
    was published, so topic stats are bucketed by the video's upload date
    instead (see collect).
    """
    v1, v2 = {}, {}
    for d in db['subtitles-tags'].find(
            {}, {'author': 1, 'permlink': 1, 'tags_list': 1, 'tags': 1,
                 'tags_list_v2': 1, '_id': 0}):
        key = (d.get('author'), d.get('permlink'))
        topics = d.get('tags_list')
        if not topics:
            # A handful of rows predate tags_list and only have the comma string.
            topics = [t for t in (d.get('tags') or '').split(',') if t]
        if topics:
            v1[key] = topics
        topics_v2 = d.get('tags_list_v2')
        if topics_v2:
            v2[key] = topics_v2
    return v1, v2


def day_of(dt):
    """UTC midnight for the day containing dt, or None if unusable."""
    if not isinstance(dt, datetime):
        return None
    return datetime(dt.year, dt.month, dt.day)


def load_short_keys(db):
    """(owner, permlink) of every short/reel. Small — ~7k — so a set is fine."""
    keys = set()
    for v in db['embed-video'].find({'short': True}, {'owner': 1, 'permlink': 1, '_id': 0}):
        keys.add((v.get('owner'), v.get('permlink')))
    for v in db['videos'].find({'isReel': True}, {'owner': 1, 'permlink': 1, '_id': 0}):
        keys.add((v.get('owner'), v.get('permlink')))
    return keys


def _as_int(value):
    """Non-negative int, or 0 for null / junk. Stream counters are caller-supplied."""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return 0
    return n if n > 0 else 0


def _as_float(value):
    """
    Non-negative float, or 0.0 for null / junk. Boost `amount` is explicitly
    nullable and its unit is whatever the caller sent, so this only guards the
    type — it does not try to normalise currencies.
    """
    try:
        n = float(value)
    except (TypeError, ValueError):
        return 0.0
    return n if n > 0 else 0.0


def norm_user(name):
    """
    Hive usernames are lowercase. The stream writer already lowercases, but a
    single mixed-case value would split a creator into two board rows, so
    normalise rather than trust.
    """
    return name.strip().lower() if isinstance(name, str) and name.strip() else None


def load_stream_hosts(db):
    """
    streamId -> host, for attributing boosts whose own `host` is null.

    One doc per stream session, so this stays small. Loaded unfiltered even on
    an incremental run: a boost landing today can belong to a stream that
    started before the window.
    """
    hosts = {}
    for s in db['stream-stats'].find({}, {'host': 1}):
        host = norm_user(s.get('host'))
        if host:
            hosts[s['_id']] = host
    return hosts


def collect(db, since):
    """
    Build {(user, day): {metric: value}} for everything at/after `since`
    (None = all history).
    """
    stats = defaultdict(_new_row)
    topic_stats = defaultdict(_new_topic_row)
    topic_stats_v2 = defaultdict(_new_topic_row)
    short_keys = load_short_keys(db)
    tag_map, tag_map_v2 = load_tag_maps(db)
    logger.info(f"{len(short_keys)} shorts/reels known, "
                f"{len(tag_map)} videos carry v1 topic tags, "
                f"{len(tag_map_v2)} carry v2 topic tags")

    def bump(user, day, metric, amount=1):
        if user and day and amount:
            stats[(user, day)][metric] += amount

    def bump_max(user, day, metric, value):
        """For high-water metrics (see MAX_METRICS) — keep the best, don't add."""
        if user and day and value:
            row = stats[(user, day)]
            row[metric] = max(row[metric], value)

    def bump_topic(user, permlink, day, kind, is_short, amount=1):
        """
        Credit every topic on this video.

        `kind` is 'uploads' or 'watch_secs'. Each bump lands in the short/video
        variant AND the combined total, so the board can be ranked either way.
        """
        if not (user and day and amount):
            return
        prefix = 'short' if is_short else 'video'
        # Same pass credits both taxonomies — v2 costs one extra dict lookup,
        # not another scan of videos/views.
        for tmap, tstats in ((tag_map, topic_stats), (tag_map_v2, topic_stats_v2)):
            for topic in tmap.get((user, permlink), ()):
                row = tstats[(user, topic, day)]
                row[f'{prefix}_{kind}'] += amount
                row[kind] += amount

    # --- 1 & 2: uploads -----------------------------------------------------
    # Both queries drop encoded-but-never-posted uploads (see HIVE_GATE_FROM).
    # Videos and shorts share these queries, so both boards get the same gate —
    # shorts orphan at the same rate as long-form.
    embed_q = {
        'status': 'published',
        '$or': [
            {'createdAt': {'$lt': HIVE_GATE_FROM}},   # pre-gate: no usable signal
            {'hive_permlink': {'$ne': None}},         # $ne None also drops missing
        ],
    }
    # Legacy docs have no hive_permlink at all (0 of 137k) — their on-chain
    # signal is steemPosted/publishFailed, and publishFailed marks exactly the
    # same failure: encoded, never made it to the chain.
    legacy_q = {'status': 'published', 'publishFailed': {'$ne': True}}
    if since:
        embed_q['createdAt'] = {'$gte': since}
        legacy_q['created'] = {'$gte': since}

    n = 0
    for v in db['embed-video'].find(
            embed_q, {'owner': 1, 'permlink': 1, 'createdAt': 1, 'short': 1, '_id': 0}):
        is_short = v.get('short') is True
        day = day_of(v.get('createdAt'))
        bump(v.get('owner'), day, 'short_uploads' if is_short else 'video_uploads')
        bump_topic(v.get('owner'), v.get('permlink'), day, 'uploads', is_short)
        n += 1
    logger.info(f"embed-video uploads scanned: {n}")

    n = 0
    for v in db['videos'].find(
            legacy_q, {'owner': 1, 'permlink': 1, 'created': 1, 'isReel': 1, '_id': 0}):
        is_short = v.get('isReel') is True
        day = day_of(v.get('created'))
        bump(v.get('owner'), day, 'short_uploads' if is_short else 'video_uploads')
        bump_topic(v.get('owner'), v.get('permlink'), day, 'uploads', is_short)
        n += 1
    logger.info(f"legacy uploads scanned: {n}")

    # --- 3 & 4: watched seconds, credited to the video's OWNER ---------------
    dur_q = {}
    if since:
        dur_q['startedAt'] = {'$gte': since}

    n = skipped = 0
    for d in db['view-durations'].find(
            dur_q, {'owner': 1, 'permlink': 1, 'watchedSeconds': 1,
                    'startedAt': 1, '_id': 0}):
        secs = d.get('watchedSeconds') or 0
        try:
            secs = int(secs)
        except (TypeError, ValueError):
            continue
        if secs <= 0 or secs > MAX_SANE_WATCH_SECS:
            skipped += 1
            continue
        owner, permlink = d.get('owner'), d.get('permlink')
        is_short = (owner, permlink) in short_keys
        metric = 'short_watch_secs' if is_short else 'video_watch_secs'
        # Watch time buckets on the day it was WATCHED (uploads bucket on the day
        # they were published) — same convention as the main board.
        watch_day = day_of(d.get('startedAt'))
        bump(owner, watch_day, metric, secs)
        bump_topic(owner, permlink, watch_day, 'watch_secs', is_short, secs)
        n += 1
    logger.info(f"view-durations scanned: {n} (skipped {skipped} implausible)")

    # --- 5: viewer-tags given to OTHER creators -----------------------------
    # Credited to the `voter` (who added the tag), not the video's author.
    tag_q = {'$expr': {'$ne': ['$voter', '$author']}}
    if since:
        tag_q['createdAt'] = {'$gte': since}

    n = 0
    for t in db['viewer-tags'].find(
            tag_q, {'voter': 1, 'createdAt': 1, '_id': 0}):
        bump(t.get('voter'), day_of(t.get('createdAt')), 'tags_given')
        n += 1
    logger.info(f"viewer-tags (for other creators) scanned: {n}")

    # --- 6: livestreams -----------------------------------------------------
    # Bucketed on the day the stream STARTED, so an overnight stream lands whole
    # on its start day rather than being split. That also keeps an incremental
    # run safe: every row it emits is inside the window it is about to prune.
    #
    # The trade-off is that a stream which started before the window and ended
    # inside it never gets its durationSec picked up by a --days run, because
    # its bucket day is already out of range. The deployed timer runs --full
    # hourly, which recomputes that day anyway, so this self-heals.
    stream_q = {}
    if since:
        stream_q['startedAt'] = {'$gte': since}

    n = live = insane = 0
    for s in db['stream-stats'].find(
            stream_q, {'host': 1, 'startedAt': 1, 'endedAt': 1, 'durationSec': 1,
                       'peakViewers': 1, 'totalViewers': 1}):
        host = norm_user(s.get('host'))
        day = day_of(s.get('startedAt'))
        if not (host and day):
            continue
        n += 1
        bump(host, day, 'streams')

        # Viewer counts are meaningful while the stream is still running; only
        # the duration has to wait for it to end.
        bump_max(host, day, 'stream_peak_viewers', _as_int(s.get('peakViewers')))
        bump(host, day, 'stream_viewers', _as_int(s.get('totalViewers')))

        if not s.get('endedAt'):
            live += 1
            continue
        secs = _as_int(s.get('durationSec'))
        if secs > MAX_SANE_STREAM_SECS:
            insane += 1
            continue
        bump(host, day, 'stream_secs', secs)
    logger.info(f"stream-stats scanned: {n} ({live} still live, no duration yet"
                f"{f', {insane} over {MAX_SANE_STREAM_SECS // 3600}h skipped' if insane else ''})")

    # --- 7: stream boosts ---------------------------------------------------
    # Credited both ways: to the host who received it and the sender who gave
    # it. Self-boosts are excluded from both — same rule as tags_given, and it
    # stops a streamer boosting their own stream to climb the board.
    boost_q = {}
    if since:
        boost_q['createdAt'] = {'$gte': since}

    n = selfboost = orphan = 0
    stream_hosts = load_stream_hosts(db)
    for b in db['stream-boosts'].find(
            boost_q, {'host': 1, 'sender': 1, 'amount': 1, 'streamId': 1,
                      'createdAt': 1, '_id': 0}):
        day = day_of(b.get('createdAt'))
        if not day:
            continue
        # `host` is nullable on the boost doc; fall back to the stream it points at.
        host = norm_user(b.get('host')) or stream_hosts.get(b.get('streamId'))
        sender = norm_user(b.get('sender'))
        if host and sender and host == sender:
            selfboost += 1
            continue
        if not host:
            orphan += 1
        amount = _as_float(b.get('amount'))   # nullable — a boost with no amount
                                              # still counts, it just adds 0
        n += 1
        if host:
            bump(host, day, 'boosts_received')
            bump(host, day, 'boost_amount_received', amount)
        if sender:
            bump(sender, day, 'boosts_given')
            bump(sender, day, 'boost_amount_given', amount)
    logger.info(f"stream-boosts scanned: {n}"
                f"{f', {selfboost} self-boosts skipped' if selfboost else ''}"
                f"{f', {orphan} with no resolvable host (sender still credited)' if orphan else ''}")

    logger.info(f"topic rows built: {len(topic_stats)}")

    return stats, topic_stats, topic_stats_v2


def write_daily(db, stats, since):
    """
    Upsert daily rows, then drop the ones that no longer have any data.

    The prune is what makes a recompute truly idempotent. An upsert alone only
    replaces days that still produce a row, so a (user, day) whose count drops to
    zero — a video deleted, or newly excluded by HIVE_GATE_FROM — would keep its
    old number forever. `since` scopes the prune to the days we just recomputed;
    None (a --full run) means every day is in scope.
    """
    daily = db['leaderboard-daily']
    if not stats:
        # Treated as a failed collect rather than a real empty day: pruning on
        # an empty result would wipe the collection.
        logger.info("no daily rows to write")
        return 0

    ops = [
        UpdateOne(
            {'user': user, 'date': date},
            {'$set': {'user': user, 'date': date, **row,
                      'updated_at': utcnow()}},
            upsert=True,
        )
        for (user, date), row in stats.items()
    ]
    for i in range(0, len(ops), 1000):
        daily.bulk_write(ops[i:i + 1000], ordered=False)

    # Compare on (user, date) tuples, not a joined string — a username could
    # contain whatever separator we picked.
    keep = set(stats)
    scope = {'date': {'$gte': since}} if since else {}
    stale = [d['_id'] for d in daily.find(scope, {'user': 1, 'date': 1})
             if (d.get('user'), d.get('date')) not in keep]
    for i in range(0, len(stale), 1000):
        daily.delete_many({'_id': {'$in': stale[i:i + 1000]}})
    logger.info(f"wrote {len(ops)} daily rows"
                f"{f', pruned {len(stale)} emptied' if stale else ''}")
    return len(ops)


def write_topic_daily(db, topic_stats, since, suffix=''):
    """
    Upsert (user, topic, day) rows, then drop the ones with nothing left.

    Same prune as write_daily, and for the same reason — a gated-out upload has
    to disappear from its topic rows too, not just the main board.

    `suffix` selects the taxonomy's collection set: '' = v1 (what production
    reads), '-v2' = the faceted taxonomy.
    """
    daily = db['leaderboard-topic-daily' + suffix]
    if not topic_stats:
        logger.info("no topic rows to write")
        return 0

    ops = [
        UpdateOne(
            {'user': user, 'topic': topic, 'date': date},
            {'$set': {'user': user, 'topic': topic, 'date': date, **row,
                      'updated_at': utcnow()}},
            upsert=True,
        )
        for (user, topic, date), row in topic_stats.items()
    ]
    for i in range(0, len(ops), 1000):
        daily.bulk_write(ops[i:i + 1000], ordered=False)

    keep = set(topic_stats)
    scope = {'date': {'$gte': since}} if since else {}
    stale = [d['_id'] for d in daily.find(scope, {'user': 1, 'topic': 1, 'date': 1})
             if (d.get('user'), d.get('topic'), d.get('date')) not in keep]
    for i in range(0, len(stale), 1000):
        daily.delete_many({'_id': {'$in': stale[i:i + 1000]}})
    logger.info(f"wrote {len(ops)} topic-daily rows{suffix}"
                f"{f', pruned {len(stale)} emptied' if stale else ''}")
    return len(ops)


def rebuild_topic_windows(db, now, suffix=''):
    """
    Recompute (window, topic, user) totals — the per-topic creator board.

    `suffix`: '' = v1 collections (production), '-v2' = faceted taxonomy.
    """
    daily = db['leaderboard-topic-daily' + suffix]
    board = db['leaderboard-topics' + suffix]

    for window, days in WINDOWS.items():
        match = {}
        start = None
        if days is not None:
            start = day_of(now) - timedelta(days=days - 1)
            match['date'] = {'$gte': start}

        pipeline = []
        if match:
            pipeline.append({'$match': match})
        pipeline.append({
            '$group': {
                '_id': {'topic': '$topic', 'user': '$user'},
                **{m: {'$sum': f'${m}'} for m in TOPIC_METRICS},
            }
        })

        ops = []
        live = []
        for r in daily.aggregate(pipeline):
            topic = r['_id'].get('topic')
            user = r['_id'].get('user')
            if not topic or not user:
                continue
            if not any(r.get(m) for m in TOPIC_METRICS):
                continue
            live.append((topic, user))
            ops.append(UpdateOne(
                {'window': window, 'topic': topic, 'user': user},
                {'$set': {'window': window, 'topic': topic, 'user': user,
                          **{m: r.get(m, 0) for m in TOPIC_METRICS},
                          'from': start, 'to': day_of(now),
                          'updated_at': utcnow()}},
                upsert=True,
            ))

        for i in range(0, len(ops), 1000):
            board.bulk_write(ops[i:i + 1000], ordered=False)

        # Drop (topic, user) pairs that aged out of this rolling window. Compare
        # on tuples rather than a joined string — a separator character is a bug
        # waiting to happen if a topic or username ever contains it.
        keep = set(live)
        stale = [d['_id'] for d in board.find({'window': window},
                                              {'topic': 1, 'user': 1})
                 if (d.get('topic'), d.get('user')) not in keep]
        if stale:
            board.delete_many({'_id': {'$in': stale}})
        logger.info(f"topics{suffix} {window:>5}: {len(ops)} (topic,creator) rows"
                    f"{f', pruned {len(stale)}' if stale else ''}")


def rebuild_windows(db, now):
    """Recompute the (window, user) totals from the daily rollups."""
    daily = db['leaderboard-daily']
    board = db['leaderboard']

    for window, days in WINDOWS.items():
        match = {}
        start = None
        if days is not None:
            start = day_of(now) - timedelta(days=days - 1)
            match['date'] = {'$gte': start}

        pipeline = []
        if match:
            pipeline.append({'$match': match})
        pipeline.append({
            '$group': {
                '_id': '$user',
                # Not all $sum — see MAX_METRICS.
                **{m: {agg_op(m): f'${m}'} for m in METRICS},
            }
        })

        rows = list(daily.aggregate(pipeline))
        ops = []
        live_users = []
        for r in rows:
            user = r.pop('_id')
            if not user:
                continue
            # Drop users with nothing in this window — they'd be dead rows.
            if not any(r.get(m) for m in METRICS):
                continue
            live_users.append(user)
            ops.append(UpdateOne(
                {'window': window, 'user': user},
                {'$set': {'window': window, 'user': user,
                          # `or 0`, not `.get(m, 0)`: $max over a set of daily
                          # rows that all predate a metric yields null, and a
                          # null column would break the UI's sort.
                          **{m: r.get(m) or 0 for m in METRICS},
                          'from': start, 'to': day_of(now),
                          'updated_at': utcnow()}},
                upsert=True,
            ))

        for i in range(0, len(ops), 1000):
            board.bulk_write(ops[i:i + 1000], ordered=False)

        # Drop users who have aged out of this rolling window, so a stale 7d
        # board doesn't keep someone who hasn't posted in a month.
        board.delete_many({'window': window, 'user': {'$nin': live_users}})
        logger.info(f"window {window:>5}: {len(ops)} users")


def ensure_indexes(db):
    db['leaderboard-daily'].create_index([('user', ASCENDING), ('date', ASCENDING)],
                                         unique=True)
    db['leaderboard-daily'].create_index([('date', ASCENDING)])
    db['leaderboard'].create_index([('window', ASCENDING), ('user', ASCENDING)],
                                   unique=True)
    # One index per metric so the UI can sort the board by any column fast.
    for m in METRICS:
        db['leaderboard'].create_index([('window', ASCENDING), (m, DESCENDING)])

    # Same index set for v1 ('') and the faceted taxonomy ('-v2').
    for suffix in ('', '-v2'):
        db['leaderboard-topic-daily' + suffix].create_index(
            [('user', ASCENDING), ('topic', ASCENDING), ('date', ASCENDING)], unique=True)
        db['leaderboard-topic-daily' + suffix].create_index([('date', ASCENDING)])
        db['leaderboard-topics' + suffix].create_index(
            [('window', ASCENDING), ('topic', ASCENDING), ('user', ASCENDING)], unique=True)
        # The core query: top creators within a topic, for a window.
        for m in TOPIC_METRICS:
            db['leaderboard-topics' + suffix].create_index(
                [('window', ASCENDING), ('topic', ASCENDING), (m, DESCENDING)])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='/app/config.yaml')
    ap.add_argument('--full', action='store_true',
                    help='rebuild every day from all history (first run)')
    ap.add_argument('--days', type=int, default=7,
                    help='recompute this many recent days (default 7)')
    args = ap.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)
    m = config['mongodb']
    db = MongoClient(m['uri'])[m['database']]

    ensure_indexes(db)

    now = utcnow()
    since = None if args.full else day_of(now) - timedelta(days=args.days - 1)
    logger.info(f"collecting {'ALL history' if args.full else f'since {since.date()}'}")

    stats, topic_stats, topic_stats_v2 = collect(db, since)
    write_daily(db, stats, since)
    write_topic_daily(db, topic_stats, since)                  # v1 — production
    write_topic_daily(db, topic_stats_v2, since, suffix='-v2')  # faceted taxonomy
    rebuild_windows(db, now)
    rebuild_topic_windows(db, now)
    rebuild_topic_windows(db, now, suffix='-v2')

    top = list(db['leaderboard'].find({'window': '7d'})
               .sort('video_uploads', DESCENDING).limit(5))
    logger.info("top 5 by video uploads (7d): " +
                (', '.join(f"{t['user']}={t['video_uploads']}" for t in top) or '(none)'))
    logger.info("done")
    return 0


if __name__ == '__main__':
    sys.exit(main())
