# Leaderboard — Frontend Guide

How to read the leaderboard, what every column means, and the exact queries to
run. This is the **data contract**, not an implementation guide.

There is no HTTP API. The board is precomputed into MongoDB and you query it
directly — no aggregation at request time, no joins. Sorting any column in any
window is a single indexed `find().sort().limit()`.

Written 2026-07-27. Stream columns exist and are indexed but read `0` until the
livestream service starts calling its endpoints — see §7.

---

## 1. Where the data lives

Database `threespeak` (same one the checker uses). Four collections, all owned
and written **only** by `src/leaderboard_stats.py`:

| collection | one doc per | use it for |
|---|---|---|
| `leaderboard` | `(window, user)` | **the board** — ranked tables |
| `leaderboard-daily` | `(user, day)` | time series / sparklines |
| `leaderboard-topics` | `(window, topic, user)` | per-topic board (v1 tags) |
| `leaderboard-topics-v2` | `(window, topic, user)` | per-topic board (v2 tags) |

Rebuilt in full every hour at `:07`. Everything is upserted and stale rows are
pruned, so a count that drops to zero actually drops.

---

## 2. The four windows

Every board row has a `window` field. **It is always part of your query** —
without it you get the same user back four times.

| `window` | covers |
|---|---|
| `7d` | last 7 days including today |
| `30d` | last 30 days |
| `365d` | last 365 days |
| `all` | all history |

Rolling windows are recomputed hourly, so `7d` shifts forward each day. Rows
also carry `from` / `to` (UTC midnight dates) and `updated_at` if you want to
show "as of".

---

## 3. The columns

All 13 are present on **every** board row, always a number, never null or
missing. A creator with no activity for a metric has `0`.

### Video

| field | meaning |
|---|---|
| `video_uploads` | long-form videos published **to Hive** |
| `short_uploads` | shorts / reels published **to Hive** |
| `video_watch_secs` | seconds their videos were watched |
| `short_watch_secs` | seconds their shorts were watched |
| `tags_given` | viewer-tags they added to **other** creators' videos |

### Livestream

| field | meaning |
|---|---|
| `streams` | stream sessions started |
| `stream_secs` | total seconds live (**ended streams only**) |
| `stream_peak_viewers` | best concurrent viewers — **a max, see §5** |
| `stream_viewers` | viewer-join events — **volume, not unique reach** |

### Boosts

| field | meaning |
|---|---|
| `boosts_received` | boosts sent to them on their streams |
| `boost_amount_received` | summed amount of those |
| `boosts_given` | boosts they sent to **other** streamers |
| `boost_amount_given` | summed amount of those |

---

## 4. Queries

### Top 20 streamers by time live, last 7 days

```js
db.leaderboard.find({ window: "7d" }).sort({ stream_secs: -1 }).limit(20)
```

That shape is the whole API. Swap the field, swap the window:

```js
db.leaderboard.find({ window: "30d" }).sort({ stream_peak_viewers: -1 }).limit(20)
db.leaderboard.find({ window: "all" }).sort({ boost_amount_received: -1 }).limit(20)
db.leaderboard.find({ window: "7d"  }).sort({ boosts_given: -1 }).limit(20)
```

Every `(window, <metric>)` pair has its own descending index, so these are pure
index scans with **no in-memory sort** — verified against the live board for
each stream column.

### One creator's card

```js
db.leaderboard.findOne({ window: "7d", user: "buttcoins" })
```

`user` is always a lowercase Hive username. Returns `null` if they had no
activity at all in that window — treat that as all-zeros, not an error.

### Only show creators who actually streamed

A creator with `0` everywhere is dropped from the board, but someone who
uploaded and never streamed still has `stream_secs: 0`. Filter explicitly:

```js
db.leaderboard.find({ window: "7d", stream_secs: { $gt: 0 } })
              .sort({ stream_secs: -1 }).limit(20)
```

### Daily series for a chart

```js
db.getCollection("leaderboard-daily").find({
  user: "buttcoins",
  date: { $gte: ISODate("2026-07-01T00:00:00Z") }
}).sort({ date: 1 })
```

`date` is UTC midnight. Days with no activity have **no row** — fill gaps with
zero client-side rather than expecting a dense series.

> Careful: summing `stream_peak_viewers` across daily rows is wrong (§5). To
> chart a peak over time, plot the daily value or take the max, never a sum.

---

## 5. `stream_peak_viewers` is a max, not a sum

Every other column adds up across days. This one does not.

A creator who peaked at **12** concurrent viewers on Monday and **9** on
Tuesday peaked at **12** for the week — not 21. Summing it would invent
concurrency that never happened. The rollup uses `$max` at both the daily and
window level, and the same rule applies to anything you compute downstream.

Practical consequences:

- The `all` window's value is their best stream **ever**, not a recent number.
- You cannot derive a weekly peak by adding the daily ones.
- It is a *high-water mark*, so it never decreases as a window widens:
  `7d ≤ 30d ≤ 365d ≤ all`, always.

---

## 6. Reading the numbers honestly

**`stream_viewers` is join volume, not audience.** It counts viewer-join events.
The same person leaving and rejoining three times counts three times. Do not
label it "unique viewers" — there is no unique-reach number available.

**`boost_amount_*` has no unit.** The amount is whatever the caller sent, with
no currency attached and no normalisation. Boosts with a null amount still count
in `boosts_*` but add `0` to the total, so a creator can have
`boosts_received: 5, boost_amount_received: 0`. Render it as a bare number, or
label the unit only once you know it.

**Self-boosts are excluded from both sides.** Boosting your own stream credits
nobody. Same rule as `tags_given`, which only counts tags on other creators'
videos.

**`stream_secs` covers ended streams only.** A stream in progress contributes to
`streams`, `stream_peak_viewers` and `stream_viewers` immediately, but its
duration appears only once it ends. A live streamer's time will look low
mid-broadcast — that is expected, not a bug.

**Streams bucket on their start day.** An overnight stream lands whole on the
day it started, so a daily chart shows it on day one rather than split.

**Uploads mean published to Hive.** An encoded file that never got a Hive post
does not count, so these numbers are lower than raw upload counts, deliberately.

---

## 7. Stream columns are zero until the service is wired

The stream and boost columns read from `stream-stats` / `stream-boosts`, written
by the checker's `/stream-stats/*` endpoints. **Nothing calls those endpoints
yet, so both collections are empty and all eight columns are `0`.**

Nothing needs to change here when that lands — the columns are already computed
and indexed, and they populate on the next hourly run. You can build and ship
the UI against them now; it will simply start showing numbers.

Two things the stream writer has to get right for these to be correct:

- **`streamId` must be stable for a whole session.** It is the upsert key. If it
  changes mid-stream, one stream becomes several docs and `stream_peak_viewers`
  — a per-doc max — silently under-reports rather than failing loudly.
- **`host` should be the lowercase Hive username.** It is lowercased defensively
  on read, but it must be the same identity used elsewhere or the streamer
  splits into two board rows.

The older `livestreams` / `liveviews` collections are the previous
channel/streamkey system and are deliberately not read by any of this.
