# Video Tags v2 — Frontend Guide

How video tags are structured, what values exist, and how to display them.
This is the **data contract**, not an implementation guide.

Counts in this doc are live as of 2026-07-18 and grow continuously — a
background service keeps tagging the back catalogue and every new upload.

---

## 1. The one thing to know first

**v1 and v2 tags coexist.** Nothing has been switched over yet.

| field | who owns it | use it for |
|---|---|---|
| `tags` / `tags_list` | **v1** — what the frontend shows *today* | current production display |
| `tags_v2` / `tags_list_v2` | **v2** — the new system | new tag UI, browse, filters |

The v2 fields are additive. v1 keeps working untouched until you decide to
switch. You can build and preview the whole v2 experience with zero risk to
what's live, and flip when ready.

---

## 2. Tag structure: one flat list, two levels of meaning

A video carries a **flat list of tags** (usually 1–3, max 5). No nesting, no
separate fields to join.

```
["gaming"]
["food", "travel"]
["crypto-finance"]        ← a category tag, see below
```

Behind that flat list sits a **2-level tree**, used for grouping and browsing:

- **Category** (7 of them) — the broad browse buckets. Small and stable.
- **Topic** (the leaves) — the specific subject.

Every topic belongs to exactly one category, so you can always roll a tag up to
its bucket. **Both levels can appear as tags in the same list** — see §4.

---

## 3. The full tag vocabulary

7 categories, 29 topics. These are the only values that can appear.
`slug` is what's stored; `label` is a suggested display name.

### Tech & Science — `tech-science`
| topic slug | label | videos |
|---|---|---:|
| `technology` | Technology | 10,171 |
| `education` | Education | 7,038 |
| `science` | Science | 1,716 |
| `programming` | Programming | 48 |

### Crypto & Finance — `crypto-finance`
| topic slug | label | videos |
|---|---|---:|
| `cryptocurrency` | Cryptocurrency | 6,639 |
| `finance` | Finance | 6,191 |
| `business` | Business | 30 |

### Entertainment — `entertainment`
| topic slug | label | videos |
|---|---|---:|
| `music` | Music | 40,791 |
| `gaming` | Gaming | 15,840 |
| `vlog` | Vlog | 13,320 |
| `film-tv` | Film & TV | new |
| `comedy` | Comedy | 270 |
| `story-time` | Story Time | 200 |
| `lifestyle` | Lifestyle | 370 |
| `commercial` | Commercial | 51 |

> `vlog` is **evidence-only**: it is applied when the author tags a video as a
> vlog or posts it to a vlog community, and is never guessed by a model. It is
> really a *format* (a vlog can be about anything), so treat it as a browse
> convenience rather than a subject — a video can be both `vlog` and `travel`.

### Arts & DIY — `arts-diy`
| topic slug | label | videos |
|---|---|---:|
| `art` | Art | 9,217 |
| `diy-crafts` | DIY & Crafts | 159 |
| `photography` | Photography | 42 |

### Food & Outdoors — `food-outdoor`
| topic slug | label | videos |
|---|---|---:|
| `nature` | Nature | 6,731 |
| `travel` | Travel | 5,023 |
| `food` | Food | 4,781 |
| `pets` | Pets | 98 |
| `gardening` | Gardening | 76 |

### Sports & Health — `sports-health`
| topic slug | label | videos |
|---|---|---:|
| `sports` | Sports | 11,973 |
| `health` | Health | 4,486 |
| `fitness` | Fitness | 16 |

### Life & Society — `life-society`
| topic slug | label | videos |
|---|---|---:|
| `news` | News | 7,448 |
| `spirituality` | Spirituality | 178 |
| `politics` | Politics | 19 |

**~134,000 videos currently carry at least one v2 tag.**

---

## 4. The one rule that will surprise you

A tag list may contain a **category slug** instead of a topic:

```
["crypto-finance"]     ← category, not a topic
["entertainment"]
```

This is intentional. When the tagger is confident about the *general area* but
can't safely pick the specific topic, it emits the category rather than
guessing wrong. It means *"definitely Crypto & Finance, not sure which topic."*

**What this means for the UI**

- A category tag is a **valid, correct, coarser** tag — not an error or a
  placeholder. Display it normally, using the category's label.
- Filtering by a category must return **both** videos tagged with that category
  *and* videos tagged with any of its topics. Filtering "Entertainment" should
  surface `music`, `gaming`, `comedy` videos *and* plain `entertainment` ones.
- Category tags are currently rare (a few dozen to a couple hundred each), but
  design for them from day one.

How to tell them apart: the 7 category slugs are listed in §3 headings.
Everything else is a topic.

---

## 5. Display guidance

**Ordering** — tags are stored best-first. The first tag is the strongest
signal, so if you show only one, show the first.

**How many** — 1–3 is typical, 5 is the hard maximum. A single-tag chip row
covers most videos.

**Grouping / browse** — use the 7 categories as the primary browse axis: a
short, stable top-level nav, with topics as refinements. This is the main
reason the tree exists — the category level is both smaller *and* more reliably
correct than the topic level.

**Long-tail topics** — some topics are tiny right now (`fitness` 16,
`politics` 19, `business` 30). They're real and will grow, but don't build UI
that assumes every topic has enough videos to fill a shelf. Drive shelves off
categories, not individual topics.

**Untagged videos** — plenty of videos have no v2 tags at all (never processed,
or processed with no confident result). Design the empty state; don't assume a
tag always exists.

---

## 6. Extra fields you may find useful

| field | meaning |
|---|---|
| `tags_list_v2` | the tag list (array of slugs) — the main one |
| `tags_v2` | same values as a comma-separated string |
| `tag_model_v2` | how it was tagged: `v1-migrated`, `v2`, `community-rule-v2` |
| `unavailableOnTagging` | `true` = the video file could not be fetched when we tried to analyse it. Strong hint the media is gone/broken — useful for hiding or flagging dead videos. ~190 so far and growing. |

An **empty** `tags_list_v2` (`[]`) means *"we analysed it and found nothing
confident"* — different from the field being absent, which means *"not yet
processed."* Treat both as untagged for display.

---

## 7. Topic leaderboards (v2)

The per-topic creator boards have v2 twins. Same shape, same fields, same
windows — only the collection name changes:

| v1 (live today) | v2 |
|---|---|
| `leaderboard-topics` | `leaderboard-topics-v2` |
| `leaderboard-topic-daily` | `leaderboard-topic-daily-v2` |

`leaderboard` and `leaderboard-daily` (the overall per-creator boards) are
**taxonomy-independent — nothing to switch there.**

Both taxonomies are rebuilt together every hour, so the v2 collections are as
fresh as the v1 ones.

**Document shape**

```json
{
  "topic": "art", "user": "acidyo", "window": "7d",
  "from": "2026-07-12T00:00:00", "to": "2026-07-18T00:00:00",
  "uploads": 0, "video_uploads": 0, "short_uploads": 0,
  "watch_secs": 140, "video_watch_secs": 140, "short_watch_secs": 0,
  "updated_at": "..."
}
```

- `window`: `7d` · `30d` · `365d` · `all`
- sortable metrics: `uploads`, `video_uploads`, `short_uploads`,
  `watch_secs`, `video_watch_secs`, `short_watch_secs`
- indexed on `(window, topic, <metric>)` — sorting by any metric is fast

**Top creators in a topic** — the common case, a straight swap:

```js
db['leaderboard-topics-v2']
  .find({ window: '7d', topic: 'gaming' })
  .sort({ uploads: -1 })
  .limit(20)
```

**Top creators in a CATEGORY** — needs the §4 rule. A category slug appears in
`topic` in its own right (videos tagged only at category level), so a category
board must sum the category *and* its leaves:

```js
db['leaderboard-topics-v2'].aggregate([
  { $match: { window: '7d',
              topic: { $in: ['crypto-finance',            // the category itself
                             'cryptocurrency','finance','business'] } } },  // its leaves
  { $group: { _id: '$user',
              uploads:    { $sum: '$uploads' },
              watch_secs: { $sum: '$watch_secs' } } },
  { $sort: { uploads: -1 } },
  { $limit: 20 }
])
```

Querying only `topic: 'crypto-finance'` is **not** a category board — it returns
just the coarsely-tagged videos and will look almost empty.

**What changes visibly:** `tutorial` disappears (it was a format, not a subject)
and `vlog`, `spirituality`, `pets`, `story-time`, `programming`, `commercial`
plus the 7 category slugs become rankable — 35 possible `topic` values.

## 8. Practical notes

- **Tags are not final.** Videos get re-tagged as the system improves; treat
  tags as derived data, cache accordingly.
- **New uploads** are tagged automatically, usually within ~15 minutes of
  publishing.
- **Counts shift.** The back catalogue is still being processed, so the smaller
  topics will grow noticeably over the coming days.
- **Vocabulary is closed.** Only the 35 slugs in §3 (28 topics + 7 categories)
  can ever appear. Anything else is a bug — safe to build a strict mapping of
  slug → label/icon/colour.
