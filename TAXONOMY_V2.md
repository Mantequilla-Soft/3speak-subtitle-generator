# Faceted Taxonomy v2 — spec / redline draft

**Status:** tree settled; implemented in `src/tag_taxonomy_v2.py` (v1 untouched).
Thresholds calibrated 2026-07-17 by `src/calibrate_tags_v2.py` (800 videos,
F-beta 0.5) → `label_thresholds_v2` / `category_threshold_v2` in `config.yaml`;
attractor labels clamped to 0.95; `programming` had <3 silver examples, so dev
community rules are its main evidence (`automotive` was cut from the taxonomy). Score cache kept for re-fits. Not yet wired: a v2 writer, CLIP vision,
audio genre.

## Why change

v1 is a **single flat list of 16 labels** (`tag_taxonomy.py:TAXONOMY`) that mixes
three different questions into one:

- **format** — `vlog`, `tutorial`, `documentary` describe *how a video is made*
- **topic** — `food`, `crypto`, `gaming` describe *what it's about*
- **fuzzy** — `education` is neither, and rarely clears its own threshold

Because they share one slot, the model is forced to choose `gaming` **or**
`tutorial` for a video that is a *tutorial about gaming*, and a talking-head crypto
rant competes `news` vs `vlog` vs `finance` for one winner. v2 makes these
**independent facets** — a video gets one value from each facet, scored separately.

## The signal-source rule (this decides what's automatable)

Each facet is detectable from a **different** signal. No single model covers the
tree — that's the core finding:

| Signal | Detects | In pipeline today? |
|---|---|---|
| **T** — transcript (Whisper) | topic, language, has-speech | ✅ yes |
| **V** — vision (CLIP on 3 frames) | **format**, visual topics (food/nature/gaming) | ❌ to build |
| **A** — audio track | music-vs-speech, **music genre** | ❌ to build (music genre only) |
| **M** — metadata (Hive tags / community / title) | topic hints | ✅ yes |
| **D** — duration | duration bucket, format hint | ✅ trivial |

Detectability legend: 🟢 reliable · 🟡 moderate (needs calibration) · 🔴 hard (low
precision — ship last, conditional only).

---

## Facet 1 — FORMAT  *(one value; signal: V + D + A)*

*How the video is presented.* This is the **new** facet and the one vision is
actually good at — it separates cleanly even on the "boring" talking-head videos
where topic is ambiguous.

| label | definition | detect | signal |
|---|---|---|---|
| `talking-head` | one person addressing the camera, static framing | 🟢 | V |
| `vlog` | handheld/personal, follows the creator through activity | 🟡 | V+D |
| `interview` / `podcast` | 2+ people in conversation, often split/side-by-side | 🟢 | V |
| `documentary` | narrated over b-roll, no persistent on-camera host | 🟡 | V+T |
| `tutorial` | instructional, hands/materials/steps shown | 🟡 | V+T |
| `screencast` | screen recording — software, slides, browser | 🟢 | V |
| `presentation` | slide deck + speaker (lecture/conference) | 🟡 | V |
| `gameplay` | game capture, HUD/UI on screen | 🟢 | V |
| `music-performance` | someone playing/singing on camera | 🟡 | V+A |
| `music-video` | produced/edited music, not a live take | 🔴 | V+A |
| `livestream-rec` | recorded stream (long, continuous, chat overlay) | 🟡 | D+V |
| `short` | <60s, usually vertical | 🟢 | D |
| `animation` | animated/rendered, not live-action | 🟡 | V |
| `compilation` | montage of clips | 🔴 | V |

> Redline: are `music-video` and `compilation` worth the precision hit? I'd
> tentatively **cut both** for v2.0 and add later if needed.

---

## Facet 2 — TOPIC  *(2-level tree; signal: T + M + V)*

*What it's about.* This is v1's job, cleaned up into a **two-level tree**:

- **Level 1 — category** (~9 buckets): the frontend's primary browse axis. Small,
  stable, human-facing.
- **Level 2 — topic** (the leaves): what we actually classify. Every leaf has
  exactly one parent, so the category is a **pure roll-up** — zero extra detection
  cost.

The three formats (`vlog`, `tutorial`, `documentary`) moved **out** to Facet 1;
gaps the untagged sample exposed are filled in (spirituality, politics).
All existing `HIVE_TAG_MAP` / `COMMUNITY_MAP` evidence feeds the **leaves**.

### Why two levels help *detection*, not just the frontend

The parent is **easier to hit than the leaf** — a graceful-degradation path:

- classify at the leaf; if the top leaf clears its threshold, emit leaf **+** its
  parent (free).
- if no leaf clears but the *parent group's* summed score is confident, emit the
  **parent only** (e.g. "clearly Tech & Science, can't tell programming vs
  technology"). The frontend still files it correctly.

So the browse axis the users actually click is the **more reliable** one, and a
video is never dropped just because two sibling leaves were too close to split.

### The tree — 7 categories

`slug` = frontend id. `gaming` and `music` now sit under **Entertainment**; their
genre sub-trees (Facet 3) still hang off those leaves.

| L1 category (slug) | L2 topic (leaf) | note | detect | signal |
|---|---|---|---|---|
| **Tech & Science** (`tech-science`) | `technology` | consumer tech, gadgets, AI use | 🟢 | T+M |
| | `programming` | coding/dev/software eng | 🟡 | T+M |
| | `science` | physics, biology, space, research | 🟡 | T+M |
| | `education` | teaching/learning across subjects | 🔴 | T |
| **Crypto & Finance** (`crypto-finance`) | `cryptocurrency` | coins, chains, DeFi, NFTs | 🟢 | T+M |
| | `finance` | investing, markets, money | 🟡 | T+M |
| | `business` | entrepreneurship, marketing | 🟡 | T |
| **Entertainment** (`entertainment`) | `gaming` | video games (→ genre, Facet 3c) | 🟢 | V+M |
| | `music` | songs, performance (→ genre, Facet 3a) | 🟢 | A+M |
| | `vlog` | **evidence-only** — author tag / vlog community, never modelled | 🟢 | M |
| | `comedy` | entertainment, humor | 🔴 | T |
| | `lifestyle` | personal, daily life, celebrations | 🔴 | T+V |
| | `story-time` | storytelling, animated stories | 🟡 | V+T |
| | `commercial` | ads, promos, trailers | 🟡 | V |
| **Arts & DIY** (`arts-diy`) | `art` | drawing, painting, digital art | 🟡 | V+M |
| | `diy-crafts` | making/building (→ genre, Facet 3b) | 🟡 | V+T |
| | `photography` | *new?* stills, camera craft | 🟡 | M |
| **Food & Outdoors** (`food-outdoor`) | `food` | cooking, recipes, eating, drink | 🟢 | T+V |
| | `travel` | trips, places, tourism | 🟢 | T+V |
| | `nature` | wildlife, landscapes, outdoors | 🟢 | V+T |
| | `gardening` | homesteading, plants, grow | 🟡 | M+V |
| | `pets` | cats, dogs, domestic animals | 🟢 | V+M |
| **Sports & Health** (`sports-health`) | `sports` | athletics, competition | 🟡 | V+M |
| | `health` | wellness, medicine, mental health | 🟡 | T |
| | `fitness` | *split?* workout, gym | 🟡 | V+T |
| **Life & Society** (`life-society`) | `news` | current events, journalism | 🟡 | T+M |
| | `politics` | commentary/opinion/activism | 🟡 | T |
| | `spirituality` | faith, religion, tarot/esoteric | 🟡 | T+V |


### Redline decisions still open

- `programming` split from `technology`? (I say **yes**)
- `politics` split from `news`? (**yes** — untagged pile is full of it)
- `gardening`: leaf under Food & Travel (proposed) vs sub-genre of DIY?
- `fitness`: own leaf under Sports & Health, or fold into `health`/`sports`?
- `photography`: real leaf or drop into `art`?
- `comedy` / `lifestyle` are 🔴 low-precision catch-alls — keep or cut?

---

## Facet 3 — SUB-GENRE  *(conditional; only runs when the parent topic wins)*

The precision-saving move: **do not** run these labels on every video. Only when
Facet 2 lands on the parent do we run a second, small classifier. Keeps cost and
error low.

### 3a. `music` → genre  *(signal: A — needs an audio model, NOT vision/transcript)*
```
rock   pop   hip-hop/rap   electronic   jazz   classical
folk/acoustic   latin   reggae   metal   country   r&b/soul
gospel/worship   afrobeat   world/traditional
```
🔴 hard. A transcript can't hear genre and a frame can't either — this needs a
CPU audio-genre classifier (PANNs / musicnn-style) on the extracted audio. It is
the **last** thing to build, and optional.

### 3b. `diy-crafts` → genre  *(signal: V + T)*
```
woodworking   building/construction   crafting/handmade   upcycling/repair
sewing/textiles   electronics/making   home-improvement   restoration
```
🟡 moderate. CLIP + transcript can take a reasonable guess; calibrate per label.

### 3c. `gaming` → genre  *(optional, signal: V + M)*
```
fps   rpg   strategy   simulation   retro   mmo   mobile   sandbox
```
🔴 hard from frames alone; Hive tags (`minecraft`, `splinterlands`) carry more.
Probably **defer**.

---

## Facet 4 — LANGUAGE  *(one value; signal: T — already computed)* 🟢

Already produced as `source_lang`. Promote it to a first-class facet. Nearly free,
high discovery value. ISO 639-1 code (`en`, `es`, `de`, `fr`, `hi`, …) or `none`
for music-only.

## Facet 5 — DURATION  *(one value; signal: D — metadata)* 🟢
```
short   (<1 min)      standard (1–20 min)      long (>20 min)
```

## Facet 6 — CONTENT FLAGS  *(0–n; signal: T + A)*
```
has-speech        music-only / no-speech        (from Whisper)   🟢
sensitive:political   sensitive:mature   sensitive:graphic  (from transcript)  🔴
```
The `has-speech` flag alone is valuable — it cleanly splits the "empty-tag" pile
into "no transcript to classify" vs "had a transcript, still failed".

## Facet 7 — AI-MADE  *(one flag; signal: M — title/description/Hive tags)* 🟡

*Was the video itself made with AI?* Metadata-only, no model — same cheap
side-channel shape as duration/flags above. Three signals, checked in order:

0. the creator is on the AI-only list (`AI_CREATORS` in `tag_taxonomy_v2.py`)
   — every video of theirs is flagged, evidence `creator:<name>`.
1. author's own Hive tags naming an AI tool/label (`ai`, `aiart`, `aivideo`,
   `midjourney`, `sunoai`, ...) — see `AI_HIVE_TAGS` in `tag_taxonomy_v2.py`.
2. title/description explicitly asserting the content is AI-made ("AI
   generated", "AI video", "made with AI", a named generator tied to a
   generation verb, ...) — see `AI_TEXT_PATTERNS`.

A bare AI-tool mention (e.g. "thumbnail edited by ChatGPT") is deliberately
**not** enough — text matches require an explicit generation claim, and a
short window before the match is checked for third-party/reporting language
("the White House **posted** an AI-generated video") so the flag isn't set
on a video that merely *discusses* someone else's AI content. Still 🟡: the
description is what the creator chose to say, not a content analysis, so
`ai_generated_evidence_v2` always records what actually matched for review.

Written by `tag_taxonomy_v2.detect_ai_generated()`, called from both the live
pipeline (`tag_live_v2.py`) and the batch tagger (`tag_videos_v2.py`) for
every new video, decoupled from topic-tag fusion — it is set even when no
topic tag is confident. `backfill_ai_flag.py` is the one-shot (and
`--watch`-able) catch-up for videos that already had v2 topic tags before
this facet existed, since the topic-tagging worklist never revisits them.

---

## Category + topic are ONE flat tag namespace

Decision: **category and topic are not separate fields — they're all just "tags".**
A category slug (`crypto-finance`) and a leaf (`cryptocurrency`) live in the *same*
list. The tree still exists (for frontend grouping and the fallback rule), but
storage stays flat.

The rule: **prefer the leaf; fall back to the category.**
- leaf clears its threshold → write the **leaf** tag (e.g. `cryptocurrency`).
- no leaf clears but the parent group is confident → write the **category** tag
  (`crypto-finance`). Still a correct, useful tag; just coarser.
- either way it's one entry in the same `tags_list` the dashboard already reads.

The parent-of map (`leaf → category`) is kept only so the frontend can roll a leaf
up to its browse bucket — it is *not* a second stored field.

## Eligibility rules

The v2 tagger (text and vision alike) skips:

1. Embed videos with **no `hive_permlink`** — never became a Hive post, so
   unreachable on every frontend (confirmed: the unplayable videos in the CLIP
   spot-check were exactly these). Legacy `videos` docs are Hive posts by
   construction and are unaffected. (~1,092 of the untagged embeds.)
2. Videos whose creator is **`hidden: true` in `contentcreators`** (951
   creators) — hidden creators are not surfaced on frontends.

## Storage schema — backwards compatible, no migration

v2 writes into the **existing** `subtitles-tags` shape. `tags` / `tags_list` are
unchanged in structure — v2 just puts richer, better values in them. The only
truly new keys are optional side-channels (`language`, `duration`, `flags`,
`ai_generated_v2`) and a new `tag_model` marker so v2 rows are re-runnable and
old consumers ignore what they don't know.

```json
{
  "author": "...", "permlink": "...",
  "tags":      "cryptocurrency,en",              // v1 field, untouched shape
  "tags_list": ["cryptocurrency", "en"],         // v1 field, untouched shape
  "language":  "en",                             // new, optional
  "duration":  "standard",                       // new, optional
  "flags":     ["has-speech"],                   // new, optional
  "ai_generated_v2": true,                       // new, optional (Facet 7)
  "ai_generated_evidence_v2": ["text:ai generated"],
  "ai_generated_checked_at": "2026-09-23T...",
  "tag_scores": { "cryptocurrency": 0.71 },
  "tag_model": "v2",                             // marks v2 rows; old rows keep their model
  "manual": false
}
```
Nothing in v1 is edited in place. `manual:true` locks still win. Old rows keep
working until re-tagged; the dashboard's `split(',')` on `tags` still works.

---

## Migration from v1's 16 labels

The three **formats** leave the tag namespace (see the Facet 1 discussion — likely
folded into vision cues, not stored as user-facing tags). Everything else stays a
tag, same name:

| v1 label | v2 destination |
|---|---|
| `vlog` `documentary` | dropped as tags (format cue only, if kept at all) |
| `tutorial` | vision/transcript cue → boosts topic; not a stored tag |
| `education` | tag `education` (leaf under Tech & Science) |
| `food` `travel` `nature` `gaming` `music` `art` `sports` `health` `science` `technology` `news` `finance` `cryptocurrency` | tag, same name (now leaves) |

Everything in `HIVE_TAG_MAP`, `COMMUNITY_MAP`, `EXCLUSIVE_COMMUNITY_MAP`,
`HIVE_TAG_STOPWORDS` is reused unchanged — it already targets these leaves. New
work is only: add the category-slug tags, wire the `leaf → category` parent map,
and expand the vocab with the new leaves.

---

## Rollout order (by ROI, cheapest first)

1. **Facets 4+5+6a** (language, duration, has-speech) — nearly free, no new model.
2. **Facet 1 format** via CLIP — the high-value new capability; helps even the
   talking-head videos vision "couldn't do topic on".
3. **Facet 2 topic** cleanup + expanded maps + looser thresholds re-pass.
4. **Facet 3 sub-genres**, conditional. `diy-crafts` first; **music genre (audio
   model) last** and only if wanted.

## Will the auto-tagger find these? — short answer per facet

- **Language, duration, has-speech:** yes, essentially solved. 🟢
- **Format:** yes, this is exactly CLIP's strength. 🟢 for the distinct ones.
- **Topic:** same as today, moderate; splits (politics/programming) help precision. 🟡
- **Sub-genres:** the hard part. DIY 🟡, gaming-genre 🔴, **music genre needs a new
  audio model** 🔴. Do these conditionally, last, and accept lower precision.
