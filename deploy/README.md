# Leaderboard stats (systemd timer, hourly)

Rolls up per-user daily stats into `leaderboard-daily`, then totals them into
`leaderboard` for the 7d / 30d / 365d / all windows.

```bash
cd /home/tibfox/mantequilla/3speak-subtitles

# 1. Build the image and do the ONE-TIME full history rebuild (~35s).
sudo ./manage.sh leaderboard-build

# 2. Install the hourly timer.
sudo cp deploy/3speak-leaderboard.service /etc/systemd/system/
sudo cp deploy/3speak-leaderboard.timer   /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now 3speak-leaderboard.timer
```

Check it:

```bash
systemctl list-timers 3speak-leaderboard       # when does it next fire?
sudo systemctl start 3speak-leaderboard        # run a refresh right now
tail -f leaderboard.log

# read the board (window defaults 7d, metric defaults video_uploads)
sudo ./manage.sh leaderboard-top 7d  video_uploads
sudo ./manage.sh leaderboard-top all tags_given
sudo ./manage.sh leaderboard-top 7d  video_watch_secs
```

The hourly run only recomputes the last 7 days (~3s). Re-run
`leaderboard-build` only if you change how a metric is calculated.

**Watch-time caveat:** `view-durations` (the only source of watched seconds)
starts 2026-07-06, so `*_watch_secs` for the 30d/365d/all windows is only as
deep as that. Uploads and tags have full history.

## Querying it yourself

`leaderboard` has one doc per (window, user), indexed for sorting by any metric:

```js
db.leaderboard.find({window: "7d"}).sort({short_uploads: -1}).limit(10)
```

## Per-topic board

`leaderboard-topics` has one doc per `(window, topic, user)` — the top creators
within each topic, using the auto-generated transcription tags:

```js
// top gaming creators this week
db.getCollection("leaderboard-topics")
  .find({window: "7d", topic: "gaming", uploads: {$gt: 0}})
  .sort({uploads: -1}).limit(10)

// or by how long their gaming content was watched
  .sort({watch_secs: -1})
```

Fields: `window`, `topic`, `user`, `uploads`, `watch_secs`, `from`, `to`.
Topics are the 16 tags in `config.yaml`.

```bash
sudo ./manage.sh leaderboard-topic gaming 7d  uploads
sudo ./manage.sh leaderboard-topic music  all watch_secs
```

**Why the job runs `--full`:** a video uploaded years ago can be *tagged* today
(the back-catalog tagger is still working), and its topic row belongs to its
**upload** day — which an incremental window would never revisit. A full rebuild
costs only ~40s because it never touches the 7.2M-row `views` collection.

---

# Metadata tagger as a systemd service

Runs the never-transcribed back-catalog tagger as a boot-persistent service that
survives SSH logout **and** reboots. The job is resumable, so an interrupted run
continues where it stopped.

## Install (run as root, one time)

```bash
cd /home/tibfox/mantequilla/3speak-subtitles

# 1. Build the image FIRST, so the service doesn't build under systemd's start
#    timeout. Wait for this to finish.
sudo ./manage.sh tag-meta-build

# 2. Install and enable the unit.
sudo cp deploy/3speak-metadata-tagger.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now 3speak-metadata-tagger
```

`enable --now` starts it immediately and makes it come back on every boot.
You can close SSH right after — it's owned by systemd, not your shell.

## Watch / control it

```bash
sudo systemctl status 3speak-metadata-tagger      # running? last exit?
tail -f /home/tibfox/mantequilla/3speak-subtitles/metadata-tagger.log   # live progress
sudo journalctl -u 3speak-metadata-tagger -f      # systemd's view

sudo systemctl stop 3speak-metadata-tagger        # graceful stop (finishes current video)
sudo systemctl start 3speak-metadata-tagger       # resume
```

## When the whole catalog is tagged

The service exits 0 and goes inactive (it will not loop — `Restart=on-failure`
only restarts on crashes). It would re-run on the next reboot and quickly skip
everything, so once you're done, disable it:

```bash
sudo systemctl disable 3speak-metadata-tagger
```

## Notes

- Runs as **root** because the `tibfox` user isn't in the `docker` group (you use
  `sudo` for docker). The container writes only to MongoDB — no local file
  ownership issues.
- `Restart=on-failure` + `RestartSec=30`: a Docker daemon hiccup or crash mid-run
  auto-resumes after 30s. A clean completion does not restart.
- To change pace or scope, edit the `command:` for `metadata-tagger` in
  `docker-compose.yml` (e.g. `--window-days`, `--until-date`, `--sleep`), then
  `sudo systemctl restart 3speak-metadata-tagger`.
```
