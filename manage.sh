#!/bin/bash
# 3Speak Subtitle Generator Management Script

set -e

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$PROJECT_DIR"

case "$1" in
    build)
        echo "Building Docker image..."
        docker compose build
        ;;

    start)
        echo "Starting subtitle generator service..."
        docker compose up -d
        ;;

    stop)
        echo "Stopping subtitle generator service..."
        docker compose down
        ;;

    restart)
        echo "Restarting subtitle generator service..."
        docker compose restart
        ;;

    logs)
        echo "Showing logs (Ctrl+C to exit)..."
        docker compose logs -f
        ;;

    status)
        echo "Service status:"
        docker compose ps
        ;;

    clean)
        echo "Cleaning temporary files..."
        rm -rf temp/*
        echo "Temporary files cleaned"
        ;;

    clean-all)
        read -p "This will delete all models, temp files, and subtitles. Are you sure? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Cleaning all data..."
            rm -rf temp/* models/* subtitles/*
            echo "All data cleaned"
        fi
        ;;

    shell)
        echo "Opening shell in container..."
        docker compose exec subtitle-generator /bin/bash
        ;;

    stats)
        echo "Storage usage:"
        echo "  Models:    $(du -sh models 2>/dev/null || echo '0')"
        echo "  Temp:      $(du -sh temp 2>/dev/null || echo '0')"
        echo "  Subtitles: $(du -sh subtitles 2>/dev/null || echo '0')"
        ;;

    retag-dry)
        echo "Dry-run re-tag (writes nothing)..."
        LIMIT="${2:-50}"
        docker compose --profile tools run --rm retagger \
            python -u src/retag.py --dry-run --limit "$LIMIT" --fetch-missing
        ;;

    retag-empty)
        echo "Re-tagging untagged videos with 50+ words of transcript, detached..."
        echo "(reuses local .srt; near-wordless shorts are skipped, not churned)"
        docker compose --profile tools up -d empty-retagger
        echo ""
        echo "Started detached — safe to close SSH. Resumable."
        echo "Follow with: $0 retag-empty-logs   |   Stop with: $0 retag-empty-stop"
        ;;

    retag-empty-logs)
        docker compose --profile tools logs -f empty-retagger
        ;;

    retag-empty-status)
        docker compose --profile tools ps empty-retagger
        ;;

    retag-empty-stop)
        echo "Stopping empty-retagger (resumable; re-run '$0 retag-empty')..."
        docker compose --profile tools stop empty-retagger
        ;;

    retag-empty-dry)
        echo "Dry-run: untagged videos with 50+ words of transcript (writes nothing)..."
        docker compose --profile tools run --rm empty-retagger \
            python -u src/retag.py --only-empty --all --dry-run \
                --min-content-words 50 --fetch-missing
        ;;

    retag)
        echo "Starting detached re-tag of all stale videos..."
        echo "Safe to close this SSH session. Resume after any interruption by re-running."
        docker compose --profile tools up -d retagger
        echo "Follow progress with: $0 retag-logs"
        ;;

    retag-logs)
        echo "Following re-tag progress (Ctrl+C to detach, job keeps running)..."
        docker compose --profile tools logs -f retagger
        ;;

    retag-status)
        docker compose --profile tools ps retagger
        ;;

    retag-stop)
        echo "Stopping re-tagger (progress is kept; re-run '$0 retag' to resume)..."
        docker compose --profile tools stop retagger
        ;;

    backfill-hls-gap-dry)
        echo "Dry-run: listing video_v2-only videos the filename-gate bug skipped..."
        docker compose --profile tools run --rm hls-gap-backfill \
            python -u src/backfill_hls_gap.py --dry-run --since "${2:-2026-02-18}"
        ;;

    backfill-hls-gap)
        echo "Starting detached backfill of video_v2-only videos (2026-06 -> 2026-08-10 gap)..."
        echo "Safe to close this SSH session. Resume after any interruption by re-running."
        docker compose --profile tools up -d hls-gap-backfill
        echo "Follow progress with: $0 backfill-hls-gap-logs"
        ;;

    backfill-hls-gap-logs)
        echo "Following backfill progress (Ctrl+C to detach, job keeps running)..."
        docker compose --profile tools logs -f hls-gap-backfill
        ;;

    backfill-hls-gap-status)
        docker compose --profile tools ps hls-gap-backfill
        ;;

    backfill-hls-gap-stop)
        echo "Stopping backfill (progress is kept; re-run '$0 backfill-hls-gap' to resume)..."
        docker compose --profile tools stop hls-gap-backfill
        ;;

    tag-meta-fast)
        echo "Evidence-only pass: tagging from author Hive tags, no classifier."
        echo "Orders of magnitude faster. Videos with no mappable tags are left"
        echo "untagged for the classifier pass to pick up afterwards."
        echo ""
        echo "NOTE: stop the classifier service first, or they will fight over the"
        echo "same videos:  sudo systemctl stop 3speak-metadata-tagger"
        echo ""
        docker rm 3speak-metadata-tagger-fast 2>/dev/null || true
        docker compose --profile tools run -d --name 3speak-metadata-tagger-fast \
            metadata-tagger \
            python -u src/tag_metadata.py --evidence-only --window-days 90 --sleep 0
        echo "Started detached. Follow with: $0 tag-meta-fast-logs"
        ;;

    tag-meta-fast-logs)
        docker logs -f 3speak-metadata-tagger-fast
        ;;

    tag-meta-dry)
        echo "Dry-run metadata tagging of the most recent window (writes nothing)..."
        docker compose --profile tools run --rm metadata-tagger \
            python -u src/tag_metadata.py --dry-run --window-days 30 \
                --max-windows 1 --fetch-missing
        ;;

    tag-meta-build)
        echo "Building the metadata-tagger image (stay attached until this finishes)..."
        docker compose --profile tools build metadata-tagger
        echo "Build done. Now run '$0 tag-meta' — it returns instantly and you can close SSH."
        ;;

    tag-meta)
        echo "Starting metadata tagger, detached..."
        docker compose --profile tools up -d metadata-tagger
        echo ""
        echo "Started. It is now owned by the Docker daemon, not this shell —"
        echo "you can close SSH immediately. Newest videos first, resumable."
        echo "Follow with: $0 tag-meta-logs   |   Stop with: $0 tag-meta-stop"
        ;;

    tag-meta-logs)
        docker compose --profile tools logs -f metadata-tagger
        ;;

    tag-meta-status)
        docker compose --profile tools ps metadata-tagger
        ;;

    tag-meta-stop)
        echo "Stopping metadata tagger (resumable; re-run '$0 tag-meta')..."
        docker compose --profile tools stop metadata-tagger
        ;;

    leaderboard-build)
        echo "Full leaderboard rebuild from all history (run once, first time)..."
        docker compose --profile tools run --rm leaderboard \
            python -u src/leaderboard_stats.py --full
        ;;

    leaderboard)
        echo "Refreshing leaderboard (recent days)..."
        docker compose --profile tools run --rm leaderboard \
            python -u src/leaderboard_stats.py --days "${2:-7}"
        ;;

    leaderboard-top)
        WINDOW="${2:-7d}"
        METRIC="${3:-video_uploads}"
        echo "Top 10 by $METRIC ($WINDOW):"
        MURI=$(python3 -c "import yaml;print(yaml.safe_load(open('config.yaml'))['mongodb']['uri'])")
        mongosh "$MURI" --quiet --eval "
          db.leaderboard.find({window:'$WINDOW'})
            .sort({$METRIC:-1}).limit(10)
            .forEach((r,i)=>print('  '+String(i+1).padStart(2)+'. '+
              r.user.padEnd(22)+r.$METRIC));
        "
        ;;

    leaderboard-topic)
        # metric: uploads | video_uploads | short_uploads
        #         watch_secs | video_watch_secs | short_watch_secs
        TOPIC="${2:-gaming}"
        WINDOW="${3:-7d}"
        METRIC="${4:-uploads}"
        echo "Top 10 creators in '$TOPIC' by $METRIC ($WINDOW):"
        MURI=$(python3 -c "import yaml;print(yaml.safe_load(open('config.yaml'))['mongodb']['uri'])")
        mongosh "$MURI" --quiet --eval "
          const r = db.getCollection('leaderboard-topics')
            .find({window:'$WINDOW', topic:'$TOPIC', $METRIC:{\$gt:0}})
            .sort({$METRIC:-1}).limit(10).toArray();
          if(!r.length) print('  (nobody yet)');
          r.forEach((x,i)=>print('  '+String(i+1).padStart(2)+'. '+
            x.user.padEnd(22)+x.$METRIC));
        "
        ;;

    set-tags)
        # ./manage.sh set-tags <url|author/permlink> <tag,tag> [--note "..."]
        shift
        docker compose --profile tools run --rm tagtools \
            python -u src/set_tags.py "$@"
        ;;

    community-rules)
        # ./manage.sh community-rules [--dry-run]
        # Applies EXCLUSIVE_COMMUNITY_MAP to existing videos. Manually-locked
        # videos are never touched.
        shift
        docker compose --profile tools run --rm tagtools \
            python -u src/apply_community_rules.py "$@"
        ;;

    test-db)
        echo "Testing MongoDB connection..."
        docker compose run --rm subtitle-generator python -c "
from src.db_manager import DatabaseManager
import yaml
with open('config.yaml') as f:
    config = yaml.safe_load(f)
db = DatabaseManager(config)
print('✓ MongoDB connection successful')
db.close()
"
        ;;

    *)
        echo "3Speak Subtitle Generator - Management Script"
        echo ""
        echo "Usage: $0 {command}"
        echo ""
        echo "Commands:"
        echo "  build       Build Docker image"
        echo "  start       Start the service"
        echo "  stop        Stop the service"
        echo "  restart     Restart the service"
        echo "  logs        View service logs"
        echo "  status      Show service status"
        echo "  clean       Clean temporary files"
        echo "  clean-all   Clean all data (models, temp, subtitles)"
        echo "  shell       Open shell in container"
        echo "  stats       Show storage statistics"
        echo "  test-db     Test MongoDB connection"
        echo ""
        echo "Re-tagging (existing videos):"
        echo "  retag-dry [N]  Preview the tag diff for N videos (default 50)"
        echo "  retag          Re-tag all stale videos, detached (survives logout)"
        echo "  retag-empty-dry  Preview re-tag of untagged videos w/ 50+ transcript words"
        echo "  retag-empty      Re-tag those untagged videos, detached (reuses local .srt)"
        echo "  retag-empty-logs Follow the detached re-tag"
        echo "  retag-empty-stop Stop it (resumable)"
        echo ""
        echo "HLS-gap backfill (native uploads the 2026-06 filename-gate bug skipped):"
        echo "  backfill-hls-gap-dry [DATE]  Preview the backlog (default since 2026-02-18)"
        echo "  backfill-hls-gap             Full transcribe/translate/tag, detached (resumable)"
        echo "  backfill-hls-gap-logs        Follow progress"
        echo "  backfill-hls-gap-status      Show status"
        echo "  backfill-hls-gap-stop        Stop it (resumable)"
        echo ""
        echo "Metadata tagging (never-transcribed back-catalog):"
        echo ""
        echo "Tag admin:"
        echo "  set-tags <video> <tags>  Manually set + lock a video's tags"
        echo "  community-rules          Apply single-topic community rules to existing videos"
        echo ""
        echo "  tag-meta-fast    Evidence-only pass (no classifier) — fast bulk tagging"
        echo "  tag-meta-dry     Preview one recent window (writes nothing)"
        echo "  tag-meta         Tag back-catalog from metadata, detached (newest first)"
        echo "  tag-meta-logs    Follow progress"
        echo "  tag-meta-status  Show status"
        echo "  tag-meta-stop    Stop (resumable)"
        echo "  retag-logs     Follow re-tag progress"
        echo "  retag-status   Show re-tagger status"
        echo "  retag-stop     Stop the re-tagger (resumable)"
        exit 1
        ;;
esac
