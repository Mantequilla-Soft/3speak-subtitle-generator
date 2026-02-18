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
        exit 1
        ;;
esac
