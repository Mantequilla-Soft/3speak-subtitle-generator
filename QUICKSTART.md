# Quick Start Guide

## Get Started in 3 Steps

### 1. Build the Docker Image
```bash
cd /home/tibfox/mantequilla/3speak-subtitles
./manage.sh build
```

This will download all required AI models (~20GB). First build takes 30-60 minutes.

### 2. Configure Languages (Optional)

Edit `config.yaml` to customize target languages:

```yaml
languages:
  - code: "en"
    name: "English"
  - code: "es"
    name: "Spanish"
  # ... add or remove languages
```

### 3. Start the Service

```bash
./manage.sh start
./manage.sh logs
```

The service will:
- Fetch videos from today from MongoDB
- Download videos from IPFS (480p)
- Generate transcriptions with Whisper
- Create subtitles in all configured languages
- Tag video content automatically
- Store everything in MongoDB
- Clean up temporary files

## What to Expect

**First Video:**
- Download: 30-60 seconds (depends on IPFS)
- Transcription: 10-15 minutes (Whisper on CPU)
- Translation: 2-4 minutes (21 languages)
- Tagging: 10-20 seconds
- Total: ~12-19 minutes

**Subsequent Videos:**
- Models are cached, so processing is faster
- Only download and compute time

## Monitoring

### View Logs
```bash
./manage.sh logs
```

### Check Status
```bash
./manage.sh status
```

### Storage Usage
```bash
./manage.sh stats
```

## Managing the Service

```bash
# Start service
./manage.sh start

# Stop service
./manage.sh stop

# Restart service
./manage.sh restart

# Clean temporary files
./manage.sh clean

# Test database connection
./manage.sh test-db
```

## Output

### Subtitle Files
Located in: `subtitles/{author}/{permlink}.{lang}.srt`

Example:
```
subtitles/
├── alice/
│   ├── my-video.en.srt
│   ├── my-video.es.srt
│   ├── my-video.fr.srt
│   └── ...
└── bob/
    └── another-video.en.srt
```

### Database Collections

**subtitles** collection:
```json
{
  "author": "alice",
  "permlink": "my-video",
  "video_cid": "Qm...",
  "language": "en",
  "subtitle_path": "/app/subtitles/alice/my-video.en.srt",
  "created_at": "2026-02-17T10:30:00Z"
}
```

**subtitles-tags** collection:
```json
{
  "author": "alice",
  "permlink": "my-video",
  "tags": "travel,vlog,adventure",
  "created_at": "2026-02-17T10:30:00Z"
}
```

## Troubleshooting

### Service won't start
```bash
./manage.sh logs
```
Check for errors in the logs.

### Out of memory
Reduce model sizes in `config.yaml`:
```yaml
models:
  whisper:
    model_size: "small"  # Instead of "medium"
```

### Can't connect to MongoDB
```bash
./manage.sh test-db
```

### IPFS downloads failing
- Service tries multiple gateways automatically
- Check network connectivity
- Verify CID exists on IPFS

## Next Steps

1. **Monitor First Run**: Watch logs to see the process
2. **Verify Output**: Check `subtitles/` directory for SRT files
3. **Query MongoDB**: Verify data in `subtitles` and `subtitles-tags` collections
4. **Customize Tags**: Edit tag list in `config.yaml` for your use case
5. **Adjust Languages**: Add/remove languages based on your audience

## Performance Tips

- **Reduce languages**: 10 languages instead of 21 = ~50% faster
- **Smaller models**: Use "small" Whisper = ~2x faster (slightly less accurate)
- **Filter by date**: Only process recent videos to avoid backlog
- **GPU support**: Add GPU to docker-compose.yml for 20-50x speed improvement

## Support

See [README.md](README.md) for detailed documentation.
