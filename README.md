# 3Speak Subtitle Generator

Automated subtitle generation service for 3Speak videos using AI-powered transcription and translation.

## Features

- **Multi-language Support**: Generate subtitles in 21+ configurable languages
- **AI-Powered Transcription**: Uses Whisper for accurate speech-to-text
- **Smart Translation**: NLLB-200 model for high-quality translations
- **Content Tagging**: Automatically categorizes videos with relevant tags
- **Timestamp Preservation**: Ensures perfect sync across all languages
- **IPFS Integration**: Downloads videos directly from IPFS
- **MongoDB Storage**: Stores subtitle metadata and tags
- **Automatic Cleanup**: Removes temporary files after processing

## System Requirements

### Option B (CPU-Only) - Current Configuration

- **RAM**: 16GB minimum (20GB recommended)
- **Storage**: 25GB + video storage
- **CPU**: 8+ cores (Ryzen 7/i7 or better)
- **Processing Time**: ~12-19 minutes per video

### Resource Breakdown

- Whisper Medium: ~5GB RAM
- NLLB Translation: ~6-8GB RAM
- Tagging Model: ~2GB RAM
- System Overhead: ~3GB RAM

## Quick Start

### 1. Build and Run with Docker

```bash
cd /home/tibfox/mantequilla/3speak-subtitles

# Build the Docker image
docker-compose build

# Run the service
docker-compose up
```

### 2. Configuration

Edit `config.yaml` to customize:

- **Languages**: Add/remove target languages
- **Tags**: Customize content classification tags
- **Processing**: Adjust batch size, date filtering
- **Models**: Change model sizes for performance tuning

### 3. Monitor Logs

```bash
docker-compose logs -f
```

## Configuration

### Language Configuration

Edit the `languages` section in `config.yaml`:

```yaml
languages:
  - code: "en"
    name: "English"
  - code: "es"
    name: "Spanish"
  # Add more languages...
```

### Supported Languages (21 Total)

English, Spanish, French, German, Portuguese, Russian, Japanese, Chinese, Arabic, Hindi, Korean, Italian, Turkish, Vietnamese, Polish, Ukrainian, Dutch, Thai, Indonesian, Bengali, Greek

### Content Tags

Customize the `tags` section for video categorization:

```yaml
tags:
  - "food"
  - "travel"
  - "vlog"
  - "tutorial"
  # Add more tags...
```

## Architecture

### Component Overview

```
┌─────────────────┐
│   MongoDB       │ ← Videos, Subtitles, Tags
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Main Service   │
└────────┬────────┘
         │
         ├──→ IPFS Fetcher ──→ Download Videos
         │
         ├──→ Transcriber ──→ Whisper (Speech-to-Text)
         │
         ├──→ Tagger ──→ Zero-Shot Classification
         │
         ├──→ Translator ──→ NLLB-200 (Multi-language)
         │
         └──→ Subtitle Generator ──→ SRT Files
```

### Data Flow

1. **Fetch**: Get videos from MongoDB (today's videos by default)
2. **Download**: Retrieve video from IPFS (480p preferred)
3. **Transcribe**: Extract audio and generate timestamped transcript
4. **Tag**: Analyze content and assign relevant tags
5. **Translate**: Generate subtitles in all target languages
6. **Save**: Store SRT files and metadata in MongoDB
7. **Cleanup**: Remove temporary video files

## Database Collections

### `videos` Collection
- Input: Video metadata with CIDs
- Fields: `author`, `permlink`, `cid`, `created_at`

### `subtitles` Collection
- Output: Subtitle metadata
- Fields:
  - `author`: Video author
  - `permlink`: Video identifier
  - `video_cid`: IPFS CID
  - `language`: Language code (ISO 639-1)
  - `subtitle_path`: Path to SRT file
  - `created_at`: Timestamp

### `subtitles-tags` Collection
- Output: Video content tags
- Fields:
  - `author`: Video author
  - `permlink`: Video identifier
  - `tags`: Comma-separated tag list
  - `created_at`: Timestamp

## Subtitle Format

Generated subtitles use standard SRT format:

```srt
1
00:00:00,000 --> 00:00:05,000
First subtitle text

2
00:00:05,000 --> 00:00:10,000
Second subtitle text
```

### Timestamp Preservation

The translation pipeline ensures timestamps are **never modified**:

1. Whisper generates segments with timestamps
2. Only text content is translated
3. Original timestamps are preserved exactly
4. Validation checks ensure format integrity

## File Structure

```
3speak-subtitles/
├── config.yaml           # Configuration file
├── docker-compose.yml    # Docker setup
├── Dockerfile           # Container definition
├── requirements.txt     # Python dependencies
├── README.md           # This file
├── src/                # Source code
│   ├── main.py         # Main orchestrator
│   ├── db_manager.py   # MongoDB operations
│   ├── ipfs_fetcher.py # IPFS downloads
│   ├── transcriber.py  # Whisper transcription
│   ├── translator.py   # NLLB translation
│   ├── tagger.py       # Content tagging
│   └── subtitle_generator.py  # SRT generation
├── models/             # AI models cache (volume)
├── temp/              # Temporary video files (volume)
└── subtitles/         # Generated SRT files (volume)
    └── {author}/
        └── {permlink}.{lang}.srt
```

## Performance Tuning

### Speed vs Quality Trade-offs

**For Faster Processing:**
```yaml
models:
  whisper:
    model_size: "small"  # Faster, less accurate
  translation:
    model: "facebook/nllb-200-distilled-600M"  # Already optimal
```

**For Better Quality:**
```yaml
models:
  whisper:
    model_size: "large-v3"  # Slower, more accurate (requires 20GB+ RAM)
```

### Reduce Languages

Processing 10 languages instead of 21 saves ~50% translation time:

```yaml
languages:
  - code: "en"
    name: "English"
  # ... keep only top 10 languages
```

## Troubleshooting

### Out of Memory

**Solution**: Reduce model sizes in `config.yaml`

```yaml
models:
  whisper:
    model_size: "small"  # Use smaller model
```

### Slow Processing

**Expected**: 12-19 minutes per video on CPU
**Solution**: Add GPU support or reduce language count

### IPFS Download Fails

**Solution**: Service automatically tries multiple gateways
- Check gateway availability
- Verify CID is valid
- Check network connectivity

### MongoDB Connection Failed

**Solution**: Verify connection string in `config.yaml`
```bash
docker-compose logs | grep MongoDB
```

## Development

### Running Without Docker

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run service
python src/main.py
```

### Testing Individual Components

```python
# Test transcription
from transcriber import Transcriber
transcriber = Transcriber(config)
segments, lang = transcriber.transcribe("video.mp4")

# Test translation
from translator import Translator
translator = Translator(config)
translated = translator.translate_segments(segments, "en", "es")
```

## Monitoring

### View Service Logs
```bash
docker-compose logs -f subtitle-generator
```

### Check Processing Status
```bash
# MongoDB query
db.subtitles.countDocuments({created_at: {$gte: new Date('2026-02-17')}})
```

### Disk Usage
```bash
# Check subtitle storage
du -sh /home/tibfox/mantequilla/3speak-subtitles/subtitles

# Check temp directory
du -sh /home/tibfox/mantequilla/3speak-subtitles/temp
```

## Future Enhancements

- [ ] GPU support for faster processing
- [ ] Parallel video processing
- [ ] Web UI for monitoring
- [ ] Quality metrics and validation
- [ ] Custom vocabulary for better accuracy

## License

MIT License

## Support

For issues or questions, contact the 3Speak development team.
