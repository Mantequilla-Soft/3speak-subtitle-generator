"""
One-off backfill: for every embed video with a hive_permlink, mirror existing
subtitles under that hive_permlink so the public API (which uses hive_permlink
in URLs) resolves.

- Creates symlinks: /subtitles/<owner>/<hive_permlink>.<lang>.srt -> <embed_permlink>.<lang>.srt
- Mirrors MongoDB doc: subtitles collection gets a sibling doc keyed by hive_permlink
  (marked with is_alias=true and embed_permlink linking back)

Idempotent: re-running is safe.
"""
from pymongo import MongoClient
from datetime import datetime
from pathlib import Path
import yaml, os

CONFIG_PATH = '/app/config.yaml'
SUBTITLES_DIR = Path('/app/subtitles')

with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

db = MongoClient(cfg['mongodb']['uri'])[cfg['mongodb']['database']]
subs_col = db[cfg['mongodb']['collection_subtitles']]


def backfill_collection(col_name: str, is_audio: bool):
    col = db[col_name]
    cursor = col.find(
        {'hive_permlink': {'$nin': [None, '']}},
        {'owner': 1, 'permlink': 1, 'hive_permlink': 1, '_id': 0},
    )

    file_count = 0
    mongo_count = 0
    skipped_same = 0

    for v in cursor:
        owner = v['owner']
        embed_pl = v['permlink']
        hive_pl = v['hive_permlink']
        if hive_pl == embed_pl:
            skipped_same += 1
            continue

        # 1) Filesystem symlinks
        author_dir = SUBTITLES_DIR / owner
        if author_dir.exists():
            for srt in author_dir.glob(f"{embed_pl}.*.srt"):
                lang = srt.name[len(embed_pl) + 1:-4]
                alias = author_dir / f"{hive_pl}.{lang}.srt"
                if alias.exists() or alias.is_symlink():
                    continue
                try:
                    os.symlink(srt.name, alias)
                    file_count += 1
                except OSError as e:
                    print(f"  symlink failed {owner}/{hive_pl}.{lang}: {e}")

        # 2) Mongo mirror
        src = subs_col.find_one({'author': owner, 'permlink': embed_pl})
        if src and src.get('subtitles'):
            existing = subs_col.find_one({'author': owner, 'permlink': hive_pl})
            if not existing:
                mirror = {
                    'author': owner,
                    'permlink': hive_pl,
                    'video_cid': src.get('video_cid'),
                    'subtitles': src['subtitles'],
                    'isEmbed': True,
                    'isAudio': is_audio,
                    'created_at': src.get('created_at', datetime.utcnow()),
                    'updated_at': datetime.utcnow(),
                    'is_alias': True,
                    'embed_permlink': embed_pl,
                }
                if src.get('video_created_at'):
                    mirror['video_created_at'] = src['video_created_at']
                subs_col.insert_one(mirror)
                mongo_count += 1

    print(f"[{col_name}] symlinks created: {file_count}, mongo mirrors created: {mongo_count}, skipped (hive=embed): {skipped_same}")


print(f"Subtitles dir: {SUBTITLES_DIR}, exists: {SUBTITLES_DIR.exists()}")
backfill_collection('embed-video', is_audio=False)
backfill_collection('embed-audio', is_audio=True)
print("Done.")
