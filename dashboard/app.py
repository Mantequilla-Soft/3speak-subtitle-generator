"""
3Speak Subtitle Dashboard
Lightweight Flask app for monitoring subtitle processing
"""

import os
import json
import socket
import struct
import shutil
import yaml
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify, request, Response
from pymongo import MongoClient
import psutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Load config
with open('/app/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# MongoDB connection
client = MongoClient(config['mongodb']['uri'], serverSelectionTimeoutMS=5000)
db = client[config['mongodb']['database']]
subtitles_col = db[config['mongodb']['collection_subtitles']]
tags_col = db[config['mongodb']['collection_tags']]
videos_col = db[config['mongodb']['collection_videos']]
embed_col = db[config['mongodb']['collection_embed']]
embed_audio_col = db[config['mongodb'].get('collection_embed_audio', 'embed-audio')]
priority_col = db[config['mongodb'].get('collection_priority', 'subtitles-priority')]
status_col = db[config['mongodb'].get('collection_status', 'subtitles-status')]
blacklist_col = db[config['mongodb'].get('collection_blacklist', 'subtitles-blacklist')]
blacklist_authors_col = db[config['mongodb'].get('collection_blacklist_authors', 'subtitles-blacklist-authors')]
priority_creators_col = db[config['mongodb'].get('collection_priority_creators', 'subtitles-priority-creators')]
hotwords_col = db[config['mongodb'].get('collection_hotwords', 'subtitles-hotwords')]
corrections_col = db[config['mongodb'].get('collection_corrections', 'subtitles-corrections')]

REFRESH_INTERVAL = config.get('dashboard', {}).get('refresh_interval', 15)
DASHBOARD_PASSWORD = config.get('dashboard', {}).get('password', '')

# Start date filter (same as subtitle-generator)
start_date_str = os.getenv('START_DATE', '')
START_DATE = datetime.strptime(start_date_str, '%Y-%m-%d') if start_date_str else None

# Prime psutil CPU measurement (first call always returns 0)
psutil.cpu_percent(interval=None)


def get_system_metrics():
    """Get host CPU, RAM, and disk usage."""
    # CPU (host-level via /proc/stat)
    cpu_percent = psutil.cpu_percent(interval=None)

    # RAM from host's /proc/meminfo (container cgroup would skew this)
    try:
        meminfo = {}
        with open('/hostfs/proc/meminfo', 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    meminfo[parts[0].rstrip(':')] = int(parts[1])
        mem_total = meminfo.get('MemTotal', 0)
        mem_available = meminfo.get('MemAvailable', 0)
        mem_used = mem_total - mem_available
        ram_percent = round((mem_used / mem_total) * 100, 1) if mem_total else 0
        ram_total_gb = round(mem_total / 1048576, 1)
        ram_used_gb = round(mem_used / 1048576, 1)
    except Exception:
        ram_percent = 0
        ram_total_gb = 0
        ram_used_gb = 0

    # Disk from host root mount
    try:
        disk = shutil.disk_usage('/hostfs')
        disk_percent = round((disk.used / disk.total) * 100, 1)
        disk_total_gb = round(disk.total / (1024 ** 3), 1)
        disk_used_gb = round(disk.used / (1024 ** 3), 1)
    except Exception:
        disk_percent = 0
        disk_total_gb = 0
        disk_used_gb = 0

    return {
        'cpu_percent': cpu_percent,
        'ram_percent': ram_percent,
        'ram_used_gb': ram_used_gb,
        'ram_total_gb': ram_total_gb,
        'disk_percent': disk_percent,
        'disk_used_gb': disk_used_gb,
        'disk_total_gb': disk_total_gb,
    }


def get_stats():
    """Gather all dashboard statistics from MongoDB."""
    # Total processed videos (documents in subtitles collection)
    total_processed = subtitles_col.count_documents({})

    # Total subtitle files (sum of all language keys across all docs)
    pipeline_total_subs = [
        {'$project': {'count': {'$size': {'$objectToArray': '$subtitles'}}}},
        {'$group': {'_id': None, 'total': {'$sum': '$count'}}}
    ]
    result = list(subtitles_col.aggregate(pipeline_total_subs))
    total_subtitles = result[0]['total'] if result else 0

    # Total tagged videos
    total_tagged = tags_col.count_documents({})

    # Language breakdown
    pipeline_langs = [
        {'$project': {'langs': {'$objectToArray': '$subtitles'}}},
        {'$unwind': '$langs'},
        {'$group': {'_id': '$langs.k', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]
    lang_counts = {doc['_id']: doc['count'] for doc in subtitles_col.aggregate(pipeline_langs)}

    # Total videos matching our date filter (all collections)
    legacy_query = {
        'filename': {'$exists': True, '$nin': [None, ''], '$regex': '^ipfs://'},
        'status': 'published'
    }
    embed_query = {
        'manifest_cid': {'$exists': True, '$nin': [None, '']},
        'status': 'published'
    }
    audio_query = {
        'audio_cid': {'$exists': True, '$nin': [None, '']},
        'status': 'published'
    }
    if START_DATE:
        legacy_query['created'] = {'$gte': START_DATE}
        embed_query['createdAt'] = {'$gte': START_DATE}
        audio_query['createdAt'] = {'$gte': START_DATE}

    total_legacy = videos_col.count_documents(legacy_query)
    total_embed = embed_col.count_documents(embed_query)
    total_audio = embed_audio_col.count_documents(audio_query)
    total_available = total_legacy + total_embed + total_audio

    # Processed counts by type
    processed_audio = subtitles_col.count_documents({'isAudio': True})
    processed_embed = subtitles_col.count_documents({'isEmbed': True, 'isAudio': {'$ne': True}})
    processed_legacy = total_processed - processed_embed - processed_audio

    pending_legacy = max(0, total_legacy - processed_legacy)
    pending_embed = max(0, total_embed - processed_embed)
    pending_audio = max(0, total_audio - processed_audio)
    pending = pending_legacy + pending_embed + pending_audio

    # === Build unified recent activity list ===
    recent = []

    # 1. Currently processing video (from status collection)
    processing_doc = status_col.find_one({})
    if processing_doc:
        recent.append({
            'author': processing_doc['author'],
            'permlink': processing_doc['permlink'],
            'isEmbed': processing_doc.get('isEmbed', False),
            'isAudio': processing_doc.get('isAudio', False),
            'state': 'processing',
            'languages': [],
            'lang_count': 0,
            'display_date': processing_doc.get('started_at'),
        })

    # 2. Priority queue items
    prio_items = list(priority_col.find(
        {}, {'author': 1, 'permlink': 1, 'requested_at': 1, '_id': 0}
    ).sort('requested_at', 1))
    prio_set = {(p['author'], p['permlink']) for p in prio_items}
    for item in prio_items:
        is_audio = bool(embed_audio_col.find_one(
            {'owner': item['author'], 'permlink': item['permlink']}, {'_id': 1}
        ))
        is_embed = is_audio or bool(embed_col.find_one(
            {'owner': item['author'], 'permlink': item['permlink']}, {'_id': 1}
        ))
        recent.append({
            'author': item['author'],
            'permlink': item['permlink'],
            'isEmbed': is_embed,
            'isAudio': is_audio,
            'state': 'prioritized',
            'languages': [],
            'lang_count': 0,
            'display_date': item.get('requested_at'),
        })

    # 3. Completed videos (last 50)
    completed_raw = list(subtitles_col.find(
        {},
        {'author': 1, 'permlink': 1, 'subtitles': 1, 'isEmbed': 1,
         'created_at': 1, 'updated_at': 1, 'video_created_at': 1,
         'processing_seconds': 1, 'video_duration_seconds': 1, 'isAudio': 1, '_id': 0}
    ).sort('updated_at', -1).limit(50))
    for doc in completed_raw:
        doc['state'] = 'complete'
        if 'subtitles' in doc:
            doc['languages'] = list(doc['subtitles'].keys())
            doc['lang_count'] = len(doc['languages'])
        else:
            doc['languages'] = []
            doc['lang_count'] = 0
        doc['display_date'] = doc.get('updated_at') or doc.get('created_at')
        recent.append(doc)

    # 4. Recent pending videos (newest from source collections, not yet processed)
    priority_creators = {
        doc['author']
        for doc in priority_creators_col.find({}, {'author': 1, '_id': 0})
    }
    processing_pair = (
        (processing_doc['author'], processing_doc['permlink'])
        if processing_doc else None
    )
    legacy_recent = list(videos_col.find(
        legacy_query,
        {'owner': 1, 'permlink': 1, 'created': 1, '_id': 0}
    ).sort('created', -1).limit(20))
    embed_recent = list(embed_col.find(
        embed_query,
        {'owner': 1, 'permlink': 1, 'createdAt': 1, '_id': 0}
    ).sort('createdAt', -1).limit(20))
    audio_recent = list(embed_audio_col.find(
        audio_query,
        {'owner': 1, 'permlink': 1, 'createdAt': 1, '_id': 0}
    ).sort('createdAt', -1).limit(20))

    source_candidates = []
    for v in legacy_recent:
        source_candidates.append({
            'author': v['owner'], 'permlink': v['permlink'],
            'isEmbed': False, 'isAudio': False, 'video_created_at': v.get('created'),
        })
    for v in embed_recent:
        source_candidates.append({
            'author': v['owner'], 'permlink': v['permlink'],
            'isEmbed': True, 'isAudio': False, 'video_created_at': v.get('createdAt'),
        })
    for v in audio_recent:
        source_candidates.append({
            'author': v['owner'], 'permlink': v['permlink'],
            'isEmbed': True, 'isAudio': True, 'video_created_at': v.get('createdAt'),
        })
    source_candidates.sort(
        key=lambda v: v.get('video_created_at') or datetime.min, reverse=True
    )

    # Batch-check which candidates are already processed
    if source_candidates:
        or_conds = [
            {'author': sv['author'], 'permlink': sv['permlink']}
            for sv in source_candidates
        ]
        processed_pairs = {
            (d['author'], d['permlink'])
            for d in subtitles_col.find({'$or': or_conds}, {'author': 1, 'permlink': 1, '_id': 0})
        }
    else:
        processed_pairs = set()

    pending_entries = []
    for sv in source_candidates:
        pair = (sv['author'], sv['permlink'])
        if pair in processed_pairs or pair in prio_set or pair == processing_pair:
            continue
        sv['state'] = 'prioritized' if sv['author'] in priority_creators else 'pending'
        sv['languages'] = []
        sv['lang_count'] = 0
        sv['display_date'] = sv.get('video_created_at')
        pending_entries.append(sv)
        if len(pending_entries) >= 50:
            break
    recent.extend(pending_entries)

    pending_items = [r for r in recent if r['state'] != 'complete']
    processed_items = [r for r in recent if r['state'] == 'complete']

    system = get_system_metrics()

    return {
        'total_processed': total_processed,
        'total_subtitles': total_subtitles,
        'total_tagged': total_tagged,
        'total_available': total_available,
        'total_legacy': total_legacy,
        'total_embed': total_embed,
        'total_audio': total_audio,
        'processed_legacy': processed_legacy,
        'processed_embed': processed_embed,
        'processed_audio': processed_audio,
        'pending': pending,
        'pending_legacy': pending_legacy,
        'pending_embed': pending_embed,
        'pending_audio': pending_audio,
        'start_date': start_date_str or 'all',
        'lang_counts': lang_counts,
        'recent': recent,
        'pending_items': pending_items,
        'processed_items': processed_items,
        'refresh_interval': REFRESH_INTERVAL,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        **system,
    }


@app.route('/')
def dashboard():
    stats = get_stats()
    return render_template('dashboard.html', **stats)


@app.route('/api/stats')
def api_stats():
    stats = get_stats()
    # Make recent serializable
    for doc in stats['recent']:
        for key in ('created_at', 'updated_at', 'video_created_at', 'display_date'):
            if key in doc and isinstance(doc[key], datetime):
                doc[key] = doc[key].isoformat()
    return jsonify(stats)


@app.route('/api/prioritize', methods=['POST'])
def api_prioritize():
    data = request.get_json(silent=True) or {}
    password = data.get('password', '')
    video_ref = data.get('video', '').strip()

    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403

    if '/' not in video_ref:
        return jsonify({'error': 'Format: author/permlink'}), 400

    author, permlink = video_ref.split('/', 1)
    if not author or not permlink:
        return jsonify({'error': 'Format: author/permlink'}), 400

    # Check video exists
    exists = (
        videos_col.find_one({'owner': author, 'permlink': permlink}, {'_id': 1})
        or embed_col.find_one({'owner': author, 'permlink': permlink}, {'_id': 1})
    )
    if not exists:
        return jsonify({'error': f'Video {author}/{permlink} not found'}), 404

    # Check not already in queue
    if priority_col.find_one({'author': author, 'permlink': permlink}):
        return jsonify({'error': 'Already in priority queue'}), 409

    priority_col.insert_one({
        'author': author,
        'permlink': permlink,
        'requested_at': datetime.now(),
    })
    logger.info(f"Priority request added: {author}/{permlink}")
    return jsonify({'ok': True, 'author': author, 'permlink': permlink})


@app.route('/api/priority')
def api_priority():
    items = list(priority_col.find(
        {}, {'author': 1, 'permlink': 1, 'requested_at': 1, '_id': 1}
    ).sort('requested_at', 1))
    for item in items:
        item['_id'] = str(item['_id'])
        if isinstance(item.get('requested_at'), datetime):
            item['requested_at'] = item['requested_at'].isoformat()
    return jsonify(items)


@app.route('/api/priority/<item_id>', methods=['DELETE'])
def api_priority_delete(item_id):
    from bson import ObjectId
    password = request.args.get('password', '')
    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403
    result = priority_col.delete_one({'_id': ObjectId(item_id)})
    if result.deleted_count:
        return jsonify({'ok': True})
    return jsonify({'error': 'Not found'}), 404


# --- Blacklist endpoints ---

@app.route('/api/blacklist', methods=['POST'])
def api_blacklist_add():
    data = request.get_json(silent=True) or {}
    password = data.get('password', '')
    video_ref = data.get('video', '').strip()

    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403

    if '/' not in video_ref:
        return jsonify({'error': 'Format: author/permlink'}), 400

    author, permlink = video_ref.split('/', 1)
    if not author or not permlink:
        return jsonify({'error': 'Format: author/permlink'}), 400

    if blacklist_col.find_one({'author': author, 'permlink': permlink}):
        return jsonify({'error': 'Already blacklisted'}), 409

    blacklist_col.insert_one({
        'author': author,
        'permlink': permlink,
        'added_at': datetime.now(),
    })
    logger.info(f"Blacklisted: {author}/{permlink}")
    return jsonify({'ok': True, 'author': author, 'permlink': permlink})


@app.route('/api/blacklist')
def api_blacklist_list():
    items = list(blacklist_col.find(
        {}, {'author': 1, 'permlink': 1, 'added_at': 1, '_id': 1}
    ).sort('added_at', -1))
    for item in items:
        item['_id'] = str(item['_id'])
        if isinstance(item.get('added_at'), datetime):
            item['added_at'] = item['added_at'].isoformat()
    return jsonify(items)


@app.route('/api/blacklist/<item_id>', methods=['DELETE'])
def api_blacklist_delete(item_id):
    from bson import ObjectId
    password = request.args.get('password', '')
    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403
    result = blacklist_col.delete_one({'_id': ObjectId(item_id)})
    if result.deleted_count:
        return jsonify({'ok': True})
    return jsonify({'error': 'Not found'}), 404


# --- Blacklist Authors endpoints ---

@app.route('/api/blacklist-authors', methods=['POST'])
def api_blacklist_authors_add():
    data = request.get_json(silent=True) or {}
    password = data.get('password', '')
    author = data.get('author', '').strip()

    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403

    if not author:
        return jsonify({'error': 'Author is required'}), 400

    if blacklist_authors_col.find_one({'author': author}):
        return jsonify({'error': 'Author already blacklisted'}), 409

    blacklist_authors_col.insert_one({
        'author': author,
        'added_at': datetime.now(),
    })
    logger.info(f"Blacklisted author: {author}")
    return jsonify({'ok': True, 'author': author})


@app.route('/api/blacklist-authors')
def api_blacklist_authors_list():
    items = list(blacklist_authors_col.find(
        {}, {'author': 1, 'added_at': 1, '_id': 1}
    ).sort('added_at', -1))
    for item in items:
        item['_id'] = str(item['_id'])
        if isinstance(item.get('added_at'), datetime):
            item['added_at'] = item['added_at'].isoformat()
    return jsonify(items)


@app.route('/api/blacklist-authors/<item_id>', methods=['DELETE'])
def api_blacklist_authors_delete(item_id):
    from bson import ObjectId
    password = request.args.get('password', '')
    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403
    result = blacklist_authors_col.delete_one({'_id': ObjectId(item_id)})
    if result.deleted_count:
        return jsonify({'ok': True})
    return jsonify({'error': 'Not found'}), 404


# --- Priority Creators endpoints ---

@app.route('/api/priority-creators', methods=['POST'])
def api_priority_creators_add():
    data = request.get_json(silent=True) or {}
    password = data.get('password', '')
    author = data.get('author', '').strip()

    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403

    if not author:
        return jsonify({'error': 'Author is required'}), 400

    if priority_creators_col.find_one({'author': author}):
        return jsonify({'error': 'Author already prioritized'}), 409

    priority_creators_col.insert_one({
        'author': author,
        'added_at': datetime.now(),
    })
    logger.info(f"Priority creator added: {author}")
    return jsonify({'ok': True, 'author': author})


@app.route('/api/priority-creators')
def api_priority_creators_list():
    items = list(priority_creators_col.find(
        {}, {'author': 1, 'added_at': 1, '_id': 1}
    ).sort('added_at', -1))
    for item in items:
        item['_id'] = str(item['_id'])
        if isinstance(item.get('added_at'), datetime):
            item['added_at'] = item['added_at'].isoformat()
    return jsonify(items)


@app.route('/api/priority-creators/<item_id>', methods=['DELETE'])
def api_priority_creators_delete(item_id):
    from bson import ObjectId
    password = request.args.get('password', '')
    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403
    result = priority_creators_col.delete_one({'_id': ObjectId(item_id)})
    if result.deleted_count:
        return jsonify({'ok': True})
    return jsonify({'error': 'Not found'}), 404


# --- Hotwords endpoints ---

@app.route('/api/hotwords', methods=['POST'])
def api_hotwords_add():
    data = request.get_json(silent=True) or {}
    password = data.get('password', '')
    word = data.get('word', '').strip()

    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403

    if not word:
        return jsonify({'error': 'Word is required'}), 400

    if hotwords_col.find_one({'word': word}):
        return jsonify({'error': 'Word already exists'}), 409

    hotwords_col.insert_one({
        'word': word,
        'added_at': datetime.now(),
    })
    logger.info(f"Hotword added: {word}")
    return jsonify({'ok': True, 'word': word})


@app.route('/api/hotwords')
def api_hotwords_list():
    items = list(hotwords_col.find(
        {}, {'word': 1, 'added_at': 1, '_id': 1}
    ).sort('added_at', -1))
    for item in items:
        item['_id'] = str(item['_id'])
        if isinstance(item.get('added_at'), datetime):
            item['added_at'] = item['added_at'].isoformat()
    return jsonify(items)


@app.route('/api/hotwords/<item_id>', methods=['DELETE'])
def api_hotwords_delete(item_id):
    from bson import ObjectId
    password = request.args.get('password', '')
    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403
    result = hotwords_col.delete_one({'_id': ObjectId(item_id)})
    if result.deleted_count:
        return jsonify({'ok': True})
    return jsonify({'error': 'Not found'}), 404


# --- Corrections endpoints ---

@app.route('/api/corrections', methods=['POST'])
def api_corrections_add():
    data = request.get_json(silent=True) or {}
    password = data.get('password', '')
    from_text = data.get('from', '').strip()
    to_text = data.get('to', '').strip()

    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403

    if not from_text or not to_text:
        return jsonify({'error': 'Both "from" and "to" are required'}), 400

    if corrections_col.find_one({'from_text': from_text}):
        return jsonify({'error': 'Correction already exists'}), 409

    corrections_col.insert_one({
        'from_text': from_text,
        'to_text': to_text,
        'added_at': datetime.now(),
    })
    logger.info(f"Correction added: {from_text} -> {to_text}")
    return jsonify({'ok': True, 'from': from_text, 'to': to_text})


@app.route('/api/corrections')
def api_corrections_list():
    items = list(corrections_col.find(
        {}, {'from_text': 1, 'to_text': 1, 'added_at': 1, '_id': 1}
    ).sort('added_at', -1))
    for item in items:
        item['_id'] = str(item['_id'])
        if isinstance(item.get('added_at'), datetime):
            item['added_at'] = item['added_at'].isoformat()
    return jsonify(items)


@app.route('/api/corrections/<item_id>', methods=['DELETE'])
def api_corrections_delete(item_id):
    from bson import ObjectId
    password = request.args.get('password', '')
    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403
    result = corrections_col.delete_one({'_id': ObjectId(item_id)})
    if result.deleted_count:
        return jsonify({'ok': True})
    return jsonify({'error': 'Not found'}), 404


# --- Reprocess endpoint ---

@app.route('/api/reprocess', methods=['POST'])
def api_reprocess():
    data = request.get_json(silent=True) or {}
    password = data.get('password', '')
    video_ref = data.get('video', '').strip()

    if not DASHBOARD_PASSWORD or password != DASHBOARD_PASSWORD:
        return jsonify({'error': 'Invalid password'}), 403

    if '/' not in video_ref:
        return jsonify({'error': 'Format: author/permlink'}), 400

    author, permlink = video_ref.split('/', 1)
    if not author or not permlink:
        return jsonify({'error': 'Format: author/permlink'}), 400

    # Delete existing subtitle document
    result = subtitles_col.delete_one({'author': author, 'permlink': permlink})
    deleted = result.deleted_count > 0

    # Delete existing tags
    tags_col.delete_one({'author': author, 'permlink': permlink})

    # Add to priority queue (skip if already queued)
    if not priority_col.find_one({'author': author, 'permlink': permlink}):
        priority_col.insert_one({
            'author': author,
            'permlink': permlink,
            'requested_at': datetime.now(),
        })

    logger.info(f"Reprocess requested: {author}/{permlink} (subtitles deleted: {deleted})")
    return jsonify({'ok': True, 'author': author, 'permlink': permlink, 'deleted': deleted})


@app.route('/api/processed')
def api_processed():
    page = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 50)), 100)
    skip = (page - 1) * per_page

    docs = list(subtitles_col.find(
        {},
        {'author': 1, 'permlink': 1, 'subtitles': 1, 'isEmbed': 1,
         'created_at': 1, 'updated_at': 1, 'video_created_at': 1,
         'processing_seconds': 1, 'video_duration_seconds': 1, 'isAudio': 1, '_id': 0}
    ).sort('updated_at', -1).skip(skip).limit(per_page))

    items = []
    for doc in docs:
        if 'subtitles' in doc:
            doc['languages'] = list(doc['subtitles'].keys())
            del doc['subtitles']
        else:
            doc['languages'] = []
        doc['display_date'] = doc.get('updated_at') or doc.get('created_at')
        for key in ('created_at', 'updated_at', 'video_created_at', 'display_date'):
            if key in doc and isinstance(doc[key], datetime):
                doc[key] = doc[key].isoformat()
        items.append(doc)

    total = subtitles_col.count_documents({})
    return jsonify({
        'items': items,
        'page': page,
        'has_more': skip + len(items) < total,
    })


DOCKER_SOCK = '/var/run/docker.sock'
BACKEND_CONTAINER = '3speak-subtitle-generator'


@app.route('/api/logs/stream')
def api_logs_stream():
    """SSE endpoint streaming backend container logs (tail 50 + follow)."""
    def generate():
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(DOCKER_SOCK)
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return

        path = (
            f'/containers/{BACKEND_CONTAINER}/logs'
            '?stdout=1&stderr=1&tail=50&follow=1'
        )
        sock.sendall(
            f'GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n'.encode()
        )

        # Read HTTP response headers
        header_buf = b''
        while b'\r\n\r\n' not in header_buf:
            chunk = sock.recv(4096)
            if not chunk:
                yield f"data: {json.dumps({'error': 'Connection closed'})}\n\n"
                sock.close()
                return
            header_buf += chunk

        status_line = header_buf[:header_buf.index(b'\r\n')].decode()
        if ' 200 ' not in status_line:
            yield f"data: {json.dumps({'error': status_line})}\n\n"
            sock.close()
            return

        idx = header_buf.index(b'\r\n\r\n') + 4
        buf = header_buf[idx:]

        # Parse Docker multiplexed stream frames (8-byte header + payload)
        while True:
            while len(buf) >= 8:
                frame_size = struct.unpack('>I', buf[4:8])[0]
                total = 8 + frame_size
                if len(buf) < total:
                    break
                line = buf[8:total].decode('utf-8', errors='replace').rstrip('\n')
                buf = buf[total:]
                yield f"data: {json.dumps(line)}\n\n"

            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf += chunk
            except Exception:
                break

        sock.close()

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', config.get('dashboard', {}).get('port', 8090)))
    logger.info(f"Starting dashboard on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
