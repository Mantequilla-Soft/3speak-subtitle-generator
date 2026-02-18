"""
3Speak Subtitle Dashboard
Lightweight Flask app for monitoring subtitle processing
"""

import os
import shutil
import yaml
import logging
from datetime import datetime
from flask import Flask, render_template, jsonify, request
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
priority_col = db[config['mongodb'].get('collection_priority', 'subtitles-priority')]
status_col = db[config['mongodb'].get('collection_status', 'subtitles-status')]

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

    # Total videos matching our date filter (both collections)
    legacy_query = {
        'filename': {'$exists': True, '$nin': [None, ''], '$regex': '^ipfs://'},
        'status': 'published'
    }
    embed_query = {
        'manifest_cid': {'$exists': True, '$nin': [None, '']},
        'status': 'published'
    }
    if START_DATE:
        legacy_query['created'] = {'$gte': START_DATE}
        embed_query['createdAt'] = {'$gte': START_DATE}

    total_legacy = videos_col.count_documents(legacy_query)
    total_embed = embed_col.count_documents(embed_query)
    total_available = total_legacy + total_embed

    # Processed counts by type
    processed_embed = subtitles_col.count_documents({'isEmbed': True})
    processed_legacy = total_processed - processed_embed

    pending_legacy = max(0, total_legacy - processed_legacy)
    pending_embed = max(0, total_embed - processed_embed)
    pending = pending_legacy + pending_embed

    # === Build unified recent activity list ===
    recent = []

    # 1. Currently processing video (from status collection)
    processing_doc = status_col.find_one({})
    if processing_doc:
        recent.append({
            'author': processing_doc['author'],
            'permlink': processing_doc['permlink'],
            'isEmbed': processing_doc.get('isEmbed', False),
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
        is_embed = bool(embed_col.find_one(
            {'owner': item['author'], 'permlink': item['permlink']}, {'_id': 1}
        ))
        recent.append({
            'author': item['author'],
            'permlink': item['permlink'],
            'isEmbed': is_embed,
            'state': 'prioritized',
            'languages': [],
            'lang_count': 0,
            'display_date': item.get('requested_at'),
        })

    # 3. Completed videos (last 15)
    completed_raw = list(subtitles_col.find(
        {},
        {'author': 1, 'permlink': 1, 'subtitles': 1, 'isEmbed': 1,
         'created_at': 1, 'updated_at': 1, 'video_created_at': 1,
         'processing_seconds': 1, '_id': 0}
    ).sort('updated_at', -1).limit(15))
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

    source_candidates = []
    for v in legacy_recent:
        source_candidates.append({
            'author': v['owner'], 'permlink': v['permlink'],
            'isEmbed': False, 'video_created_at': v.get('created'),
        })
    for v in embed_recent:
        source_candidates.append({
            'author': v['owner'], 'permlink': v['permlink'],
            'isEmbed': True, 'video_created_at': v.get('createdAt'),
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
        sv['state'] = 'pending'
        sv['languages'] = []
        sv['lang_count'] = 0
        sv['display_date'] = sv.get('video_created_at')
        pending_entries.append(sv)
        if len(pending_entries) >= 10:
            break
    recent.extend(pending_entries)

    system = get_system_metrics()

    return {
        'total_processed': total_processed,
        'total_subtitles': total_subtitles,
        'total_tagged': total_tagged,
        'total_available': total_available,
        'total_legacy': total_legacy,
        'total_embed': total_embed,
        'processed_legacy': processed_legacy,
        'processed_embed': processed_embed,
        'pending': pending,
        'pending_legacy': pending_legacy,
        'pending_embed': pending_embed,
        'start_date': start_date_str or 'all',
        'lang_counts': lang_counts,
        'recent': recent,
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


if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', config.get('dashboard', {}).get('port', 8090)))
    logger.info(f"Starting dashboard on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
