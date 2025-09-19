"""Herramientas para almacenar vistas previas y analisis de partidos."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, Iterable, List, Tuple

_BASE_DIR = Path(__file__).resolve().parent.parent
_STORAGE_FILE = _BASE_DIR / "preview_store.json"
_STORAGE_LOCK = Lock()


def _ensure_store_dict(raw: object) -> Dict[str, object]:
    if not isinstance(raw, dict):
        raw = {}
    matches = raw.get('matches')
    if not isinstance(matches, dict):
        raw['matches'] = {}
    return raw  # type: ignore[return-value]


def _load_store_unlocked() -> Dict[str, object]:
    if not _STORAGE_FILE.exists():
        return {'matches': {}}
    try:
        with _STORAGE_FILE.open('r', encoding='utf-8') as handler:
            data = json.load(handler)
    except Exception:
        return {'matches': {}}
    return _ensure_store_dict(data)


def _write_store_unlocked(store: Dict[str, object]) -> None:
    _STORAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with _STORAGE_FILE.open('w', encoding='utf-8') as handler:
        json.dump(store, handler, indent=2, ensure_ascii=True)


def list_previews(payload_type: str = 'preview') -> List[Dict[str, object]]:
    with _STORAGE_LOCK:
        store = _load_store_unlocked()
        matches = store.get('matches', {})
        items: List[Dict[str, object]] = []
        if isinstance(matches, dict):
            for match_id, bucket in matches.items():
                if not isinstance(bucket, dict):
                    continue
                typed_entry = bucket.get(payload_type)
                if not isinstance(typed_entry, dict):
                    continue
                payload = typed_entry.get('payload')
                payload_dict = payload if isinstance(payload, dict) else None
                items.append({
                    'match_id': str(match_id),
                    'payload_type': payload_type,
                    'stored_at': typed_entry.get('stored_at'),
                    'source': typed_entry.get('source'),
                    'payload': payload_dict,
                })
        items.sort(key=lambda item: item.get('stored_at') or '', reverse=True)
        return items


def get_preview(match_id: str, payload_type: str = 'preview') -> Dict[str, object] | None:
    match_id = str(match_id)
    with _STORAGE_LOCK:
        store = _load_store_unlocked()
        matches = store.get('matches', {})
        if not isinstance(matches, dict):
            return None
        bucket = matches.get(match_id)
        if not isinstance(bucket, dict):
            return None
        typed_entry = bucket.get(payload_type)
        if not isinstance(typed_entry, dict):
            return None
        payload = typed_entry.get('payload')
        if not isinstance(payload, dict):
            return None
        payload_copy = json.loads(json.dumps(payload))
        meta_key = '_cached_preview' if payload_type == 'preview' else '_cached_analysis'
        payload_copy[meta_key] = {
            'stored_at': typed_entry.get('stored_at'),
            'source': typed_entry.get('source'),
            'payload_type': payload_type,
        }
        return payload_copy


def upsert_previews(
    entries: Iterable[Tuple[str, Dict[str, object]]],
    source: str = 'manual_range',
    payload_type: str = 'preview'
) -> Dict[str, int]:
    timestamp = datetime.now(timezone.utc).isoformat()
    added = 0
    updated = 0
    serializable_entries: List[Tuple[str, Dict[str, object]]] = []
    for raw_match_id, payload in entries:
        match_id = str(raw_match_id).strip()
        if not match_id or not isinstance(payload, dict):
            continue
        serializable_entries.append((match_id, json.loads(json.dumps(payload))))
    if not serializable_entries:
        return {'added': added, 'updated': updated}
    with _STORAGE_LOCK:
        store = _load_store_unlocked()
        matches = store.get('matches')
        if not isinstance(matches, dict):
            matches = {}
            store['matches'] = matches
        for match_id, payload in serializable_entries:
            bucket = matches.get(match_id)
            if not isinstance(bucket, dict):
                bucket = {}
                matches[match_id] = bucket
            entry = {
                'payload': payload,
                'stored_at': timestamp,
                'source': source,
            }
            if payload_type in bucket:
                updated += 1
            else:
                added += 1
            bucket[payload_type] = entry
        store['last_updated'] = timestamp
        _write_store_unlocked(store)
    return {'added': added, 'updated': updated}


def delete_preview(match_id: str, payload_type: str = 'preview') -> bool:
    match_id = str(match_id)
    with _STORAGE_LOCK:
        store = _load_store_unlocked()
        matches = store.get('matches')
        if not isinstance(matches, dict):
            return False
        bucket = matches.get(match_id)
        if not isinstance(bucket, dict) or payload_type not in bucket:
            return False
        bucket.pop(payload_type, None)
        if not bucket:
            matches.pop(match_id, None)
        store['last_updated'] = datetime.now(timezone.utc).isoformat()
        _write_store_unlocked(store)
    return True
