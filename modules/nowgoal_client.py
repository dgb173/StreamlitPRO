from __future__ import annotations

import datetime as dt
import math
import re
import threading
from typing import Any, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from zoneinfo import ZoneInfo

URL_NOWGOAL_BASE = "https://live20.nowgoal25.com"
BF_DATA_PATH = "/gf/data/bf_en-idn.js"
REQUEST_TIMEOUT_SECONDS = 12
_CACHE_TTL_SECONDS = 60

SPAIN_TZ = ZoneInfo('Europe/Madrid')

_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Referer": URL_NOWGOAL_BASE,
}

_requests_session: requests.Session | None = None
_requests_session_lock = threading.Lock()
_BF_CACHE: dict[str, Any] = {"timestamp": None, "entries": None}


def _get_shared_requests_session() -> requests.Session:
    global _requests_session
    with _requests_session_lock:
        if _requests_session is None:
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=0.4, status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(max_retries=retries)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update(_REQUEST_HEADERS)
            _requests_session = session
        return _requests_session


def _download_bf_js() -> str | None:
    url = f"{URL_NOWGOAL_BASE}{BF_DATA_PATH}"
    try:
        resp = _get_shared_requests_session().get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:  # pragma: no cover - network uncertainty
        print(f"Error al descargar {url}: {exc}")
        return None


def _fetch_bf_dataset() -> list[dict[str, Any]]:
    now = dt.datetime.utcnow()
    cached_timestamp = _BF_CACHE.get("timestamp")
    cached_entries = _BF_CACHE.get("entries")
    if cached_entries is not None and cached_timestamp is not None:
        if (now - cached_timestamp).total_seconds() < _CACHE_TTL_SECONDS:
            return cached_entries  # type: ignore[return-value]

    raw = _download_bf_js()
    if raw is None:
        return cached_entries or []

    entries = _parse_bf_dataset(raw)
    _BF_CACHE["timestamp"] = now
    _BF_CACHE["entries"] = entries
    return entries


def _parse_bf_dataset(text: str) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("A[") or "]=[" not in line:
            continue
        try:
            start = line.index("]=[") + 3
            end = line.rindex("]")
        except ValueError:
            continue
        payload = line[start:end]
        values = _parse_js_array(payload)
        entry = _build_entry(values)
        if entry is not None:
            matches.append(entry)
    return matches


def _parse_js_array(entry: str) -> list[Any]:
    values: list[tuple[str, Any]] = []
    token = ""
    in_string = False
    escaped = False

    for ch in entry:
        if in_string:
            if escaped:
                token += ch
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == "'":
                values.append(("str", token))
                token = ""
                in_string = False
            else:
                token += ch
        else:
            if ch == "'":
                in_string = True
            elif ch == ',':
                values.append(("token", token.strip()))
                token = ""
            else:
                token += ch
    if in_string:
        values.append(("str", token))
    else:
        values.append(("token", token.strip()))

    result: list[Any] = []
    for kind, raw in values:
        if kind == "str":
            result.append(raw)
            continue
        if raw in ("", "undefined"):
            result.append(None)
            continue
        if raw in ("True", "False"):
            result.append(raw == "True")
            continue
        try:
            if "." in raw:
                result.append(float(raw))
            else:
                result.append(int(raw))
        except Exception:
            result.append(raw)
    return result


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return None


def _clean_team_name(value: Any) -> str:
    if not isinstance(value, str):
        return "-"
    cleaned = re.sub(r"<[^>]+>", "", value)
    cleaned = cleaned.replace("&nbsp;", " ").strip()
    return cleaned or "-"


def _build_entry(values: list[Any]) -> dict[str, Any] | None:
    if len(values) < 9:
        return None

    match_id_raw = values[0]
    home_raw = values[4] if len(values) > 4 else None
    away_raw = values[6] if len(values) > 6 else None
    kickoff_raw = values[8]

    try:
        match_id = str(int(match_id_raw))
    except (TypeError, ValueError):
        return None

    home_team = _clean_team_name(home_raw)
    away_team = _clean_team_name(away_raw)

    if not isinstance(kickoff_raw, str):
        return None
    try:
        match_time = dt.datetime.strptime(kickoff_raw.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

    status = _coerce_int(values[19] if len(values) > 19 else None) or 0
    home_score = _coerce_int(values[12] if len(values) > 12 else None)
    away_score = _coerce_int(values[13] if len(values) > 13 else None)
    handicap_value = _coerce_float(values[28] if len(values) > 28 else None)
    goal_line_value = _coerce_float(values[34] if len(values) > 34 else None)

    return {
        "id": match_id,
        "match_time": match_time,
        "status": status,
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "handicap": handicap_value,
        "goal_line": goal_line_value,
    }


def _format_match_time(match_time: dt.datetime) -> str:
    try:
        if match_time.tzinfo is None:
            aware = match_time.replace(tzinfo=dt.timezone.utc)
        else:
            aware = match_time.astimezone(dt.timezone.utc)
        local_time = aware.astimezone(SPAIN_TZ)
    except Exception:
        local_time = match_time
    return local_time.strftime("%d/%m %H:%M")


def _format_line(value: float | int | str | None) -> str:
    if value is None:
        return "-"
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or "-"
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):  # pragma: no cover - defensive
            return "-"
        return f"{value:g}"
    return str(value)


def _serialize_match(entry: dict[str, Any], *, include_score: bool) -> dict[str, str]:
    display_time = _format_match_time(entry["match_time"])
    payload: dict[str, str] = {
        "id": entry["id"],
        "time": display_time,
        "home_team": entry["home_team"],
        "away_team": entry["away_team"],
        "handicap": _format_line(entry.get("handicap")),
        "goal_line": _format_line(entry.get("goal_line")),
    }
    if include_score:
        home_score = entry.get("home_score")
        away_score = entry.get("away_score")
        if home_score is not None and away_score is not None:
            payload["score"] = f"{home_score}-{away_score}"
    return payload


def fetch_upcoming_matches(
    limit: int = 20,
    offset: int = 0,
    handicap_filter: str | None = None,
) -> list[dict[str, str]]:
    limit = _ensure_positive_int(limit, default=20, maximum=200)
    offset = max(0, offset)
    dataset = _fetch_bf_dataset()
    now = dt.datetime.utcnow()

    candidates = [
        entry
        for entry in dataset
        if entry["status"] == 0 or entry["match_time"] >= now
    ]
    candidates.sort(key=lambda item: item["match_time"])

    if handicap_filter:
        target = normalize_handicap_to_half_bucket_str(handicap_filter)
        if target is not None:
            filtered: list[dict[str, Any]] = []
            for entry in candidates:
                normalized = normalize_handicap_to_half_bucket_str(
                    _format_line(entry.get("handicap"))
                )
                if normalized == target:
                    filtered.append(entry)
            candidates = filtered

    sliced = candidates[offset : offset + limit]
    return [_serialize_match(entry, include_score=False) for entry in sliced]


def fetch_finished_matches(
    limit: int = 20,
    offset: int = 0,
    handicap_filter: str | None = None,
) -> list[dict[str, str]]:
    limit = _ensure_positive_int(limit, default=20, maximum=200)
    offset = max(0, offset)
    dataset = _fetch_bf_dataset()
    now = dt.datetime.utcnow()

    candidates = [
        entry
        for entry in dataset
        if entry["status"] in {1, 2, 3, 4}
        and entry.get("home_score") is not None
        and entry.get("away_score") is not None
        and entry["match_time"] <= now
    ]
    candidates.sort(key=lambda item: item["match_time"], reverse=True)

    if handicap_filter:
        target = normalize_handicap_to_half_bucket_str(handicap_filter)
        if target is not None:
            filtered: list[dict[str, Any]] = []
            for entry in candidates:
                normalized = normalize_handicap_to_half_bucket_str(
                    _format_line(entry.get("handicap"))
                )
                if normalized == target:
                    filtered.append(entry)
            candidates = filtered

    sliced = candidates[offset : offset + limit]
    return [_serialize_match(entry, include_score=True) for entry in sliced]


def collect_handicap_options(matches: Iterable[dict[str, str]]) -> list[str]:
    raw_values = {
        normalize_handicap_to_half_bucket_str(match.get("handicap"))
        for match in matches
        if normalize_handicap_to_half_bucket_str(match.get("handicap")) is not None
    }
    return sorted(raw_values, key=lambda item: float(item))


def _ensure_positive_int(value: int, *, default: int, maximum: int = 100) -> int:
    if not isinstance(value, int) or value <= 0:
        return default
    return min(value, maximum)


def _parse_number_clean(text: str | None) -> float | None:
    if text is None:
        return None
    txt = str(text).strip()
    txt = txt.replace(",", ".")
    txt = txt.replace("+", "")
    txt = txt.replace(" ", "")
    match = re.search(r"^[+-]?\d+(?:\.\d+)?$", txt)
    if match:
        try:
            return float(match.group(0))
        except ValueError:
            return None
    return None


def _parse_handicap_to_float(text: str | None) -> float | None:
    if text is None:
        return None
    cleaned = str(text).strip()
    if "/" in cleaned:
        parts = [p for p in re.split(r"/", cleaned) if p]
        numbers: list[float] = []
        for part in parts:
            value = _parse_number_clean(part)
            if value is None:
                return None
            numbers.append(value)
        if not numbers:
            return None
        return sum(numbers) / len(numbers)
    return _parse_number_clean(cleaned)


def _bucket_to_half(value: float | None) -> float | None:
    if value is None:
        return None
    if value == 0:
        return 0.0
    sign = -1.0 if value < 0 else 1.0
    abs_value = abs(value)
    base = math.floor(abs_value + 1e-9)
    fraction = abs_value - base

    def close(a: float, b: float) -> bool:
        return abs(a - b) < 1e-6

    if close(fraction, 0.0):
        bucket = float(base)
    elif close(fraction, 0.5) or close(fraction, 0.25) or close(fraction, 0.75):
        bucket = base + 0.5
    else:
        bucket = round(abs_value * 2) / 2.0
        fraction = bucket - math.floor(bucket)
        if close(fraction, 0.0) and (
            abs(abs_value - (math.floor(bucket) + 0.25)) < 0.26
            or abs(abs_value - (math.floor(bucket) + 0.75)) < 0.26
        ):
            bucket = math.floor(bucket) + 0.5
    return sign * bucket


def normalize_handicap_to_half_bucket_str(text: str | None) -> str | None:
    value = _parse_handicap_to_float(text)
    if value is None:
        return None
    bucket = _bucket_to_half(value)
    if bucket is None:
        return None
    return f"{bucket:.1f}"
