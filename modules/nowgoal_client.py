"""Utility helpers to fetch match listings from Nowgoal.

This module centralises the logic that was previously embedded in the Flask
application so that it can be reused from the Streamlit front-end.  The
implementation relies exclusively on ``requests`` and BeautifulSoup which are
compatible with Streamlit Cloud (share.streamlit.io).
"""

from __future__ import annotations

import datetime
import math
import re
import threading
from typing import Iterable

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL_NOWGOAL = "https://live20.nowgoal25.com/"
REQUEST_TIMEOUT_SECONDS = 12

_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Referer": URL_NOWGOAL,
}

_requests_session: requests.Session | None = None
_requests_session_lock = threading.Lock()


def _build_nowgoal_url(path: str | None = None) -> str:
    if not path:
        return URL_NOWGOAL
    base = URL_NOWGOAL.rstrip("/")
    suffix = path.lstrip("/")
    return f"{base}/{suffix}"


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


def _fetch_nowgoal_html(path: str | None = None) -> str | None:
    url = _build_nowgoal_url(path)
    try:
        response = _get_shared_requests_session().get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.text
    except Exception as exc:  # pragma: no cover - network failure is acceptable
        print(f"Error al obtener {url} con requests: {exc}")
        return None


def _parse_number_clean(text: str | None) -> float | None:
    if text is None:
        return None
    txt = str(text).strip()
    txt = txt.replace("−", "-")
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
    return _parse_number_clean(cleaned.replace("+", ""))


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


def parse_main_page_matches(
    html_content: str, limit: int = 20, offset: int = 0, handicap_filter: str | None = None
) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_content, "html.parser")
    match_rows = soup.find_all("tr", id=lambda value: value and value.startswith("tr1_"))
    upcoming_matches: list[dict[str, object]] = []
    now_utc = datetime.datetime.utcnow()

    for row in match_rows:
        match_id = row.get("id", "").replace("tr1_", "")
        if not match_id:
            continue

        time_cell = row.find("td", {"name": "timeData"})
        if not time_cell or not time_cell.has_attr("data-t"):
            continue

        try:
            match_time = datetime.datetime.strptime(time_cell["data-t"], "%Y-%m-%d %H:%M:%S")
        except (ValueError, IndexError):
            continue

        if match_time < now_utc:
            continue

        home_team_tag = row.find("a", {"id": f"team1_{match_id}"})
        away_team_tag = row.find("a", {"id": f"team2_{match_id}"})
        odds_data = row.get("odds", "").split(",")
        handicap = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10] if len(odds_data) > 10 else "N/A"

        if handicap == "N/A":
            continue

        upcoming_matches.append(
            {
                "id": match_id,
                "time_obj": match_time,
                "home_team": home_team_tag.text.strip() if home_team_tag else "N/A",
                "away_team": away_team_tag.text.strip() if away_team_tag else "N/A",
                "handicap": handicap,
                "goal_line": goal_line,
            }
        )

    if handicap_filter:
        try:
            target = normalize_handicap_to_half_bucket_str(handicap_filter)
            if target is not None:
                filtered = []
                for match in upcoming_matches:
                    value = normalize_handicap_to_half_bucket_str(match.get("handicap", ""))
                    if value == target:
                        filtered.append(match)
                upcoming_matches = filtered
        except Exception:
            pass

    upcoming_matches.sort(key=lambda item: item["time_obj"])
    paginated = upcoming_matches[offset : offset + limit]

    results: list[dict[str, str]] = []
    for match in paginated:
        display = dict(match)
        display["time"] = (match["time_obj"] + datetime.timedelta(hours=2)).strftime("%H:%M")
        del display["time_obj"]
        results.append(display)
    return results


def parse_main_page_finished_matches(
    html_content: str, limit: int = 20, offset: int = 0, handicap_filter: str | None = None
) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_content, "html.parser")
    match_rows = soup.find_all("tr", id=lambda value: value and value.startswith("tr1_"))
    finished_matches: list[dict[str, object]] = []

    for row in match_rows:
        match_id = row.get("id", "").replace("tr1_", "")
        if not match_id:
            continue

        state = row.get("state")
        if state is not None and state != "-1":
            continue

        cells = row.find_all("td")
        if len(cells) < 8:
            continue

        home_team_tag = row.find("a", {"id": f"team1_{match_id}"})
        away_team_tag = row.find("a", {"id": f"team2_{match_id}"})

        score_cell = cells[6]
        score_text = "N/A"
        if score_cell:
            bold = score_cell.find("b")
            if bold:
                score_text = bold.text.strip()
            else:
                score_text = score_cell.get_text(strip=True)

        if not re.match(r"^\d+\s*-\s*\d+$", score_text):
            continue

        odds_data = row.get("odds", "").split(",")
        handicap = odds_data[2] if len(odds_data) > 2 else "N/A"
        goal_line = odds_data[10] if len(odds_data) > 10 else "N/A"

        if handicap == "N/A":
            continue

        time_cell = row.find("td", {"name": "timeData"})
        match_time = datetime.datetime.now()
        if time_cell and time_cell.has_attr("data-t"):
            try:
                match_time = datetime.datetime.strptime(time_cell["data-t"], "%Y-%m-%d %H:%M:%S")
            except (ValueError, IndexError):
                continue

        finished_matches.append(
            {
                "id": match_id,
                "time_obj": match_time,
                "home_team": home_team_tag.text.strip() if home_team_tag else "N/A",
                "away_team": away_team_tag.text.strip() if away_team_tag else "N/A",
                "score": score_text,
                "handicap": handicap,
                "goal_line": goal_line,
            }
        )

    if handicap_filter:
        try:
            target = normalize_handicap_to_half_bucket_str(handicap_filter)
            if target is not None:
                filtered = []
                for match in finished_matches:
                    value = normalize_handicap_to_half_bucket_str(match.get("handicap", ""))
                    if value == target:
                        filtered.append(match)
                finished_matches = filtered
        except Exception:
            pass

    finished_matches.sort(key=lambda item: item["time_obj"], reverse=True)
    paginated = finished_matches[offset : offset + limit]

    results: list[dict[str, str]] = []
    for match in paginated:
        display = dict(match)
        display["time"] = (match["time_obj"] + datetime.timedelta(hours=2)).strftime("%d/%m %H:%M")
        del display["time_obj"]
        results.append(display)
    return results


def _ensure_positive_int(value: int, *, default: int, maximum: int = 100) -> int:
    if not isinstance(value, int) or value <= 0:
        return default
    return min(value, maximum)


def fetch_upcoming_matches(limit: int = 20, offset: int = 0, handicap_filter: str | None = None) -> list[dict[str, str]]:
    limit = _ensure_positive_int(limit, default=20, maximum=200)
    offset = max(0, offset)
    html = _fetch_nowgoal_html()
    if not html:
        return []
    matches = parse_main_page_matches(html, limit + offset, 0, handicap_filter)
    return matches[offset: offset + limit]


def fetch_finished_matches(limit: int = 20, offset: int = 0, handicap_filter: str | None = None) -> list[dict[str, str]]:
    limit = _ensure_positive_int(limit, default=20, maximum=200)
    offset = max(0, offset)
    html = _fetch_nowgoal_html("football/results")
    if not html:
        return []
    matches = parse_main_page_finished_matches(html, limit + offset, 0, handicap_filter)
    return matches[offset: offset + limit]


def collect_handicap_options(matches: Iterable[dict[str, str]]) -> list[str]:
    raw_values = {
        normalize_handicap_to_half_bucket_str(match.get("handicap"))
        for match in matches
        if normalize_handicap_to_half_bucket_str(match.get("handicap")) is not None
    }
    return sorted(raw_values, key=lambda item: float(item))

