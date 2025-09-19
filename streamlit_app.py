from __future__ import annotations

from pathlib import Path
import datetime as dt
from typing import Any, Iterable

import pandas as pd
import streamlit as st
from jinja2 import Environment, FileSystemLoader, select_autoescape
from streamlit.components.v1 import html as components_html
from zoneinfo import ZoneInfo

from modules.nowgoal_client import (
    collect_handicap_options,
    fetch_finished_matches,
    fetch_upcoming_matches,
)
from modules.estudio_scraper import (
    format_ah_as_decimal_string_of,
    generar_analisis_mercado_simplificado,
    obtener_datos_completos_partido,
    obtener_datos_preview_ligero,
)
from modules.preview_storage import delete_preview, list_previews, upsert_previews


PAGE_TITLE = "Analizador Profesional de Partidos"
PAGE_ICON = "AP"
MAX_MATCHES = 50
DEFAULT_MATCH_COUNT = 20

PREVIEW_TZ = ZoneInfo("Europe/Madrid")


st.set_page_config(page_title=PAGE_TITLE, page_icon=PAGE_ICON, layout="wide")


TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)


def _render_template(name: str, **context: Any) -> str:
    template = _jinja_env.get_template(name)
    return template.render(**context)


@st.cache_data(ttl=120, show_spinner=False)
def _cached_upcoming_matches(limit: int, handicap_filter: str | None) -> list[dict[str, Any]]:
    return fetch_upcoming_matches(limit=limit, offset=0, handicap_filter=handicap_filter)


@st.cache_data(ttl=120, show_spinner=False)
def _cached_finished_matches(limit: int, handicap_filter: str | None) -> list[dict[str, Any]]:
    return fetch_finished_matches(limit=limit, offset=0, handicap_filter=handicap_filter)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_handicap_options(view: str) -> list[str]:
    if view == "finished":
        sample_matches = fetch_finished_matches(limit=200, offset=0, handicap_filter=None)
    else:
        sample_matches = fetch_upcoming_matches(limit=200, offset=0, handicap_filter=None)
    return collect_handicap_options(sample_matches)


def _get_filter_values(view: str, options: list[str]) -> tuple[str, str]:
    select_key = f"select_filter_{view}"
    custom_key = f"custom_filter_{view}"

    default_option = "(Sin filtro)"
    option_list = [default_option] + options
    if select_key not in st.session_state:
        st.session_state[select_key] = default_option
    if custom_key not in st.session_state:
        st.session_state[custom_key] = ""

    selected = st.sidebar.selectbox(
        "Filtrar por handicap",
        option_list,
        key=select_key,
        help="Selecciona un valor disponible o utiliza el campo manual para un valor personalizado.",
    )
    custom_value = st.sidebar.text_input(
        "Handicap manual",
        value=st.session_state[custom_key],
        key=custom_key,
        help="Introduce manualmente un handicap (ej. 0, 0.25, -0.75).",
    )

    return selected, custom_value


def _resolve_handicap_filter(selected: str, custom: str) -> str | None:
    if custom and custom.strip():
        return custom.strip()
    if selected and selected != "(Sin filtro)":
        return selected
    return None


def _clear_filters(view: str) -> None:
    st.session_state[f"select_filter_{view}"] = "(Sin filtro)"
    st.session_state[f"custom_filter_{view}"] = ""


def _ensure_session_defaults() -> None:
    st.session_state.setdefault("preview_cache", {})
    st.session_state.setdefault("analysis_cache", {})
    st.session_state.setdefault("list_view", "upcoming")

def _normalize_query_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return str(value)


def _get_query_param_first(name: str) -> str | None:
    return _normalize_query_value(st.query_params.get(name))


def _sync_query_params(expected: dict[str, str]) -> None:
    params_proxy = st.query_params
    current = {key: _normalize_query_value(params_proxy.get(key)) for key in list(params_proxy.keys())}
    if current == expected:
        return
    params_proxy.clear()
    for key, value in expected.items():
        params_proxy[key] = value


def _set_analysis_query(match_id: str, origin: str) -> None:
    _sync_query_params({
        'view': 'analysis',
        'match_id': str(match_id),
        'origin': origin,
    })
    st.rerun()


def _render_stats_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    try:
        df = pd.DataFrame(rows)
    except ValueError:
        return
    if not {"label", "home", "away"}.issubset(df.columns):
        return
    df = df.rename(columns={"label": "Estadistica", "home": "Casa", "away": "Fuera"})
    df = df.set_index("Estadistica")
    st.table(df)



PREVIEW_STYLE = """
<style>
.preview-root { display: flex; flex-direction: column; gap: 1.2rem; }
.preview-card { background: #ffffff; border-radius: 16px; padding: 1.25rem 1.5rem; box-shadow: 0 12px 26px rgba(15, 23, 42, 0.08); border: 1px solid rgba(148, 163, 184, 0.2); }
.preview-card.main-card { background: linear-gradient(135deg,#1e3a8a 0%,#2563eb 100%); color: #f8fafc; }
.preview-header { display: flex; justify-content: space-between; align-items: center; gap: 0.75rem; font-size: 1.05rem; font-weight: 600; }
.preview-header .home-name, .preview-header .away-name { flex: 1; }
.preview-header .vs { font-size: 0.95rem; opacity: 0.85; }
.preview-body { margin-top: 0.75rem; display: flex; flex-direction: column; gap: 0.35rem; }
.preview-line { margin: 0; font-size: 0.92rem; }
.preview-subtitle { margin: 0 0 0.6rem 0; font-size: 1rem; font-weight: 600; }
.preview-card-grid { display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); }
.preview-card.home-card { border-top: 4px solid #2563eb; }
.preview-card.away-card { border-top: 4px solid #f97316; }
.preview-card.neutral-card { border-top: 4px solid #16a34a; }
.preview-card.info-card { background: #f8fafc; border: 1px solid rgba(148, 163, 184, 0.35); }
.preview-score { font-size: 1.6rem; font-weight: 600; margin-bottom: 0.35rem; }
.preview-match-line { margin: 0; font-weight: 500; }
.preview-line.text-muted { color: #64748b; }
.preview-stat-table { width: 100%; border-collapse: collapse; margin-top: 0.5rem; font-size: 0.85rem; }
.preview-stat-table td { padding: 0.25rem 0.45rem; border-bottom: 1px solid rgba(226, 232, 240, 0.6); }
.stat-home { font-weight: 600; color: #1d4ed8; text-align: left; }
.stat-away { font-weight: 600; color: #f97316; text-align: right; }
.stat-label { text-align: center; color: #475569; font-weight: 500; }
.preview-columns { display: grid; gap: 1rem; grid-template-columns: minmax(0, 2fr) minmax(0, 1fr); align-items: start; }
.preview-columns.single { grid-template-columns: minmax(0, 1fr); }
.preview-columns .col-left, .preview-columns .col-right { display: flex; flex-direction: column; gap: 1rem; }
.preview-alert { padding: 0.85rem 1rem; border-radius: 12px; font-size: 0.9rem; }
.preview-alert.info { background: rgba(59, 130, 246, 0.12); color: #1d4ed8; border: 1px solid rgba(59, 130, 246, 0.2); }
.cover-badge { display: inline-flex; align-items: center; padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.78rem; font-weight: 600; margin-left: 0.3rem; }
.cover-positive { background: rgba(34, 197, 94, 0.18); color: #15803d; }
.cover-negative { background: rgba(248, 113, 113, 0.18); color: #b91c1c; }
.cover-neutral { background: rgba(148, 163, 184, 0.25); color: #1f2937; }
.cover-unknown { background: rgba(248, 250, 252, 0.8); color: #334155; border: 1px dashed rgba(226, 232, 240, 0.8); }
.preview-card.info-card p { margin-bottom: 0.35rem; }
.form-grid { display: grid; gap: 0.4rem; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
.form-item { display: flex; flex-direction: column; background: #ffffff; border-radius: 12px; padding: 0.65rem 0.75rem; border: 1px solid rgba(148, 163, 184, 0.2); box-shadow: 0 6px 15px rgba(15, 23, 42, 0.04); }
.form-label { font-weight: 600; color: #0f172a; }
.form-value { font-size: 0.88rem; color: #475569; }
.preview-indirect-table { width: 100%; border-collapse: collapse; margin-top: 0.6rem; font-size: 0.85rem; }
.preview-indirect-table th, .preview-indirect-table td { padding: 0.35rem 0.45rem; border-bottom: 1px solid rgba(203, 213, 225, 0.6); text-align: left; }
.preview-indirect-table th { background: rgba(15, 23, 42, 0.04); font-weight: 600; }
</style>
"""


def _format_cover_status_html(status: str | None) -> str:
    if not status:
        return ""
    normalized = str(status).strip()
    if not normalized:
        return ""
    upper = normalized.upper()
    mapping = {
        "CUBIERTO": ("cover-positive", "CUBIERTO"),
        "NO CUBIERTO": ("cover-negative", "NO CUBIERTO"),
        "NULO": ("cover-neutral", "NULO"),
        "PUSH": ("cover-neutral", "PUSH"),
    }
    css_class, label = mapping.get(upper, ("cover-unknown", normalized.title()))
    return f"<span class='cover-badge {css_class}'>{label}</span>"


def _render_cover_line_html(status: str | None) -> str:
    badge = _format_cover_status_html(status)
    if not badge:
        return ""
    return f"<p class='preview-line'><strong>Estado:</strong> {badge}</p>"


def _render_stats_rows_html(rows: Iterable[dict[str, Any]] | None) -> str:
    if not rows:
        return ""
    html_rows: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = row.get("label")
        home = row.get("home")
        away = row.get("away")
        home_html = str(home) if home not in (None, "") else "-"
        label_html = str(label) if label not in (None, "") else "-"
        away_html = str(away) if away not in (None, "") else "-"
        html_rows.append(
            f"<tr><td class='stat-home'>{home_html}</td><td class='stat-label'>{label_html}</td><td class='stat-away'>{away_html}</td></tr>"
        )
    if not html_rows:
        return ""
    return "<table class='preview-stat-table'><tbody>" + "".join(html_rows) + "</tbody></table>"


def _short_date(value: Any) -> str:
    if value in (None, ""):
        return "-"
    text_value = str(value).strip()
    if not text_value:
        return "-"
    if "T" in text_value:
        text_value = text_value.split("T", 1)[0]
    if len(text_value) > 10:
        return text_value[:10]
    return text_value


def _humanize_timestamp(value: str | None) -> str:
    if not value:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        local = parsed.astimezone(PREVIEW_TZ)
        return local.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(value)


def _format_match_datetime(preview: dict[str, Any]) -> str:
    dt_value = preview.get("match_datetime")
    if isinstance(dt_value, str):
        try:
            parsed = dt.datetime.fromisoformat(dt_value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            local = parsed.astimezone(PREVIEW_TZ)
            return local.strftime("%d/%m/%Y %H:%M")
        except ValueError:
            pass
    date = preview.get("match_date")
    time = preview.get("match_time")
    if date and time:
        return f"{date} {time}"
    if date:
        return str(date)
    return "-"


def _render_recent_card_block(title: str, card: dict[str, Any] | None, variant: str) -> str:
    if not card:
        return ""
    score_raw = card.get("score") or card.get("score_line") or "-"
    score = str(score_raw).replace(":", " - ")
    home_team = card.get("home") or card.get("home_team") or "-"
    away_team = card.get("away") or card.get("away_team") or "-"
    match_line = f"<p class='preview-match-line'><span class='home-name'>{home_team}</span> vs <span class='away-name'>{away_team}</span></p>"
    date_html = _short_date(card.get("date"))
    odds = card.get("ah")
    odds_html = ""
    if odds not in (None, ""):
        odds_html = f"<p class='preview-line'><strong>AH:</strong> <span class='ah-value'>{odds}</span></p>"
    cover_html = _render_cover_line_html(card.get("cover_status"))
    stats_html = _render_stats_rows_html(card.get("stats_rows"))
    analysis = card.get("analysis")
    analysis_html = f"<p class='preview-line'><strong>Insight:</strong> {analysis}</p>" if analysis else ""
    return (
        f"<div class='preview-card {variant}'>"
        f"<div class='preview-card-header'>{title}</div>"
        "<div class='preview-card-body'>"
        f"<div class='preview-score'>{score}</div>"
        f"{match_line}"
        f"<p class='preview-line text-muted'>{date_html}</p>"
        f"{odds_html}"
        f"{cover_html}"
        f"{stats_html}"
        f"{analysis_html}"
        "</div></div>"
    )


def _build_market_summary_block(preview: dict[str, Any]) -> str:
    home = preview.get("home_name") or preview.get("home_team") or "-"
    away = preview.get("away_name") or preview.get("away_team") or "-"
    handicap = preview.get("handicap") or {}
    ah_line = handicap.get("ah_line") or "-"
    favorite = handicap.get("favorite") or "Sin favorito"
    cover = _render_cover_line_html(handicap.get("cover_on_last_h2h"))
    kickoff = _format_match_datetime(preview)
    content = [
        "<div class='preview-card main-card'>",
        f"<div class='preview-header'><span class='home-name'>{home}</span><span class='vs'>vs</span><span class='away-name'>{away}</span></div>",
        "<div class='preview-body'>",
        f"<p class='preview-line'><strong>Fecha / hora (España):</strong> {kickoff}</p>",
        f"<p class='preview-line'><strong>Handicap actual:</strong> <span class='ah-value'>{ah_line}</span></p>",
        f"<p class='preview-line'><strong>Favorito:</strong> {favorite}</p>",
    ]
    if cover:
        content.append(cover)
    content.append("</div></div>")
    return "".join(content)


def _build_recent_cards_section(preview: dict[str, Any]) -> str:
    data = preview.get("recent_indirect") or {}
    if not data and preview.get("recent_indirect_full"):
        rif = preview.get("recent_indirect_full") or {}
        data = {
            "last_home": rif.get("last_home"),
            "last_away": rif.get("last_away"),
            "h2h_col3": rif.get("h2h_col3"),
        }
    if not isinstance(data, dict):
        return ""
    home_label = preview.get("home_name") or preview.get("home_team") or "Equipo local"
    away_label = preview.get("away_name") or preview.get("away_team") or "Equipo visitante"
    cards: list[str] = []
    if data.get("last_home"):
        cards.append(_render_recent_card_block(f"Ultimo {home_label} (Casa)", data.get("last_home"), "home-card"))
    if data.get("last_away"):
        cards.append(_render_recent_card_block(f"Ultimo {away_label} (Fuera)", data.get("last_away"), "away-card"))
    if data.get("h2h_col3"):
        cards.append(_render_recent_card_block("H2H rivales (Col3)", data.get("h2h_col3"), "neutral-card"))
    if not cards:
        return ""
    return "<div class='preview-card-grid'>" + "".join(cards) + "</div>"


def _build_recent_form_block(preview: dict[str, Any]) -> str:
    rf = preview.get("recent_form") or {}
    home_form = rf.get("home") or {}
    away_form = rf.get("away") or {}
    if not home_form and not away_form:
        return ""
    home_label = preview.get("home_name") or preview.get("home_team") or "Equipo local"
    away_label = preview.get("away_name") or preview.get("away_team") or "Equipo visitante"
    items: list[str] = []
    if home_form:
        wins = home_form.get("wins", 0)
        total = home_form.get("total", 0)
        items.append(f"<div class='form-item'><span class='form-label'>{home_label}</span><span class='form-value'>{wins} victorias en {total} partidos recientes</span></div>")
    if away_form:
        wins = away_form.get("wins", 0)
        total = away_form.get("total", 0)
        items.append(f"<div class='form-item'><span class='form-label'>{away_label}</span><span class='form-value'>{wins} victorias en {total} partidos recientes</span></div>")
    if not items:
        return ""
    return "<div class='preview-card info-card'><h4 class='preview-subtitle'>Rendimiento reciente (ultimo 8)</h4><div class='form-grid'>" + "".join(items) + "</div></div>"


def _build_h2h_stats_block(preview: dict[str, Any]) -> str:
    h2h = preview.get("h2h_stats") or {}
    if not isinstance(h2h, dict):
        return ""
    total = h2h.get("home_wins", 0) + h2h.get("away_wins", 0) + h2h.get("draws", 0)
    if total == 0:
        return ""
    home_label = preview.get("home_name") or preview.get("home_team") or "Local"
    away_label = preview.get("away_name") or preview.get("away_team") or "Visitante"
    return (
        "<div class='preview-card info-card'>"
        "<h4 class='preview-subtitle'>Historial directo (ultimos 8)</h4>"
        f"<p class='preview-line'><strong>Victorias {home_label}:</strong> {h2h.get('home_wins', 0)}</p>"
        f"<p class='preview-line'><strong>Victorias {away_label}:</strong> {h2h.get('away_wins', 0)}</p>"
        f"<p class='preview-line'><strong>Empates:</strong> {h2h.get('draws', 0)}</p>"
        "</div>"
    )


def _build_dangerous_attacks_block(preview: dict[str, Any]) -> str:
    favorite = preview.get("favorite_dangerous_attacks")
    ataques = preview.get("dangerous_attacks") or preview.get("ataques_peligrosos") or {}
    snippets: list[str] = []
    if isinstance(favorite, dict) and favorite.get("name"):
        calidad = "muy superior" if favorite.get("very_superior") else "con ligera ventaja"
        own = favorite.get("own", "-")
        rival = favorite.get("rival", "-")
        snippets.append(f"<p class='preview-line'><strong>{favorite.get('name')}:</strong> {calidad} ({own} vs {rival}).</p>")
    if isinstance(ataques, dict):
        for key in ("team1", "team2"):
            data = ataques.get(key)
            if not isinstance(data, dict) or not data.get("name"):
                continue
            calidad = "muy superior" if data.get("very_superior") else "equilibrados"
            own = data.get("own", "-")
            rival = data.get("rival", "-")
            snippets.append(f"<p class='preview-line'><strong>{data.get('name')}:</strong> {calidad} ({own} vs {rival}).</p>")
    if not snippets:
        return ""
    return "<div class='preview-card info-card'><h4 class='preview-subtitle'>Ataques peligrosos</h4>" + "".join(snippets) + "</div>"


def _build_h2h_indirect_block(preview: dict[str, Any]) -> str:
    h2h_indirect = preview.get("h2h_indirect") or {}
    if not isinstance(h2h_indirect, dict):
        return ""
    samples = h2h_indirect.get("samples") or []
    home_better = h2h_indirect.get("home_better") or 0
    away_better = h2h_indirect.get("away_better") or 0
    draws = h2h_indirect.get("draws") or 0
    summary = f"<p class='preview-line'><strong>Comparativa rivales comunes:</strong> Local mejor en {home_better}, visitante mejor en {away_better}, empate en {draws}.</p>"
    rows: list[str] = []
    for sample in samples[:5]:
        if not isinstance(sample, dict):
            continue
        rival = str(sample.get("rival", "-")).title()
        home_margin = sample.get("home_margin", "-")
        away_margin = sample.get("away_margin", "-")
        verdict = str(sample.get("verdict", "-")).title()
        rows.append(f"<tr><td>{rival}</td><td>{home_margin}</td><td>{away_margin}</td><td>{verdict}</td></tr>")
    table_html = ""
    if rows:
        table_html = "<table class='preview-indirect-table'><thead><tr><th>Rival</th><th>Margen local</th><th>Margen visitante</th><th>Favor</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    return "<div class='preview-card neutral-card'><h4 class='preview-subtitle'>Rivales comunes</h4>" + summary + table_html + "</div>"


def _build_simplified_html_block(preview: dict[str, Any]) -> str:
    html = preview.get("simplified_html") or preview.get("analisis_simplificado_html")
    if html:
        return "<div class='preview-card info-card'>" + html + "</div>"
    return ""


def _build_preview_columns(preview: dict[str, Any]) -> str:
    left_parts = [
        _build_recent_form_block(preview),
        _build_recent_cards_section(preview),
        _build_h2h_indirect_block(preview),
        _build_dangerous_attacks_block(preview),
    ]
    left_html = "".join(part for part in left_parts if part)
    right_parts = [
        _build_h2h_stats_block(preview),
        _build_simplified_html_block(preview),
    ]
    right_html = "".join(part for part in right_parts if part)
    if not left_html and not right_html:
        return ""
    if not right_html:
        return f"<div class='preview-columns single'><div class='col-left'>{left_html}</div></div>"
    if not left_html:
        return f"<div class='preview-columns single'><div class='col-left'>{right_html}</div></div>"
    return f"<div class='preview-columns'><div class='col-left'>{left_html}</div><div class='col-right'>{right_html}</div></div>"


def _build_cache_notice(preview: dict[str, Any]) -> str:
    meta = None
    if isinstance(preview.get("_cached_preview"), dict):
        meta = preview.get("_cached_preview")
    elif isinstance(preview.get("_cached_analysis"), dict):
        meta = preview.get("_cached_analysis")
    if meta:
        label = meta.get("source") or "almacén"
        stored = _humanize_timestamp(meta.get("stored_at"))
        return f"<div class='preview-alert info'>Datos recuperados del almacén ({label}) · {stored}</div>"
    source = preview.get("_preview_source")
    timestamp = preview.get("_preview_timestamp")
    if source and timestamp:
        stored = _humanize_timestamp(timestamp)
        return f"<div class='preview-alert info'>Datos generados en vivo ({source}) · {stored}</div>"
    return ""


def _render_preview(preview: dict[str, Any]) -> None:
    if not isinstance(preview, dict) or not preview:
        st.info("No hay datos disponibles para esta vista previa.")
        return
    st.markdown(PREVIEW_STYLE, unsafe_allow_html=True)
    sections: list[str] = []
    notice = _build_cache_notice(preview)
    if notice:
        sections.append(notice)
    summary = _build_market_summary_block(preview)
    if summary:
        sections.append(summary)
    columns = _build_preview_columns(preview)
    if columns:
        sections.append(columns)
    html_body = "".join(sections)
    if not html_body:
        st.info("No hay información detallada disponible para esta vista previa.")
        return
    st.markdown(f"<div class='preview-root'>{html_body}</div>", unsafe_allow_html=True)


def _render_match_card(match: dict[str, Any], view: str) -> None:
    header = f"{match['time']} - {match['home_team']} vs {match['away_team']}"
    if view == "finished":
        header += f" - Resultado: {match.get('score', 'N/A')}"

    with st.expander(header):
        info_lines = [
            f"**Handicap:** {match.get('handicap', '-')}",
            f"**Linea de goles:** {match.get('goal_line', '-')}",
            f"**ID Nowgoal:** `{match['id']}`",
        ]
        st.markdown("  \n".join(info_lines))

        columns_config = [1, 1, 1]
        include_storage = view == "finished"
        if include_storage:
            columns_config.append(1)

        columns = st.columns(columns_config)
        col_preview, col_analysis, col_json = columns[:3]
        col_storage = columns[3] if include_storage else None

        cache_key = (view, match["id"])
        preview_cache: dict[tuple[str, str], dict[str, Any]] = st.session_state["preview_cache"]

        if col_preview.button("Vista previa ligera", key=f"preview_btn_{view}_{match['id']}"):
            with st.spinner("Calculando vista previa..."):
                preview_data = obtener_datos_preview_ligero(match["id"])
            preview_cache[cache_key] = preview_data

        if col_analysis.button("Abrir analisis completo", key=f"analysis_btn_{view}_{match['id']}"):
            _set_analysis_query(match["id"], origin=view)

        if col_json.button("Ver JSON crudo", key=f"json_btn_{view}_{match['id']}"):
            preview_data = preview_cache.get(cache_key)
            if preview_data is None:
                with st.spinner("Generando datos JSON..."):
                    preview_data = obtener_datos_preview_ligero(match["id"])
                    preview_cache[cache_key] = preview_data
            st.json(preview_data)

        if include_storage and col_storage is not None:
            with col_storage:
                st.caption("Almacen")

                if st.button("Guardar preview", key=f"store_preview_{match['id']}"):
                    preview_data = preview_cache.get(cache_key)
                    if preview_data is None:
                        with st.spinner("Calculando vista previa..."):
                            preview_data = obtener_datos_preview_ligero(match["id"])
                            preview_cache[cache_key] = preview_data
                    if not preview_data or preview_data.get("error"):
                        st.error(preview_data.get("error", "No se pudo generar la vista previa."))
                    else:
                        summary = upsert_previews(
                            [(match["id"], preview_data)],
                            source="streamlit_preview",
                            payload_type="preview",
                        )
                        st.success(
                            f"Preview guardada (nuevos: {summary.get('added', 0)}, actualizados: {summary.get('updated', 0)})."
                        )

                if st.button("Guardar analisis", key=f"store_analysis_{match['id']}"):
                    analysis_cache: dict[str, Any] = st.session_state["analysis_cache"]
                    analysis_data = analysis_cache.get(match["id"])
                    if analysis_data is None:
                        with st.spinner("Calculando analisis completo..."):
                            analysis_data = obtener_datos_completos_partido(match["id"])
                            analysis_cache[match["id"]] = analysis_data
                    if not analysis_data or analysis_data.get("error"):
                        st.error(analysis_data.get("error", "No se pudo generar el analisis."))
                    else:
                        summary = upsert_previews(
                            [(match["id"], analysis_data)],
                            source="streamlit_analysis",
                            payload_type="analysis",
                        )
                        st.success(
                            f"Analisis guardado (nuevos: {summary.get('added', 0)}, actualizados: {summary.get('updated', 0)})."
                        )


def _render_matches_list(view: str) -> None:
    st.header("Panel principal")
    match_count = st.sidebar.slider(
        "Cantidad de partidos a mostrar",
        min_value=5,
        max_value=MAX_MATCHES,
        value=DEFAULT_MATCH_COUNT,
        step=5,
    )

    options = _cached_handicap_options(view)
    selected_option, custom_value = _get_filter_values(view, options)

    if st.sidebar.button("Limpiar filtro"):
        _clear_filters(view)
        st.rerun()

    handicap_filter = _resolve_handicap_filter(selected_option, custom_value)

    if view == "finished":
        matches = _cached_finished_matches(match_count, handicap_filter)
    else:
        matches = _cached_upcoming_matches(match_count, handicap_filter)

    if not matches:
        st.warning("No se encontraron partidos para los criterios seleccionados.")
        return

    for match in matches:
        _render_match_card(match, view=view)


def _render_analysis(match_id: str, origin: str) -> None:
    st.sidebar.markdown("---")
    if st.sidebar.button("Volver a la lista", use_container_width=True):
        if origin in {"upcoming", "finished", "storage"}:
            _sync_query_params({"view": origin})
        else:
            _sync_query_params({})
        st.rerun()

    st.header(f"Analisis completo del partido {match_id}")

    analysis_cache: dict[str, Any] = st.session_state["analysis_cache"]
    if match_id not in analysis_cache:
        with st.spinner("Ejecutando analisis completo. Este proceso puede tardar unos segundos..."):
            analysis_cache[match_id] = obtener_datos_completos_partido(match_id)

    datos = analysis_cache[match_id]
    if not datos or (isinstance(datos, dict) and datos.get("error")):
        st.error(datos.get("error", "No se pudieron obtener los datos del partido."))
        return

    main_odds = datos.get("main_match_odds_data")
    h2h_data = datos.get("h2h_data")
    home_name = datos.get("home_name")
    away_name = datos.get("away_name")
    analisis_simplificado_html = ""
    if all([main_odds, h2h_data, home_name, away_name]):
        analisis_simplificado_html = generar_analisis_mercado_simplificado(main_odds, h2h_data, home_name, away_name)

    rendered = _render_template(
        "estudio.html",
        data=datos,
        format_ah=format_ah_as_decimal_string_of,
        analisis_simplificado_html=analisis_simplificado_html,
    )

    components_html(rendered, height=2300, scrolling=True)


def main() -> None:
    _ensure_session_defaults()

    st.title(f"{PAGE_ICON} {PAGE_TITLE}")
    st.markdown(
        "Esta interfaz web esta optimizada para funcionar enteramente en Streamlit Cloud "
        "manteniendo todas las capacidades de analisis del proyecto original."
    )

    if st.sidebar.button("Actualizar datos", help="Limpia la cache de datos externos"):
        st.cache_data.clear()
        st.session_state["preview_cache"].clear()
        st.session_state["analysis_cache"].clear()
        st.success("Caches limpiadas correctamente. Los datos se recargaran en la proxima consulta.")

    view_param = _get_query_param_first("view")
    match_id_param = _get_query_param_first("match_id")
    if view_param == "analysis" and match_id_param:
        origin = _get_query_param_first("origin") or "upcoming"
        _render_analysis(match_id_param, origin)
        return

    current_view = st.session_state.get("list_view", "upcoming")
    if view_param in {"upcoming", "finished", "storage"}:
        current_view = view_param

    view_labels = {
        "Proximos partidos": "upcoming",
        "Resultados finalizados": "finished",
        "Almacen de analisis": "storage",
    }
    reverse_labels = {v: k for k, v in view_labels.items()}

    sidebar_choice = st.sidebar.radio(
        "Selecciona la vista",
        list(view_labels.keys()),
        index=list(view_labels.values()).index(current_view),
    )
    current_view = view_labels[sidebar_choice]
    st.session_state["list_view"] = current_view

    _sync_query_params({"view": current_view})
    st.subheader(reverse_labels[current_view])

    if current_view == "storage":
        _render_storage_manager()
        return

    _render_matches_list(current_view)


if __name__ == "__main__":
    main()

