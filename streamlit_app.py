from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
from jinja2 import Environment, FileSystemLoader, select_autoescape
from streamlit.components.v1 import html as components_html

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


PAGE_TITLE = "Analizador Profesional de Partidos"
PAGE_ICON = "⚽"
MAX_MATCHES = 50
DEFAULT_MATCH_COUNT = 20


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
        "Filtrar por hándicap",
        option_list,
        key=select_key,
        help="Selecciona un valor disponible o utiliza el campo manual para un valor personalizado.",
    )
    custom_value = st.sidebar.text_input(
        "Hándicap manual",
        value=st.session_state[custom_key],
        key=custom_key,
        help="Introduce manualmente un hándicap (ej. 0, 0.25, -0.75).",
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


def _render_stats_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    try:
        df = pd.DataFrame(rows)
    except ValueError:
        return
    if not {"label", "home", "away"}.issubset(df.columns):
        return
    df = df.rename(columns={"label": "Estadística", "home": "Casa", "away": "Fuera"})
    df = df.set_index("Estadística")
    st.table(df)


def _render_recent_indirect(preview: dict[str, Any]) -> None:
    indirect = preview.get("recent_indirect") or {}
    col_left, col_right, col_center = st.columns(3)

    with col_left:
        last_home = indirect.get("last_home")
        st.markdown("### Último partido del local")
        if last_home:
            st.markdown(
                f"**{last_home.get('home')}** vs {last_home.get('away')}  "
                f"\nMarcador: {last_home.get('score', 'N/A')}  "
                f"\nHándicap: {last_home.get('ah', '-')}"
            )
            _render_stats_rows(last_home.get("stats_rows") or [])
        else:
            st.info("Sin datos recientes del equipo local en liga.")

    with col_center:
        h2h = indirect.get("h2h_col3")
        st.markdown("### Rivales comunes")
        if h2h:
            st.markdown(
                f"{h2h.get('score_line', 'N/A')}  \n"
                f"Hándicap: {h2h.get('ah', '-')}  \n"
                f"Fecha: {h2h.get('date', 'N/A')}"
            )
            _render_stats_rows(h2h.get("stats_rows") or [])
        else:
            st.info("No se encontraron enfrentamientos de rivales comunes recientes.")

    with col_right:
        last_away = indirect.get("last_away")
        st.markdown("### Último partido del visitante")
        if last_away:
            st.markdown(
                f"**{last_away.get('home')}** vs {last_away.get('away')}  "
                f"\nMarcador: {last_away.get('score', 'N/A')}  "
                f"\nHándicap: {last_away.get('ah', '-')}"
            )
            _render_stats_rows(last_away.get("stats_rows") or [])
        else:
            st.info("Sin datos recientes del equipo visitante en liga.")


def _render_preview(preview: dict[str, Any]) -> None:
    st.markdown("---")
    st.markdown("#### Vista previa ligera")
    handicap = preview.get("handicap", {})
    st.markdown(
        f"**Hándicap actual:** {handicap.get('ah_line', '-') }  "
        f"\n**Favorito:** {handicap.get('favorite') or 'Sin favorito claro'}  "
        f"\n**Último H2H vs línea:** {handicap.get('cover_on_last_h2h', 'N/D')}"
    )

    recent_form = preview.get("recent_form") or {}
    if recent_form:
        col_home, col_away = st.columns(2)
        home = recent_form.get("home") or {}
        away = recent_form.get("away") or {}
        with col_home:
            st.metric(
                label=f"Racha {preview.get('home_team', 'Local')}",
                value=f"{home.get('wins', 0)} victorias",
                delta=f"Últimos {home.get('total', 0)} partidos",
            )
        with col_away:
            st.metric(
                label=f"Racha {preview.get('away_team', 'Visitante')}",
                value=f"{away.get('wins', 0)} victorias",
                delta=f"Últimos {away.get('total', 0)} partidos",
            )

    h2h_stats = preview.get("h2h_stats") or {}
    if h2h_stats:
        st.markdown(
            f"**Enfrentamientos directos (últimos 8):** "
            f"{h2h_stats.get('home_wins', 0)} victorias local / "
            f"{h2h_stats.get('away_wins', 0)} victorias visitante / "
            f"{h2h_stats.get('draws', 0)} empates."
        )

    indirect = preview.get("h2h_indirect") or {}
    if indirect.get("samples"):
        st.markdown("#### Comparativa de rivales comunes")
        rows = []
        for sample in indirect["samples"]:
            rows.append(
                {
                    "Rival": sample.get("rival", "-"),
                    "Margen Local": sample.get("home_margin"),
                    "Margen Visitante": sample.get("away_margin"),
                    "Veredicto": sample.get("verdict"),
                }
            )
        df = pd.DataFrame(rows)
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.markdown("No hay suficientes datos de rivales comunes para esta vista previa.")

    dangerous = preview.get("favorite_dangerous_attacks")
    if dangerous:
        if dangerous.get("very_superior"):
            st.success(
                f"Ataques peligrosos: {dangerous['name']} genera una ventaja clara "
                f"({dangerous['own']} vs {dangerous['rival']})."
            )
        else:
            st.info(
                f"Ataques peligrosos equilibrados para {dangerous['name']} "
                f"({dangerous['own']} vs {dangerous['rival']})."
            )

    _render_recent_indirect(preview)

    st.markdown("---")
    st.caption("Datos obtenidos de Nowgoal. Vista previa calculada mediante scraping ligero.")


def _normalize_query_params() -> dict[str, list[str]]:
    """Return current query parameters as a dict of lists."""

    try:
        raw_params = dict(st.query_params)
    except Exception:
        try:  # pragma: no cover - compatibility with older versions
            raw_params = st.experimental_get_query_params()
        except Exception:
            raw_params = {}

    normalized: dict[str, list[str]] = {}
    for key, value in raw_params.items():
        if isinstance(value, list):
            normalized[key] = [str(item) for item in value]
        elif value is None:
            normalized[key] = []
        else:
            normalized[key] = [str(value)]
    return normalized


def _query_param_first(params: dict[str, Sequence[str]], key: str, default: str | None = None) -> str | None:
    values = params.get(key)
    if not values:
        return default
    return next(iter(values), default)


def _replace_query_params(**params: str | None) -> None:
    sanitized = {key: value for key, value in params.items() if value is not None}
    try:
        st.query_params.clear()
        if sanitized:
            st.query_params.update({key: str(value) for key, value in sanitized.items()})
    except Exception:  # pragma: no cover - compatibility branch
        st.experimental_set_query_params(**{key: str(value) for key, value in sanitized.items()})


def _rerun() -> None:
    rerun_callable = getattr(st, "rerun", None)
    if rerun_callable is not None:
        rerun_callable()
    else:  # pragma: no cover - fallback for older Streamlit versions
        st.experimental_rerun()


def _set_analysis_query(match_id: str, origin: str) -> None:
    _replace_query_params(view="analysis", match_id=match_id, origin=origin)
    _rerun()


def _render_match_card(match: dict[str, Any], view: str) -> None:
    header = f"{match['time']} · {match['home_team']} vs {match['away_team']}"
    if view == "finished":
        header += f" · Resultado: {match.get('score', 'N/A')}"

    with st.expander(header):
        st.markdown(
            f"**Hándicap:** {match.get('handicap', '-') }  "
            f"\n**Línea de goles:** {match.get('goal_line', '-') }  "
            f"\n**ID Nowgoal:** `{match['id']}`"
        )

        col_preview, col_analysis, col_json = st.columns([1, 1, 1])

        cache_key = (view, match["id"])
        preview_cache: dict[tuple[str, str], dict[str, Any]] = st.session_state["preview_cache"]

        if col_preview.button("Vista previa ligera", key=f"preview_btn_{view}_{match['id']}"):
            with st.spinner("Calculando vista previa..."):
                preview_data = obtener_datos_preview_ligero(match["id"])
            preview_cache[cache_key] = preview_data

        if col_analysis.button("Abrir análisis completo", key=f"analysis_btn_{view}_{match['id']}"):
            _set_analysis_query(match["id"], origin=view)

        if col_json.button("Ver JSON crudo", key=f"json_btn_{view}_{match['id']}"):
            preview_data = preview_cache.get(cache_key)
            if preview_data is None:
                with st.spinner("Generando datos JSON..."):
                    preview_data = obtener_datos_preview_ligero(match["id"])
                    preview_cache[cache_key] = preview_data
            st.json(preview_data)

        preview_data = preview_cache.get(cache_key)
        if preview_data:
            if preview_data.get("error"):
                st.error(preview_data["error"])
            else:
                _render_preview(preview_data)


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
        _rerun()

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
    if st.sidebar.button("⬅️ Volver a la lista", use_container_width=True):
        target_params = {"view": origin} if origin in {"upcoming", "finished"} else {}
        _replace_query_params(**target_params)
        _rerun()

    st.header(f"Análisis completo del partido {match_id}")

    analysis_cache: dict[str, Any] = st.session_state["analysis_cache"]
    if match_id not in analysis_cache:
        with st.spinner("Ejecutando análisis completo. Este proceso puede tardar unos segundos..."):
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
        "Esta interfaz web está optimizada para funcionar íntegramente en Streamlit Cloud "
        "manteniendo todas las capacidades de análisis del proyecto original."
    )

    if st.sidebar.button("🔁 Actualizar datos", help="Limpia la caché de datos externos"):
        st.cache_data.clear()
        st.session_state["preview_cache"].clear()
        st.session_state["analysis_cache"].clear()
        st.success("Cachés limpiadas correctamente. Los datos se recargarán en la próxima consulta.")

    query_params = _normalize_query_params()
    if _query_param_first(query_params, "view") == "analysis" and query_params.get("match_id"):
        match_id = query_params["match_id"][0]
        origin = _query_param_first(query_params, "origin", "upcoming") or "upcoming"
        _render_analysis(match_id, origin)
        return

    current_view = st.session_state.get("list_view", "upcoming")
    view_from_query = _query_param_first(query_params, "view")
    if view_from_query in {"upcoming", "finished"}:
        current_view = view_from_query

    view_labels = {
        "Próximos partidos": "upcoming",
        "Resultados finalizados": "finished",
    }
    reverse_labels = {v: k for k, v in view_labels.items()}

    sidebar_choice = st.sidebar.radio(
        "Selecciona la vista",
        list(view_labels.keys()),
        index=list(view_labels.values()).index(current_view),
    )
    current_view = view_labels[sidebar_choice]
    st.session_state["list_view"] = current_view

    _replace_query_params(view=current_view)
    st.subheader(reverse_labels[current_view])

    _render_matches_list(current_view)


if __name__ == "__main__":
    main()

