from __future__ import annotations

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
from modules.preview_storage import delete_preview, list_previews, upsert_previews


PAGE_TITLE = "Analizador Profesional de Partidos"
PAGE_ICON = "AP"
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


def _render_recent_indirect(preview: dict[str, Any]) -> None:
    indirect = preview.get("recent_indirect") or {}
    if not indirect:
        return

    st.markdown("#### Referencias indirectas recientes")

    col_home, col_away, col_h2h = st.columns(3)

    last_home = indirect.get("last_home") or {}
    with col_home:
        st.markdown("**Ultimo partido del local**")
        if last_home:
            st.markdown(f"{last_home.get('home', '-')} vs {last_home.get('away', '-')}")
            st.markdown(f"Marcador: {last_home.get('score', '-')}")
            st.markdown(f"AH: {last_home.get('ah', '-')}")
            _render_stats_rows(last_home.get("stats_rows") or [])
        else:
            st.info("Sin datos del local en liga.")

    last_away = indirect.get("last_away") or {}
    with col_away:
        st.markdown("**Ultimo partido del visitante**")
        if last_away:
            st.markdown(f"{last_away.get('home', '-')} vs {last_away.get('away', '-')}")
            st.markdown(f"Marcador: {last_away.get('score', '-')}")
            st.markdown(f"AH: {last_away.get('ah', '-')}")
            _render_stats_rows(last_away.get("stats_rows") or [])
        else:
            st.info("Sin datos del visitante en liga.")

    h2h_col3 = indirect.get("h2h_col3") or {}
    with col_h2h:
        st.markdown("**H2H rivales (columna 3)**")
        if h2h_col3:
            st.markdown(f"Marcador: {h2h_col3.get('score_line', '-')}")
            st.markdown(f"AH: {h2h_col3.get('ah', '-')}")
            st.markdown(f"Cover: {h2h_col3.get('cover', '-')}")
        else:
            st.info("Sin datos de rivales comunes recientes.")




def _render_storage_entry(entry: dict[str, Any], payload_type: str) -> None:
    match_id = str(entry.get("match_id", ""))
    stored_at = entry.get("stored_at") or "Sin fecha"
    source = entry.get("source") or "manual"
    payload = entry.get("payload") or {}
    header = f"{payload_type.title()} - {match_id or '(sin ID)'} - {stored_at}"

    with st.expander(header):
        st.write(f"Fuente: {source}")
        st.write(f"Guardado: {stored_at}")
        if payload:
            if payload_type == "preview":
                _render_preview(payload)
            st.json(payload)
        else:
            st.info("No hay datos disponibles para esta entrada.")

        col_view, col_delete = st.columns(2)

        if payload_type == "analysis" and payload:
            if col_view.button(
                f"Abrir analisis {match_id}",
                key=f"storage_open_analysis_{match_id}_{stored_at}",
            ):
                analysis_cache: dict[str, Any] = st.session_state["analysis_cache"]
                analysis_cache[match_id] = payload
                _set_analysis_query(match_id, origin="storage")

        elif payload_type == "preview" and payload:
            if col_view.button(
                f"Cargar preview {match_id}",
                key=f"storage_open_preview_{match_id}_{stored_at}",
            ):
                cache = st.session_state["preview_cache"]
                cache[("storage", match_id)] = payload
                st.success("Preview disponible en la cache local para su consulta inmediata.")

        if col_delete.button(
            f"Eliminar {payload_type}",
            key=f"storage_delete_{payload_type}_{match_id}_{stored_at}",
        ):
            if delete_preview(match_id, payload_type=payload_type):
                st.success("Entrada eliminada correctamente.")
                st.experimental_rerun()
            else:
                st.error("No se pudo eliminar la entrada.")


def _render_storage_manager() -> None:
    st.header("Almacen de estudios")
    storage_file = Path(__file__).resolve().parent / "preview_store.json"
    st.caption(f"Datos persistidos en: {storage_file}")

    tabs = st.tabs(["Vistas previas", "Analisis guardados"])
    preview_entries = list_previews(payload_type="preview")
    analysis_entries = list_previews(payload_type="analysis")

    with tabs[0]:
        if not preview_entries:
            st.info("No hay vistas previas guardadas.")
        else:
            for entry in preview_entries:
                _render_storage_entry(entry, "preview")

    with tabs[1]:
        if not analysis_entries:
            st.info("No hay analisis guardados.")
        else:
            for entry in analysis_entries:
                _render_storage_entry(entry, "analysis")

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
        st.experimental_rerun()

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
        target_params = {"view": origin} if origin in {"upcoming", "finished", "storage"} else {}
        st.experimental_set_query_params(**target_params)
        st.experimental_rerun()

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

    query_params = st.experimental_get_query_params()
    if query_params.get("view", [None])[0] == "analysis" and query_params.get("match_id"):
        match_id = query_params["match_id"][0]
        origin = query_params.get("origin", ["upcoming"])[0]
        _render_analysis(match_id, origin)
        return

    current_view = st.session_state.get("list_view", "upcoming")
    view_from_query = query_params.get("view", [None])[0]
    if view_from_query in {"upcoming", "finished", "storage"}:
        current_view = view_from_query

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

    st.experimental_set_query_params(view=current_view)
    st.subheader(reverse_labels[current_view])

    if current_view == "storage":
        _render_storage_manager()
        return

    _render_matches_list(current_view)


if __name__ == "__main__":
    main()

