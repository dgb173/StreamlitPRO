
import streamlit as st
import asyncio
import pandas as pd
from models import MatchResult
import scraper
import datetime
import pytz

# 1. Page Setup
st.set_page_config(page_title="StreamlitPRO v2 AI", layout="wide")

# 2. Inject Custom CSS
custom_css = """
<style>
    body { background-color: #f8f9fa; }
    .stTabs [data-baseweb="tab-list"] { gap: 2px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: #F0F2F6; border-radius: 4px 4px 0px 0px; gap: 1px; padding-top: 10px; padding-bottom: 10px; }
    .stTabs [aria-selected="true"] { background-color: #FFFFFF; }
    .team-name { font-weight: 500; }
    .match-time { font-style: italic; color: #6c757d; }
    .odds-badge { font-size: 0.9em; padding: 0.3em 0.6em; border-radius: 0.25rem; }
    .handicap { background-color: #17a2b8; color: white; }
    .goal-line { background-color: #28a745; color: white; }
    .score { background-color: #343a40; color: white; }

    /* Compact styling for preview */
    .preview-card { background-color: #fff; border: 1px solid #ddd; border-radius: 5px; padding: 10px; margin-bottom: 10px; font-size: 0.85rem; }
    .preview-card h6 { font-size: 0.9rem; margin-bottom: 0.5rem; border-bottom: 1px solid #eee; padding-bottom: 0.3rem; }
    .preview-card .score-line { font-size: 1.1rem; font-weight: bold; text-align: center; }
    .preview-card .teams { font-size: 0.8rem; text-align: center; color: #555; }
    .preview-card .date { font-size: 0.75rem; text-align: center; color: #888; margin-bottom: 0.5rem; }
    .stat-table { width: 100%; font-size: 0.8rem; }
    .stat-table td { padding: 2px 4px; }
    .stat-label { text-align: center; color: #666; }
    .stat-value-home { text-align: left; font-weight: bold; }
    .stat-value-away { text-align: right; font-weight: bold; }
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# 3. Main Title and Warning
st.title("StreamlitPRO v2 AI")
st.warning("**Atención:** Esta aplicación realiza web scraping en tiempo real y requiere que Google Chrome y ChromeDriver estén instalados y accesibles en el sistema.")

# 4. Data Loading Functions
@st.cache_data(ttl=300) # Cache for 5 minutes
async def load_all_matches():
    return await scraper.get_all_matches_async()

@st.cache_data(ttl=600) # Cache analysis for 10 minutes
def load_analysis(match_id: str):
    return scraper.get_match_preview_data(match_id)

# 5. Main App Logic
with st.spinner("Cargando lista de partidos en tiempo real..."):
    try:
        all_matches_data = asyncio.run(load_all_matches())
        all_matches = [MatchResult(**m) for m in all_matches_data]
    except Exception as e:
        st.error(f"Error fatal al cargar la lista de partidos: {e}")
        all_matches = []

# Filter matches
SPAIN_TZ = pytz.timezone('Europe/Madrid')
now_spain = datetime.datetime.now(SPAIN_TZ)

upcoming_matches = []
finished_matches = []

for m in all_matches:
    if m.status == "Finalizado":
        finished_matches.append(m)
    elif m.status == "Próximo":
        try:
            match_time = datetime.datetime.strptime(m.time, '%H:%M').time()
            match_datetime = now_spain.replace(hour=match_time.hour, minute=match_time.minute, second=0, microsecond=0)
            if match_datetime < now_spain:
                match_datetime += datetime.timedelta(days=1)
            upcoming_matches.append((m, match_datetime))
        except ValueError:
            continue

upcoming_matches.sort(key=lambda x: x[1])

# 6. UI Rendering
tab1, tab2 = st.tabs(["Próximos Partidos", "Resultados Finalizados"])

def render_stats_table(stats: list[dict]):
    html = "<table class='stat-table'>"
    for stat in stats:
        html += f"<tr><td class='stat-value-home'>{stat.get('home', '')}</td><td class='stat-label'>{stat.get('label', '')}</td><td class='stat-value-away'>{stat.get('away', '')}</td></tr>"
    html += "</table>"
    return html

def render_preview(match_id: str, home_team: str, away_team: str):
    with st.spinner(f"Realizando scraping para el análisis de {home_team} vs {away_team}..."):
        analysis = load_analysis(match_id)

    if not analysis or analysis.get("error"):
        st.error(f"No se pudo cargar el análisis. Error: {analysis.get('error', 'Desconocido')}")
        return

    ri = analysis.get("recent_indirect", {})
    ultimo_local = ri.get("last_home")
    ultimo_visitante = ri.get("last_away")
    h2h_rivales = ri.get("h2h_col3")

    st.markdown("<h6>Rendimiento Reciente</h6>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    with col1:
        if ultimo_local:
            st.markdown(f"<div class='preview-card'><h6>Último {home_team} (Casa)</h6><div class='score-line'>{ultimo_local.get('score', '')}</div><div class='teams'>{ultimo_local.get('home', '')} vs {ultimo_local.get('away', '')}</div><div class='date'>{ultimo_local.get('date', '')}</div>{render_stats_table(ultimo_local.get('stats_rows', []))}</div>", unsafe_allow_html=True)
    with col2:
        if ultimo_visitante:
            st.markdown(f"<div class='preview-card'><h6>Último {away_team} (Fuera)</h6><div class='score-line'>{ultimo_visitante.get('score', '')}</div><div class='teams'>{ultimo_visitante.get('home', '')} vs {ultimo_visitante.get('away', '')}</div><div class='date'>{ultimo_visitante.get('date', '')}</div>{render_stats_table(ultimo_visitante.get('stats_rows', []))}</div>", unsafe_allow_html=True)
    with col3:
        if h2h_rivales:
            st.markdown(f"<div class='preview-card'><h6>H2H Rivales (Col3)</h6><div class='score-line'>{h2h_rivales.get('score_line', '')}</div><div class='date'>{h2h_rivales.get('date', '')}</div>{render_stats_table(h2h_rivales.get('stats_rows', []))}</div>", unsafe_allow_html=True)

with tab1:
    st.header("Próximos Partidos")
    if not upcoming_matches:
        st.warning("No se encontraron partidos próximos.")
    else:
        for match, _ in upcoming_matches:
            with st.expander(f"{match.home_team} vs {match.away_team}"):
                # UI for each match
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write(f"**Hora:** {match.time}")
                with col2:
                    st.markdown(f"**Hándicap:** <span class='badge odds-badge handicap'>{match.handicap}</span>", unsafe_allow_html=True)
                with col3:
                    st.markdown(f"**Línea de Goles:** <span class='badge odds-badge goal-line'>{match.goal_line if match.goal_line else 'N/A'}</span>", unsafe_allow_html=True)
                with col4:
                    if st.button("Vista Previa Ligera", key=f"preview_{match.id}"):
                        render_preview(match.id, match.home_team, match.away_team)

with tab2:
    st.header("Resultados Finalizados")
    if not finished_matches:
        st.warning("No se encontraron partidos finalizados.")
    else:
        for match in finished_matches:
            with st.expander(f"{match.home_team} vs {match.away_team} - {match.score}"):
                # UI for each match
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.write(f"**Hora:** {match.time}")
                with col2:
                    st.markdown(f"**Hándicap:** <span class='badge odds-badge handicap'>{match.handicap}</span>", unsafe_allow_html=True)
                with col3:
                    st.markdown(f"**Línea de Goles:** <span class='badge odds-badge goal-line'>{match.goal_line if match.goal_line else 'N/A'}</span>", unsafe_allow_html=True)
                with col4:
                    if st.button("Vista Previa Ligera", key=f"preview_{match.id}"):
                        render_preview(match.id, match.home_team, match.away_team)
