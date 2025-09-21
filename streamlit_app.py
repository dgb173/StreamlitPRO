
import streamlit as st
import asyncio
import pandas as pd
from models import MatchResult
from ai_scraper import scrape_matches

# 1. Page Setup
st.set_page_config(page_title="StreamlitPRO v2 AI", layout="wide")

# 2. Inject Custom CSS
custom_css = """
<style>
    body { background-color: #f8f9fa; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #F0F2F6;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FFFFFF;
    }
    .team-name { font-weight: 500; }
    .match-time { font-style: italic; color: #6c757d; }
    .odds-badge { font-size: 0.9em; padding: 0.3em 0.6em; border-radius: 0.25rem; }
    .handicap { background-color: #17a2b8; color: white; }
    .goal-line { background-color: #28a745; color: white; }
    .score { background-color: #343a40; color: white; }
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

# 3. Main Title
st.title("StreamlitPRO v2 AI")

# 4. Data Loading
@st.cache_data(ttl=300) # Cache data for 5 minutes
async def load_data() -> list[MatchResult]:
    return await scrape_matches()

# Main app logic
try:
    all_matches = asyncio.run(load_data())
except Exception as e:
    st.error(f"Error al cargar los datos: {e}")
    all_matches = []

# 5. Separate data
upcoming_matches = [m for m in all_matches if m.status not in ["Finalizado", "FT"]]
finished_matches = [m for m in all_matches if m.status in ["Finalizado", "FT"]]

# 6. UI Components
tab1, tab2 = st.tabs(["Próximos Partidos", "Resultados Finalizados"])

def display_matches(matches: list[MatchResult], is_finished: bool):
    if not matches:
        st.warning("No se encontraron partidos.")
        return

    # Handicap Filter
    handicap_options = sorted(list(set([m.handicap for m in matches if m.handicap])))
    handicap_filter = st.selectbox("Filtrar por hándicap:", ["Todos"] + handicap_options, key=f"filter_{is_finished}")

    # Display Data
    filtered_matches = matches
    if handicap_filter != "Todos":
        filtered_matches = [m for m in matches if m.handicap == handicap_filter]

    for match in filtered_matches:
        with st.expander(f"{match.home_team} vs {match.away_team}"):
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.write(f"**Liga:** {match.league}")
            with col2:
                st.write(f"**Hora:** {match.time}")
                st.write(f"**Estado:** {match.status}")
            with col3:
                if is_finished:
                    st.markdown(f"**Resultado:** <span class='badge odds-badge score'>{match.score}</span>", unsafe_allow_html=True)
            with col4:
                st.markdown(f"**Hándicap:** <span class='badge odds-badge handicap'>{match.handicap}</span>", unsafe_allow_html=True)
            with col5:
                st.markdown(f"**Línea de Goles:** <span class='badge odds-badge goal-line'>{match.goal_line if match.goal_line else 'N/A'}</span>", unsafe_allow_html=True)
            
            st.write("Aquí irá el análisis detallado del partido.")

with tab1:
    st.header("Próximos Partidos")
    display_matches(upcoming_matches, is_finished=False)

with tab2:
    st.header("Resultados Finalizados")
    display_matches(finished_matches, is_finished=True)

