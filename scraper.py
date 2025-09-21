# scraper.py
import asyncio
from modules.estudio_scraper import obtener_datos_preview_ligero
from modules.estudio_scraper import obtener_datos_completos_partido # This is a placeholder for the actual main page scraping logic

# This is a placeholder for the actual main page scraping logic from the Flask app.
# The original Flask app had complex logic for this in the app.py itself.
# For now, we will return a mock list of matches, but the preview will be real.

def get_upcoming_matches():
    # In a real implementation, this would call the logic from the old app.py's 
    # get_main_page_matches_async function.
    # For now, returning a static list to allow the UI to be built.
    return [
        {"id": "2433945", "time": "22:00", "home_team": "CA Atlanta", "away_team": "Almirante Brown", "handicap": "0.25", "goal_line": "2.0"},
        {"id": "2433946", "time": "23:00", "home_team": "Club Atletico Mitre", "away_team": "Gimnasia y Tiro", "handicap": "0.5", "goal_line": "2.25"},
    ]

def get_finished_matches():
    return [
        {"id": "2433947", "time": "FT", "home_team": "Racing Club", "away_team": "Godoy Cruz", "handicap": "-0.75", "goal_line": "2.5", "score": "1-0"},
    ]

def get_match_preview_data(match_id: str):
    """
    Calls the lightweight scraper to get the preview data.
    """
    try:
        data = obtener_datos_preview_ligero(match_id)
        return data
    except Exception as e:
        print(f"Error in get_match_preview_data for match_id {match_id}: {e}")
        return {"error": str(e)}
