# models.py
from pydantic import BaseModel
from typing import List, Optional

class MatchResult(BaseModel):
    id: str
    league: str
    time: str
    status: str
    home_team: str
    score: str
    away_team: str
    handicap: Optional[str] = None
    goal_line: Optional[str] = None
