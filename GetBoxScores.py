import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import nba_api
import time
import os
import openpyxl
import re

from nba_api.stats.endpoints import BoxScoreTraditionalV3
from nba_api.stats.endpoints import BoxScoreAdvancedV3
from nba_api.stats.endpoints import leaguegamefinder

"""
boxscores.py — simplest NBA player box-score exporter for Tableau (2025–26)

What it does
• Pulls all games for a season (Regular Season or Playoffs)
• Fetches per-game, per-player traditional box scores
• Adds game context (date, matchup, win/loss, optional game_number)
• Writes ONE clean CSV: exports/players_YYYY-YY_{regular|playoffs}.csv

"""
def _check_season_str(season: str) -> str:
    """
    Validate 'YYYY-YY' format (e.g., '2025-26') to catch typos early.
    This prevents confusing errors from the NBA API later.
    """
    if not re.match(r"^\d{4}-\d{2}$", season):
        raise ValueError('Season must look like "2025-26" (YYYY-YY).')
    return season

