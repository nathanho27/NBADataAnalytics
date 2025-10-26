import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import nba_api
import time
import os
import openpyxl
import re
from pathlib import Path

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
def parse_seasons_arg(seasons: str):
    """
    Accepts 'YYYY-YY' for single season (e.g., '2024-25'), or 'YYYY-YYYY' range (e.g., '2020-2025').
    Returns list of season strings acceptable to LeagueGameFinder like '2024-25', '2023-24', etc.
    """
    # single season: 2024-25
    single = re.fullmatch(r"(\d{4})-(\d{2})", seasons)
    if single:
        start, end2 = map(int, single.groups())
        if end2 == (start + 1) % 100:
            return [f"{start}-{str((start + 1) % 100).zfill(2)}"]
        raise ValueError("Season end must be start+1 (e.g., 2024-25).")

    # range: 2020-2025  ->  2020-21, 2021-22, ..., 2024-25
    rng = re.fullmatch(r"(\d{4})-(\d{4})", seasons)
    if rng:
        y1, y2 = map(int, rng.groups())
        if y2 <= y1:
            raise ValueError("End year must be greater than start year.")
        out = []
        for y in range(y1, y2):
            out.append(f"{y}-{str((y + 1) % 100).zfill(2)}")
        return out

    raise ValueError("Use 'YYYY-YY' (single) or 'YYYY-YYYY' (range). Examples: '2024-25' or '2020-2025'.")


def get_games_for_seasons(seasons: list[str], season_type: str = "Regular Season") -> pd.DataFrame:
    """
    Use LeagueGameFinder to pull games for given season strings.
    season_type in {'Regular Season', 'Playoffs', 'Pre Season', 'All-Star'}
    Returns a DataFrame with at least GAME_ID, GAME_DATE, TEAM_ID, SEASON_ID.
    """
    frames = []
    for s in seasons:
        print(f"Loading games for {s} ({season_type})")
        finder = leaguegamefinder.LeagueGameFinder(
            season_nullable=s,
            season_type_nullable=season_type,
            league_id_nullable="00"
        )
        df = finder.get_data_frames()[0].copy()
        frames.append(df)
        time.sleep(0.4)  # be polite
    games = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    # normalize types
    if "GAME_DATE" in games.columns:
        games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"])
    return games

