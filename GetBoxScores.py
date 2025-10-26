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
boxscores.py — simplest NBA player box-score exporter for Tableau (2025-26)

What it does
• Pulls all games for a season (Regular Season or Playoffs)
• Fetches per-game, per-player traditional box scores
• Adds game context (date, matchup, win/loss, optional game_number)
• Writes ONE clean CSV: exports/players_YYYY-YY_{regular|playoffs}.csv

"""
def parse_seasons_arg(seasons: str):
    """
    'YYYY-YY' -> single season (e.g., '2025-26')
    'YYYY-YYYY' -> range (e.g., '2020-2026') -> ['2020-21', ... '2025-26']
    """
    single = re.fullmatch(r"(\d{4})-(\d{2})", seasons)
    if single:
        start, end2 = map(int, single.groups())
        if end2 == (start + 1) % 100:
            return [f"{start}-{str((start + 1) % 100).zfill(2)}"]
        raise ValueError("Season end must be start+1, e.g. 2025-26.")
    rng = re.fullmatch(r"(\d{4})-(\d{4})", seasons)
    if rng:
        y1, y2 = map(int, rng.groups())
        if y2 <= y1:
            raise ValueError("End year must be greater than start year.")
        return [f"{y}-{str((y + 1) % 100).zfill(2)}" for y in range(y1, y2)]
    raise ValueError("Use 'YYYY-YY' (single) or 'YYYY-YYYY' (range). Examples: '2025-26', '2020-2026'.")

def get_games(seasons: list[str], season_type: str) -> pd.DataFrame:
    frames = []
    for s in seasons:
        print(f"Loading games for {s} ({season_type})")
        finder = leaguegamefinder.LeagueGameFinder(
            season_nullable=s,
            season_type_nullable=season_type,
            league_id_nullable="00",
        )
        df = finder.get_data_frames()[0].copy()
        frames.append(df)
        time.sleep(0.35)  # be polite
    games = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if "GAME_DATE" in games.columns:
        games["GAME_DATE"] = pd.to_datetime(games["GAME_DATE"])
    return games

def game_ids(games: pd.DataFrame) -> list[str]:
    return games["GAME_ID"].astype(str).unique().tolist()

def fetch_box(game_id: str, level: str, advanced: bool) -> pd.DataFrame:
    """
    level: 'player' -> index 0, 'team' -> index 1
    advanced: True = AdvancedV3, False = TraditionalV3
    """
    idx = 0 if level == "player" else 1
    box = BoxScoreAdvancedV3(game_id=game_id) if advanced else BoxScoreTraditionalV3(game_id=game_id)
    df = box.get_data_frames()[idx].copy()
    df["gameId"] = game_id  # consistent key
    return df

def merge_trad_adv(gid: str, level: str) -> pd.DataFrame:
    """
    Merge Traditional + Advanced for a single game at requested level.
    Player merge keys: ['gameId','PLAYER_ID','TEAM_ID'] (all present on v3)
    Team merge keys:   ['gameId','TEAM_ID']
    """
    time.sleep(0.6)
    trad = fetch_box(gid, level, advanced=False)
    adv  = fetch_box(gid, level, advanced=True)

    if level == "player":
        keys = ["gameId", "PLAYER_ID", "TEAM_ID"]
    else:
        keys = ["gameId", "TEAM_ID"]

    # avoid duplicate columns on merge
    shared = set(trad.columns).intersection(set(adv.columns)) - set(keys)
    trad = trad.rename(columns={c: f"{c}_trad" for c in shared})
    adv  = adv.rename(columns={c: f"{c}_adv"  for c in shared})

    out = pd.merge(trad, adv, on=keys, how="outer")
    return out