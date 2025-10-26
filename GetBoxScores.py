import argparse
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

def dedupe(df: pd.DataFrame, level: str) -> pd.DataFrame:
    if df.empty:
        return df
    if level == "player":
        keys = ["gameId", "PLAYER_ID"]
    else:
        keys = ["gameId", "TEAM_ID"]
    return df.drop_duplicates(subset=keys, keep="last")

def save_progress(df: pd.DataFrame, csv_path: Path, ckpt_path: Path, last_gid: str):
    df.to_csv(csv_path, index=False)
    ckpt_path.write_text(last_gid)

# -- main export --

def export_merged_csv(
    seasons_arg: str = "2025-26",
    season_type: str = "Regular Season",
    level: str = "player",
    out_dir: str = "data/csv",
    filename_prefix: str | None = None,
    resume: bool = True,
):
    seasons = parse_seasons_arg(seasons_arg)
    gm = get_games(seasons, season_type)
    if gm.empty:
        print("No games found.")
        return
    gids = game_ids(gm)
    print(f"Found {len(gids)} games.")

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    season_tag = seasons_arg.replace(" ", "")
    type_tag = season_type.replace(" ", "")
    name = filename_prefix or f"boxscores_merged_{level}_{season_tag}_{type_tag}.csv"
    csv_path = Path(out_dir) / name
    ckpt_dir = Path(out_dir) / "checkpoints"
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    ckpt_path = ckpt_dir / f"last_gid_{name}.txt"

    # load existing
    if csv_path.exists():
        out_df = pd.read_csv(csv_path, dtype={"gameId": str})
        out_df = dedupe(out_df, level)
    else:
        out_df = pd.DataFrame()

    start_idx = 0
    if resume and ckpt_path.exists():
        last = ckpt_path.read_text().strip()
        if last and last in gids:
            start_idx = gids.index(last)
            print(f"Resuming from gameId {last} (index {start_idx})")

    for i in range(start_idx, len(gids)):
        gid = gids[i]
        try:
            merged = merge_trad_adv(gid, level)
            out_df = pd.concat([out_df, merged], ignore_index=True)
            out_df = dedupe(out_df, level)
            save_progress(out_df, csv_path, ckpt_path, gid)
            left = len(gids) - i - 1
            print(f"[{i+1}/{len(gids)}] {gid} done. Remaining: {left}")
        except Exception as e:
            print(f"[WARN] {gid} failed: {e}. Saving and backing off.")
            save_progress(out_df, csv_path, ckpt_path, gid)
            time.sleep(20)  # short backoff then continue

    print(f"Done. Wrote {len(out_df):,} rows -> {csv_path}")

def main():
    p = argparse.ArgumentParser(description="Export merged (Traditional+Advanced) NBA box scores to CSV (Tableau-ready).")
    p.add_argument("--seasons", default="2025-26", help="Season or range, e.g. '2025-26' or '2020-2026'")
    p.add_argument("--season-type", default="Regular Season", choices=["Regular Season","Playoffs","Pre Season","All-Star"])
    p.add_argument("--out-dir", default="data/csv")
    p.add_argument("--player-only", action="store_true", help="Export player-level only")
    p.add_argument("--team-only", action="store_true", help="Export team-level only")
    args = p.parse_args()

    if args.player_only and args.team_only:
        print("Choose only one of --player-only or --team-only (or neither to do both).")
        return

    # default: do both
    do_player = True if not args.team_only else False
    do_team   = True if not args.player_only else False

    if do_player:
        export_merged_csv(
            seasons_arg=args.seasons,
            season_type=args.season_type,
            level="player",
            out_dir=args.out_dir,
        )
    if do_team:
        export_merged_csv(
            seasons_arg=args.seasons,
            season_type=args.season_type,
            level="team",
            out_dir=args.out_dir,
        )

if __name__ == "__main__":
    main()

