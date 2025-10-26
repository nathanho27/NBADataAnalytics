import os
import re
import time
import numpy as np
import pandas as pd

from nba_api.stats.endpoints import (
    BoxScoreTraditionalV3,
    BoxScoreAdvancedV3,
    leaguegamefinder,
)

def _season_iter(seasons: str):
    """
    Accepts:
      - 'YYYY-YY' for a single season (e.g., '2025-26')
      - 'YYYY-YYYY' for an inclusive range of start years (e.g., '2020-2026')
        which expands to: ['2020-21','2021-22',...,'2025-26']

    Returns a list like ['2025-26'] or multiple seasons.
    """
    range_match = re.match(r"^(\d{4})-(\d{2}|\d{4})$", seasons)
    if not range_match:
        raise ValueError('Use "YYYY-YY" or "YYYY-YYYY", e.g., "2025-26" or "2020-2026"')

    start, end = range_match.groups()
    start = int(start)

    # Case 1: single season like 2025-26 (end is '26')
    if len(end) == 2:
        return [f"{start}-{int(end):02d}"]

    # Case 2: multi-season where end is full year like 2026
    end = int(end)
    out = []
    for y in range(start, end + 1):
        out.append(f"{y}-{(y+1) % 100:02d}")
    return out


def get_games(seasons='2025-26', season_type='Regular Season', league_id='00') -> pd.DataFrame:
    """
    Returns a tidy Games DataFrame for the given season(s) and season_type.
    Primary keys: (GAME_ID, TEAM_ID) when joined to team-level box scores.
    """
    all_games = []
    for s in _season_iter(seasons):
        print(f"Loading games for {s} • {season_type}")
        gf = leaguegamefinder.LeagueGameFinder(
            season_nullable=s,
            league_id_nullable=league_id,
            season_type_nullable=season_type
        )
        g = gf.get_data_frames()[0]
        # Defensive: ensure GAME_DATE is datetime, GAME_ID is string
        g["GAME_DATE"] = pd.to_datetime(g["GAME_DATE"], errors="coerce")
        g["GAME_ID"] = g["GAME_ID"].astype(str)
        all_games.append(g)

    if not all_games:
        return pd.DataFrame()

    games = pd.concat(all_games, ignore_index=True)

    # Keep a consistent minimal set first for convenience in BI tools
    keep_first = [
        "SEASON_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME", "GAME_ID",
        "GAME_DATE", "MATCHUP", "WL", "PTS"
    ]
    existing = [c for c in keep_first if c in games.columns]
    return games[existing + [c for c in games.columns if c not in existing]].copy()


def add_game_number(games: pd.DataFrame, playoffs: bool = False) -> pd.DataFrame:
    """
    Adds GAME_NUMBER and GAME_NUMBER_REV by team x season.
    For playoffs=True, numbers continue after 82 (i.e., 83, 84, ...).
    """
    if games.empty:
        return games

    df = games.copy()
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    df = df.sort_values(["TEAM_ID", "SEASON_ID", "GAME_DATE"])

    grp = df.groupby(["TEAM_ID", "SEASON_ID"], sort=False)
    if playoffs:
        df["GAME_NUMBER"] = 82 + grp.cumcount() + 1
        df["GAME_NUMBER_REV"] = 82 + grp["TEAM_ID"].transform("size") - grp.cumcount()
    else:
        df["GAME_NUMBER"] = grp.cumcount() + 1
        df["GAME_NUMBER_REV"] = grp["TEAM_ID"].transform("size") - grp.cumcount()

    return df


def get_game_ids(games: pd.DataFrame):
    """Unique game IDs as a Python list of strings."""
    return games["GAME_ID"].astype(str).unique().tolist()


def _ensure_dirs(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _read_existing_csv(path: str, key_col="gameId") -> pd.DataFrame:
    if os.path.exists(path):
        df = pd.read_csv(path, dtype={key_col: str})
        # normalize key col spelling across endpoints (should already be 'gameId')
        if key_col not in df.columns:
            for cand in ["GAME_ID", "game_id", "Game_ID"]:
                if cand in df.columns:
                    df = df.rename(columns={cand: key_col})
                    break
        df[key_col] = df[key_col].astype(str)
        return df
    else:
        return pd.DataFrame({key_col: pd.Series(dtype="object")})


def _clean_for_tableau(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light, safe cleaning for Tableau Public:
    - enforce gameId as string
    - parse common datetime columns if present (soft attempt)
    - drop exact duplicate rows
    """
    out = df.copy()
    if "gameId" in out.columns:
        out["gameId"] = out["gameId"].astype(str)

    # Parse columns that look like date/timestamp (best effort, not strict)
    for col in [c for c in out.columns if c.lower().endswith(("date", "timestamp"))]:
        out[col] = pd.to_datetime(out[col], errors="ignore")

    out = out.drop_duplicates().reset_index(drop=True)
    return out


def get_historical_boxscores(
    game_ids,
    season_token: str,
    playoffs: bool = False,
    time_buffer: float = 0.6,
    advanced: bool = False,
    team: bool = False,
    out_dir: str = "exports"
) -> str:
    """
    Pulls many box scores and saves ONE Tableau-ready CSV.

    Parameters
    ----------
    game_ids : list[str]
        GAME_ID values to fetch.
    season_token : str
        Label for the output filename, e.g. '2025-26' or '2020-2026'.
    playoffs : bool
        If True, filename includes _playoffs.
    time_buffer : float
        Sleep between requests to reduce timeouts.
    advanced : bool
        If True, use BoxScoreAdvancedV3, else BoxScoreTraditionalV3.
    team : bool
        If True, use team-level table (index=1), else player-level (index=0).
    out_dir : str
        Output directory.

    Returns
    -------
    str
        Full path of the written CSV.
    """
    if time_buffer < 0.01:
        time_buffer = 0.01

    df_index = 1 if team else 0
    team_label = "team" if team else "player"
    mode_label = "adv" if advanced else "trad"
    p_label = "_playoffs" if playoffs else ""

    out_path = os.path.join(out_dir, f"{mode_label}_boxscores_{season_token}_{team_label}{p_label}.csv")
    last_id_path = os.path.join(out_dir, f"last_game_id_{season_token}_{team_label}{p_label}.txt")
    _ensure_dirs(out_path)

    # Load prior
    all_data = _read_existing_csv(out_path, key_col="gameId")
    done_ids = set(all_data["gameId"].astype(str)) if not all_data.empty else set()

    remaining = [gid for gid in game_ids if str(gid) not in done_ids]

    # Resume if marker exists
    if os.path.exists(last_id_path):
        try:
            with open(last_id_path, "r") as fh:
                last_gid = fh.read().strip()
            if last_gid in remaining:
                last_idx = remaining.index(last_gid)
                remaining = remaining[last_idx:]
                print(f"Resuming from gameId {last_gid}")
        except Exception:
            pass

    print(f"Starting box score fetch • mode={mode_label}, team={team}, playoffs={playoffs}")
    print(f"Already have {len(done_ids)} games, {len(remaining)} remaining")

    for i, gid in enumerate(remaining, 1):
        try:
            bs = BoxScoreAdvancedV3(game_id=gid) if advanced else BoxScoreTraditionalV3(game_id=gid)
            df = bs.get_data_frames()[df_index].copy()

            # Ensure gameId text column
            if "gameId" in df.columns:
                df["gameId"] = df["gameId"].astype(str)
            else:
                for cand in ["GAME_ID", "game_id", "Game_ID"]:
                    if cand in df.columns:
                        df = df.rename(columns={cand: "gameId"})
                        df["gameId"] = df["gameId"].astype(str)
                        break

            all_data = pd.concat([all_data, df], ignore_index=True)

            left = len(remaining) - i
            print(f"[{i}/{len(remaining)}] {gid} • left: {left}")
            time.sleep(time_buffer)

            # Periodic checkpoint
            if (i % 100 == 0) or (i == len(remaining)):
                tmp = _clean_for_tableau(all_data)
                tmp.to_csv(out_path, index=False)
                with open(last_id_path, "w") as fh:
                    fh.write(str(gid))

        except Exception as e:
            # Save progress and back off
            print(f"Error on gameId={gid}: {e} • Saving progress to {out_path}")
            _clean_for_tableau(all_data).to_csv(out_path, index=False)
            with open(last_id_path, "w") as fh:
                fh.write(str(gid))
            time_buffer = round(min(time_buffer * 1.5, 10.0), 2)
            print(f"Increasing time_buffer to {time_buffer} seconds, pausing…")
            time.sleep(max(30.0, time_buffer))

    # Final write
    final_df = _clean_for_tableau(all_data)
    final_df.to_csv(out_path, index=False)
    print(f"Finished: wrote {len(final_df):,} rows → {out_path}")
    return out_path


def update_boxscores(
    game_ids,
    out_csv: str,
    time_buffer: float = 0.6,
    advanced: bool = False,
    team: bool = False
) -> str:
    """
    Incrementally append missing gameIds to an existing CSV (or create it).
    """
    season_token = os.path.splitext(os.path.basename(out_csv))[0]
    out_dir = os.path.dirname(out_csv) or "."
    return get_historical_boxscores(
        game_ids=game_ids,
        season_token=season_token,
        playoffs=False,  # for playoffs targets, call get_historical_boxscores with playoffs=True
        time_buffer=time_buffer,
        advanced=advanced,
        team=team,
        out_dir=out_dir,
    )

def update_all(season="2025-26", league_id="00"):
    """
    One-click refresher for the given season:
      - Saves games CSVs (regular + playoffs)
      - Updates traditional & advanced, player & team boxscores for both segments
    """
    os.makedirs("exports", exist_ok=True)
    os.makedirs("exports/games", exist_ok=True)

    # Regular season games
    games = get_games(season, season_type="Regular Season", league_id=league_id)
    games = add_game_number(games, playoffs=False)
    games.to_csv(f"exports/games/games_{season}_regular.csv", index=False)
    reg_ids = get_game_ids(games)

    # Playoff games
    pgames = get_games(season, season_type="Playoffs", league_id=league_id)
    pgames = add_game_number(pgames, playoffs=True)
    pgames.to_csv(f"exports/games/games_{season}_playoffs.csv", index=False)
    ply_ids = get_game_ids(pgames)

    # Regular season box scores → Tableau-ready CSVs
    get_historical_boxscores(reg_ids, season, playoffs=False, advanced=False, team=False, out_dir="exports")  # trad player
    get_historical_boxscores(reg_ids, season, playoffs=False, advanced=False, team=True,  out_dir="exports")  # trad team
    get_historical_boxscores(reg_ids, season, playoffs=False, advanced=True,  team=False, out_dir="exports")  # adv player
    get_historical_boxscores(reg_ids, season, playoffs=False, advanced=True,  team=True,  out_dir="exports")  # adv team

    # Playoffs box scores → Tableau-ready CSVs
    get_historical_boxscores(ply_ids, season, playoffs=True, advanced=False, team=False, out_dir="exports")   # trad player
    get_historical_boxscores(ply_ids, season, playoffs=True, advanced=False, team=True,  out_dir="exports")   # trad team
    get_historical_boxscores(ply_ids, season, playoffs=True, advanced=True,  team=False, out_dir="exports")   # adv player
    get_historical_boxscores(ply_ids, season, playoffs=True, advanced=True,  team=True,  out_dir="exports")   # adv team

    print("Finished updating all CSVs for Tableau.")


if __name__ == "__main__":
    # EXAMPLE A: Single season, traditional PLAYER box scores, regular season only (2025-26)
    games = get_games("2025-26", season_type="Regular Season")
    game_ids = get_game_ids(games)
    get_historical_boxscores(
    game_ids,
    season_token="2025-26",
    playoffs=False,
    advanced=False,
    team=False,
    out_dir="exports"
    )

    # EXAMPLE B: Playoffs, advanced PLAYER box scores for 2025-26
    # p_games = get_games("2025-26", season_type="Playoffs")
    # p_game_ids = get_game_ids(p_games)
    # get_historical_boxscores(
    #     p_game_ids,
    #     season_token="2025-26",
    #     playoffs=True,
    #     advanced=True,
    #     team=False,
    #     out_dir="exports"
    # )

    # EXAMPLE C: One-shot update for everything in 2025-26
    # update_all("2025-26")
    pass
