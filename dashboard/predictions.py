"""
HR prediction engine for the MLB DiamondPipeline dashboard.

Model (simplified log5):
    hr_prob = (batter_hr_rate * pitcher_hr_rate / league_avg) * park_factor

Inputs:
  batter_hr_rate   = season HR / PA          (from analytics.mart_player_stats)
  pitcher_hr_rate  = HR allowed / BF (est.)  (from analytics.mart_pitching_leaders)
  park_factor      = static lookup by venue  (historical HR park factors)
  league_avg       = 0.034                   (3.4% per PA — modern MLB baseline)

Probable pitchers are fetched live from the MLB Stats API since they change daily
and are not part of the batch pipeline.
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())

# ── Constants ─────────────────────────────────────────────────────────────────

LEAGUE_AVG_HR_PER_PA: float = 0.034   # modern MLB average
BATTERS_PER_INNING:   float = 4.3     # approximate (walks/hits push above 3)
MIN_PA:               int   = 20      # exclude batters with too few PAs
MAX_HR_PROB:          float = 0.25    # cap probability at 25%
REGRESSION_PA:        int   = 200     # PA needed for HR rate to fully stabilize

# Historical HR park factors (1.0 = league average).
# Source: multi-year park factor averages adjusted for ballpark dimensions.
PARK_FACTORS: dict[str, float] = {
    "Coors Field":                  1.35,
    "Great American Ball Park":     1.22,
    "Guaranteed Rate Field":        1.18,
    "Citizens Bank Park":           1.14,
    "Fenway Park":                  1.10,
    "Yankee Stadium":               1.08,
    "Globe Life Field":             1.06,
    "Truist Park":                  1.04,
    "Chase Field":                  1.02,
    "Wrigley Field":                1.00,
    "American Family Field":        1.00,
    "Progressive Field":            0.99,
    "Minute Maid Park":             0.99,
    "Angel Stadium":                0.98,
    "Target Field":                 0.97,
    "Comerica Park":                0.97,
    "Camden Yards":                 0.96,
    "Kauffman Stadium":             0.94,
    "Busch Stadium":                0.92,
    "loanDepot park":               0.90,
    "PNC Park":                     0.89,
    "Nationals Park":               0.89,
    "T-Mobile Park":                0.88,
    "Oracle Park":                  0.86,
    "Petco Park":                   0.83,
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _engine():
    return create_engine(
        "postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "mlb"),
            pw=os.getenv("POSTGRES_PASSWORD", "mlbpassword"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5433"),
            db=os.getenv("POSTGRES_DB", "mlb_pipeline"),
        ),
        pool_pre_ping=True,
    )


def _parse_innings(ip_str: str | None) -> float:
    """Convert baseball innings notation '62.1' → 62.333, '62.2' → 62.667."""
    if not ip_str:
        return 0.0
    try:
        parts = str(ip_str).split(".")
        whole = int(parts[0])
        frac  = int(parts[1]) / 3 if len(parts) > 1 else 0
        return whole + frac
    except (ValueError, IndexError):
        return 0.0


# ── Core functions ────────────────────────────────────────────────────────────

def get_probable_pitchers(game_date: date = None) -> pd.DataFrame:
    """
    Fetch today's probable starters from the MLB API.
    Returns one row per team-side with probable pitcher info.
    """
    game_date = game_date or date.today()
    try:
        r = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={
                "sportId": 1,
                "date":    game_date.strftime("%Y-%m-%d"),
                "hydrate": "probablePitcher",
                "gameType": "R",
            },
            timeout=10,
        )
        r.raise_for_status()
    except Exception as exc:
        print(f"[predictions] probable pitcher API error: {exc}")
        return pd.DataFrame()

    rows = []
    for day in r.json().get("dates", []):
        for game in day.get("games", []):
            venue = game.get("venue", {}).get("name", "")
            home  = game["teams"]["home"]["team"]["name"]
            away  = game["teams"]["away"]["team"]["name"]

            # Each game → two rows (one per batting team)
            for batting_side, pitching_side in [("away", "home"), ("home", "away")]:
                pitcher = game["teams"][pitching_side].get("probablePitcher") or {}
                rows.append({
                    "game_pk":        game["gamePk"],
                    "venue":          venue,
                    "matchup":        f"{away} @ {home}",
                    "batting_team":   game["teams"][batting_side]["team"]["name"],
                    "batting_team_id": game["teams"][batting_side]["team"]["id"],
                    "pitcher_id":     pitcher.get("id"),
                    "pitcher_name":   pitcher.get("fullName", "TBD"),
                    "game_status":    game["status"]["detailedState"],
                })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def build_hr_leaderboard(game_date: date = None) -> pd.DataFrame:
    """
    Compute today's HR probability for every qualifying batter vs their
    probable opposing pitcher, adjusted for park factor.

    Returns a ranked DataFrame ready for the dashboard.
    """
    game_date = game_date or date.today()

    matchups = get_probable_pitchers(game_date)
    if matchups.empty:
        return pd.DataFrame()

    # Filter to games with a known probable pitcher
    matchups = matchups[matchups["pitcher_id"].notna()]
    if matchups.empty:
        return pd.DataFrame()

    engine = _engine()
    try:
        with engine.connect() as conn:
            batters = pd.read_sql(text("""
                SELECT player_id, player_name, team_id, team_name,
                       home_runs, plate_appearances, batting_avg, ops
                FROM analytics.mart_player_stats
                WHERE plate_appearances >= :min_pa
            """), conn, params={"min_pa": MIN_PA})

            pitchers = pd.read_sql(text("""
                SELECT player_id, player_name,
                       home_runs_allowed, innings_pitched
                FROM analytics.mart_pitching_leaders
                WHERE role = 'Starter'
            """), conn)
    except Exception as exc:
        print(f"[predictions] DB query error: {exc}")
        return pd.DataFrame()

    # Pitcher HR rate per estimated batter faced
    pitchers["ip_dec"] = pitchers["innings_pitched"].apply(_parse_innings)
    pitchers["bf_est"] = pitchers["ip_dec"] * BATTERS_PER_INNING
    pitchers["pitcher_hr_rate"] = (
        pitchers["home_runs_allowed"]
        / pitchers["bf_est"].replace(0, float("nan"))
    ).fillna(LEAGUE_AVG_HR_PER_PA)

    # Batter HR rate with regression to the mean — prevents early-season small
    # samples from dominating (a player with 2 HR in 22 PA looks like a 9% HR
    # hitter without this; REGRESSION_PA controls how quickly we trust the rate).
    batters["batter_hr_rate"] = (
        (batters["home_runs"] + LEAGUE_AVG_HR_PER_PA * REGRESSION_PA)
        / (batters["plate_appearances"] + REGRESSION_PA)
    )

    rows = []
    for _, matchup in matchups.iterrows():
        park   = PARK_FACTORS.get(matchup["venue"], 1.0)
        p_row  = pitchers[pitchers["player_id"] == matchup["pitcher_id"]]
        p_rate = p_row.iloc[0]["pitcher_hr_rate"] if not p_row.empty else LEAGUE_AVG_HR_PER_PA
        p_name = p_row.iloc[0]["player_name"] if not p_row.empty else matchup["pitcher_name"]

        team_batters = batters[batters["team_id"] == matchup["batting_team_id"]]
        for _, b in team_batters.iterrows():
            prob = (b["batter_hr_rate"] * p_rate / LEAGUE_AVG_HR_PER_PA) * park
            prob = min(prob, MAX_HR_PROB)

            rows.append({
                "batter_name":   b["player_name"],
                "team":          b["team_name"],
                "pitcher":       p_name,
                "matchup":       matchup["matchup"],
                "venue":         matchup["venue"],
                "park_factor":   round(park, 2),
                "hr_prob":       round(prob, 4),
                "hr_prob_pct":   f"{prob * 100:.1f}%",
                "season_hr":     int(b["home_runs"]),
                "season_pa":     int(b["plate_appearances"]),
                "batting_avg":   b["batting_avg"],
                "ops":           b["ops"],
            })

    if not rows:
        return pd.DataFrame()

    return (
        pd.DataFrame(rows)
        .sort_values("hr_prob", ascending=False)
        .reset_index(drop=True)
    )
