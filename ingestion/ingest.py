import pandas as pd
from datetime import date

from ingestion.mlb_api import MLBApiClient
from ingestion.db import load_dataframe


def validate(df: pd.DataFrame, name: str, required_cols: list[str]) -> None:
    if df.empty:
        print(f"  [warn] {name}: no data returned from API")
        return
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing expected columns {missing}")
    null_pct = df[required_cols].isnull().mean() * 100
    flagged = null_pct[null_pct > 0]
    if not flagged.empty:
        print(f"  [warn] {name} null rates (%):\n{flagged.to_string()}")
    print(f"  [ok] {name}: {len(df)} rows, {len(df.columns)} columns")


def run_ingestion() -> None:
    client = MLBApiClient()
    print(f"Starting MLB ingestion — season {client.season}\n")

    print("Teams...")
    teams = client.get_teams()
    validate(teams, "teams", ["team_id", "team_name", "division_name", "league_name"])
    load_dataframe(teams, "raw_teams")

    print("Standings...")
    standings = client.get_standings()
    validate(standings, "standings", ["team_id", "team_name", "wins", "losses", "win_pct"])
    load_dataframe(standings, "raw_standings")

    print("Schedule (today)...")
    schedule = client.get_schedule(date.today())
    validate(schedule, "schedule", ["game_pk", "home_team_id", "away_team_id", "status"])
    load_dataframe(schedule, "raw_schedule")

    print("\nIngestion complete.")


if __name__ == "__main__":
    run_ingestion()
