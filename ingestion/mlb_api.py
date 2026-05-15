import requests
import pandas as pd
from datetime import date

BASE_URL = "https://statsapi.mlb.com/api/v1"


class MLBApiClient:
    def __init__(self, season: int = None):
        self.season = season or date.today().year
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "mlb-diamond-pipeline/1.0"})

    def _get(self, endpoint: str, params: dict = None) -> dict:
        url = f"{BASE_URL}{endpoint}"
        response = self.session.get(url, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    def get_teams(self) -> pd.DataFrame:
        data = self._get("/teams", params={"sportId": 1, "season": self.season})
        rows = []
        for team in data.get("teams", []):
            rows.append({
                "team_id": team["id"],
                "team_name": team["name"],
                "abbreviation": team.get("abbreviation"),
                "team_code": team.get("teamCode"),
                "division_id": team.get("division", {}).get("id"),
                "division_name": team.get("division", {}).get("name"),
                "league_id": team.get("league", {}).get("id"),
                "league_name": team.get("league", {}).get("name"),
                "venue_id": team.get("venue", {}).get("id"),
                "venue_name": team.get("venue", {}).get("name"),
                "season": self.season,
            })
        return pd.DataFrame(rows)

    # Static maps — division/league IDs are stable across seasons
    _DIVISION_NAMES = {
        200: "AL West", 201: "AL East", 202: "AL Central",
        203: "NL West", 204: "NL East", 205: "NL Central",
    }
    _LEAGUE_NAMES = {103: "American League", 104: "National League"}

    def get_standings(self) -> pd.DataFrame:
        data = self._get("/standings", params={
            "leagueId": "103,104",
            "season": self.season,
            "standingsTypes": "regularSeason",
        })
        rows = []
        for record in data.get("records", []):
            division_id = record.get("division", {}).get("id")
            league_id = record.get("league", {}).get("id")
            division = self._DIVISION_NAMES.get(division_id, str(division_id))
            league = self._LEAGUE_NAMES.get(league_id, str(league_id))
            for tr in record.get("teamRecords", []):
                lr = tr.get("leagueRecord", {})
                rows.append({
                    "team_id": tr["team"]["id"],
                    "team_name": tr["team"]["name"],
                    "division_id": division_id,
                    "division": division,
                    "league_id": league_id,
                    "league": league,
                    "wins": int(lr.get("wins", 0)),
                    "losses": int(lr.get("losses", 0)),
                    "win_pct": float(lr.get("pct", "0").replace(".000", "0")),
                    "games_back": tr.get("gamesBack", "-"),
                    "games_played": int(tr.get("gamesPlayed", 0)),
                    "season": self.season,
                })
        return pd.DataFrame(rows)

    def get_schedule(self, game_date: date = None) -> pd.DataFrame:
        game_date = game_date or date.today()
        data = self._get("/schedule", params={
            "sportId": 1,
            "date": game_date.strftime("%Y-%m-%d"),
            "hydrate": "linescore",
        })
        rows = []
        for day in data.get("dates", []):
            for game in day.get("games", []):
                rows.append({
                    "game_pk": game["gamePk"],
                    "game_date": game["gameDate"],
                    "status": game["status"]["detailedState"],
                    "home_team_id": game["teams"]["home"]["team"]["id"],
                    "home_team": game["teams"]["home"]["team"]["name"],
                    "away_team_id": game["teams"]["away"]["team"]["id"],
                    "away_team": game["teams"]["away"]["team"]["name"],
                    "home_score": game["teams"]["home"].get("score"),
                    "away_score": game["teams"]["away"].get("score"),
                    "venue": game.get("venue", {}).get("name"),
                })
        return pd.DataFrame(rows)

    def get_game_feed(self, game_pk: int) -> dict:
        # Game feed lives under v1.1, not v1
        url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
