import requests
import pandas as pd
from datetime import date

BASE_URL = "https://statsapi.mlb.com/api/v1"

SEASON_START = date(2026, 3, 25)  # 2026 MLB Opening Day


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

    def get_season_schedule(self, start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """Full season schedule from Opening Day to today."""
        start = start_date or SEASON_START
        end   = end_date   or date.today()
        data  = self._get("/schedule", params={
            "sportId":   1,
            "startDate": start.strftime("%Y-%m-%d"),
            "endDate":   end.strftime("%Y-%m-%d"),
            "gameType":  "R",
            "hydrate":   "linescore",
        })
        rows = []
        for day in data.get("dates", []):
            for game in day.get("games", []):
                rows.append({
                    "game_pk":      game["gamePk"],
                    "game_date":    game["gameDate"],
                    "status":       game["status"]["detailedState"],
                    "home_team_id": game["teams"]["home"]["team"]["id"],
                    "home_team":    game["teams"]["home"]["team"]["name"],
                    "away_team_id": game["teams"]["away"]["team"]["id"],
                    "away_team":    game["teams"]["away"]["team"]["name"],
                    "home_score":   game["teams"]["home"].get("score"),
                    "away_score":   game["teams"]["away"].get("score"),
                    "venue":        game.get("venue", {}).get("name"),
                })
        return pd.DataFrame(rows)

    def get_hitting_stats(self) -> pd.DataFrame:
        """Official MLB season hitting stats for all players."""
        data = self._get("/stats", params={
            "stats":      "season",
            "group":      "hitting",
            "season":     self.season,
            "sportId":    1,
            "limit":      2000,
            "playerPool": "All",
        })
        rows = []
        for split in data.get("stats", [{}])[0].get("splits", []):
            player = split.get("player", {})
            team   = split.get("team", {})
            stat   = split.get("stat", {})
            rows.append({
                "player_id":         player.get("id"),
                "player_name":       player.get("fullName"),
                "team_id":           team.get("id"),
                "team_name":         team.get("name"),
                "games":             stat.get("gamesPlayed"),
                "at_bats":           stat.get("atBats"),
                "plate_appearances": stat.get("plateAppearances"),
                "hits":              stat.get("hits"),
                "doubles":           stat.get("doubles"),
                "triples":           stat.get("triples"),
                "home_runs":         stat.get("homeRuns"),
                "rbi":               stat.get("rbi"),
                "walks":             stat.get("baseOnBalls"),
                "strikeouts":        stat.get("strikeOuts"),
                "stolen_bases":      stat.get("stolenBases"),
                "batting_avg":       stat.get("avg"),
                "obp":               stat.get("obp"),
                "slg":               stat.get("slg"),
                "ops":               stat.get("ops"),
                "season":            self.season,
            })
        return pd.DataFrame(rows)

    def get_pitching_stats(self) -> pd.DataFrame:
        """Official MLB season pitching stats for all players."""
        data = self._get("/stats", params={
            "stats":      "season",
            "group":      "pitching",
            "season":     self.season,
            "sportId":    1,
            "limit":      2000,
            "playerPool": "All",
        })
        rows = []
        for split in data.get("stats", [{}])[0].get("splits", []):
            player = split.get("player", {})
            team   = split.get("team", {})
            stat   = split.get("stat", {})
            rows.append({
                "player_id":         player.get("id"),
                "player_name":       player.get("fullName"),
                "team_id":           team.get("id"),
                "team_name":         team.get("name"),
                "games":             stat.get("gamesPlayed"),
                "games_started":     stat.get("gamesStarted"),
                "wins":              stat.get("wins"),
                "losses":            stat.get("losses"),
                "saves":             stat.get("saves"),
                "innings_pitched":   stat.get("inningsPitched"),
                "hits_allowed":      stat.get("hits"),
                "earned_runs":       stat.get("earnedRuns"),
                "walks_allowed":     stat.get("baseOnBalls"),
                "strikeouts":        stat.get("strikeOuts"),
                "home_runs_allowed": stat.get("homeRuns"),
                "era":               stat.get("era"),
                "whip":              stat.get("whip"),
                "k_per_9":           stat.get("strikeoutsPer9Inn"),
                "bb_per_9":          stat.get("walksPer9Inn"),
                "season":            self.season,
            })
        return pd.DataFrame(rows)

    def get_game_feed(self, game_pk: int) -> dict:
        # Game feed lives under v1.1, not v1
        url = f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
        response = self.session.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
