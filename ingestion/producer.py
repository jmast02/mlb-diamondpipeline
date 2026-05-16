import json
import os
from datetime import date, timedelta
from dotenv import load_dotenv, find_dotenv
from confluent_kafka import Producer

from ingestion.mlb_api import MLBApiClient

load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_TEAMS          = "mlb.teams"
TOPIC_STANDINGS      = "mlb.standings"
TOPIC_SCHEDULE       = "mlb.schedule"
TOPIC_GAME_EVENTS    = "mlb.game_events"
TOPIC_HITTING_STATS  = "mlb.hitting_stats"
TOPIC_PITCHING_STATS = "mlb.pitching_stats"

COMPLETED_STATUSES = {"Final", "Game Over"}
LIVE_STATUSES      = {"In Progress", "Manager Challenge"}


class MLBProducer:
    def __init__(self):
        self.client = MLBApiClient()
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "mlb-pipeline-producer",
        })

    def _publish(self, topic: str, key: str, payload: dict) -> None:
        self.producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload, default=str).encode("utf-8"),
        )

    def publish_teams(self) -> int:
        teams = self.client.get_teams()
        for _, row in teams.iterrows():
            self._publish(TOPIC_TEAMS, str(row["team_id"]), row.to_dict())
        self.producer.flush()
        return len(teams)

    def publish_standings(self) -> int:
        standings = self.client.get_standings()
        for _, row in standings.iterrows():
            self._publish(TOPIC_STANDINGS, str(row["team_id"]), row.to_dict())
        self.producer.flush()
        return len(standings)

    def publish_schedule(self, game_date: date) -> tuple[int, object]:
        schedule = self.client.get_schedule(game_date)
        for _, row in schedule.iterrows():
            self._publish(TOPIC_SCHEDULE, str(row["game_pk"]), row.to_dict())
        self.producer.flush()
        return len(schedule), schedule

    def publish_game_events(self, game_pk: int) -> int:
        feed = self.client.get_game_feed(game_pk)
        plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
        for play in plays:
            result  = play.get("result", {})
            about   = play.get("about", {})
            matchup = play.get("matchup", {})
            event = {
                "game_pk":       game_pk,
                "at_bat_index":  about.get("atBatIndex"),
                "inning":        about.get("inning"),
                "half_inning":   about.get("halfInning"),
                "start_time":    about.get("startTime"),
                "end_time":      about.get("endTime"),
                "is_complete":   about.get("isComplete", False),
                "is_scoring":    about.get("isScoringPlay", False),
                "event_type":    result.get("event"),
                "description":   result.get("description"),
                "rbi":           result.get("rbi"),
                "home_score":    result.get("homeScore"),
                "away_score":    result.get("awayScore"),
                "batter_id":     matchup.get("batter", {}).get("id"),
                "batter_name":   matchup.get("batter", {}).get("fullName"),
                "pitcher_id":    matchup.get("pitcher", {}).get("id"),
                "pitcher_name":  matchup.get("pitcher", {}).get("fullName"),
            }
            self._publish(
                TOPIC_GAME_EVENTS,
                f"{game_pk}_{about.get('atBatIndex', 0)}",
                event,
            )
        self.producer.flush()
        return len(plays)

    def publish_season_schedule(self) -> int:
        schedule = self.client.get_season_schedule()
        for _, row in schedule.iterrows():
            self._publish(TOPIC_SCHEDULE, str(row["game_pk"]), row.to_dict())
        self.producer.flush()
        return len(schedule)

    def publish_hitting_stats(self) -> int:
        stats = self.client.get_hitting_stats()
        for _, row in stats.iterrows():
            self._publish(TOPIC_HITTING_STATS, str(row["player_id"]), row.to_dict())
        self.producer.flush()
        return len(stats)

    def publish_pitching_stats(self) -> int:
        stats = self.client.get_pitching_stats()
        for _, row in stats.iterrows():
            self._publish(TOPIC_PITCHING_STATS, str(row["player_id"]), row.to_dict())
        self.producer.flush()
        return len(stats)

    def _find_playable_games(self, schedule) -> list[tuple]:
        """Return (game_pk, label) pairs for games that have play-by-play data."""
        games = []
        for _, row in schedule.iterrows():
            if row["status"] in COMPLETED_STATUSES | LIVE_STATUSES:
                label = f"{row['away_team']} @ {row['home_team']} [{row['status']}]"
                games.append((int(row["game_pk"]), label))
        return games

    def run(self, game_date: date = None) -> None:
        game_date = game_date or date.today()
        print(f"MLB Producer — {game_date}\n")

        print(f"Teams → {TOPIC_TEAMS}")
        n = self.publish_teams()
        print(f"  {n} messages published\n")

        print(f"Standings → {TOPIC_STANDINGS}")
        n = self.publish_standings()
        print(f"  {n} messages published\n")

        print(f"Full season schedule → {TOPIC_SCHEDULE}")
        n = self.publish_season_schedule()
        print(f"  {n} messages published\n")

        print(f"Hitting stats (official) → {TOPIC_HITTING_STATS}")
        n = self.publish_hitting_stats()
        print(f"  {n} messages published\n")

        print(f"Pitching stats (official) → {TOPIC_PITCHING_STATS}")
        n = self.publish_pitching_stats()
        print(f"  {n} messages published\n")

        print(f"Schedule (today fallback reference) → {TOPIC_SCHEDULE}")
        _, schedule = self.publish_schedule(game_date)
        print(f"  today's schedule refreshed\n")

        print(f"Game events → {TOPIC_GAME_EVENTS}")
        games = self._find_playable_games(schedule)

        if not games:
            print(f"  No completed/live games on {game_date} — scanning recent dates...")
            for days_back in range(1, 8):
                past = game_date - timedelta(days=days_back)
                _, past_schedule = self.publish_schedule(past)
                games = self._find_playable_games(past_schedule)
                if games:
                    print(f"  Found {len(games)} completed game(s) on {past}")
                    break

        total = 0
        for game_pk, label in games:
            count = self.publish_game_events(game_pk)
            print(f"  game {game_pk} ({label}): {count} plays")
            total += count

        print(f"\nDone. Total game events published: {total}")


if __name__ == "__main__":
    MLBProducer().run()
