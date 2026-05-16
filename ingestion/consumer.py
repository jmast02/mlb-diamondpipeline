import json
import os
import time
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from confluent_kafka import Consumer

from ingestion.db import load_dataframe

load_dotenv(find_dotenv())

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_TEAMS          = "mlb.teams"
TOPIC_STANDINGS      = "mlb.standings"
TOPIC_SCHEDULE       = "mlb.schedule"
TOPIC_GAME_EVENTS    = "mlb.game_events"
TOPIC_HITTING_STATS  = "mlb.hitting_stats"
TOPIC_PITCHING_STATS = "mlb.pitching_stats"

BATCH_SIZE   = 50
IDLE_TIMEOUT = 10  # seconds of silence before exiting

# Teams/standings/stats are full snapshots — always replace.
# Schedule accumulates across days — append + deduplicate in dbt.
# Game events are immutable facts — always append.
IF_EXISTS = {
    TOPIC_TEAMS:          "replace",
    TOPIC_STANDINGS:      "replace",
    TOPIC_SCHEDULE:       "append",
    TOPIC_GAME_EVENTS:    "append",
    TOPIC_HITTING_STATS:  "replace",
    TOPIC_PITCHING_STATS: "replace",
}

TABLE_MAP = {
    TOPIC_TEAMS:          "raw_teams",
    TOPIC_STANDINGS:      "raw_standings",
    TOPIC_SCHEDULE:       "raw_schedule",
    TOPIC_GAME_EVENTS:    "raw_game_events",
    TOPIC_HITTING_STATS:  "raw_hitting_stats",
    TOPIC_PITCHING_STATS: "raw_pitching_stats",
}


class MLBConsumer:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "mlb-pipeline-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        self.buffers: dict[str, list[dict]] = {t: [] for t in TABLE_MAP}
        self.counts:  dict[str, int]        = {t: 0 for t in TABLE_MAP}

    def _flush(self, topic: str) -> None:
        buf = self.buffers[topic]
        if not buf:
            return
        df = pd.DataFrame(buf)
        load_dataframe(df, TABLE_MAP[topic], if_exists=IF_EXISTS[topic])
        print(f"  [flush] {TABLE_MAP[topic]}: {len(buf)} rows written")
        buf.clear()

    def _flush_all(self) -> None:
        for topic in self.buffers:
            self._flush(topic)

    def run(self, idle_timeout: int = IDLE_TIMEOUT) -> None:
        self.consumer.subscribe(list(TABLE_MAP.keys()))
        print(f"Consumer subscribed — idle timeout: {idle_timeout}s\n")

        last_msg_at = time.time()

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if time.time() - last_msg_at > idle_timeout:
                        print(f"No messages for {idle_timeout}s — flushing and exiting.")
                        break
                    continue

                if msg.error():
                    print(f"[error] {msg.error()}")
                    continue

                topic = msg.topic()
                payload = json.loads(msg.value().decode("utf-8"))
                self.buffers[topic].append(payload)
                self.counts[topic] += 1
                last_msg_at = time.time()

                # Only mid-stream flush for append topics (events, schedule).
                # Snapshot topics (replace) accumulate all messages and write once at the end
                # — flushing mid-stream would overwrite earlier batches with only 50 rows.
                if len(self.buffers[topic]) >= BATCH_SIZE and IF_EXISTS[topic] == "append":
                    self._flush(topic)

        finally:
            self._flush_all()
            self.consumer.close()

        print("\nConsumer summary:")
        for topic, count in self.counts.items():
            print(f"  {TABLE_MAP[topic]}: {count} messages consumed")


if __name__ == "__main__":
    MLBConsumer().run()
