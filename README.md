# ⚾ MLB DiamondPipeline

A production-style, end-to-end data engineering portfolio project that ingests live MLB game data, streams it through Apache Kafka, transforms it with dbt, orchestrates everything with Airflow, and serves it through an interactive Plotly Dash dashboard — all running locally with a single command.

---

## Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly_Dash-3F4F75?style=flat&logo=plotly&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=flat&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=flat&logo=grafana&logoColor=white)

---

## Architecture

```
MLB Stats API  (free · no auth · live data)
      │
      │ HTTP GET (JSON)
      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Apache Airflow                           │
│              Orchestrates pipeline @ hourly                 │
│                                                             │
│  produce ──▶ consume ──▶ dbt_deps ──▶ dbt_staging          │
│                                            │                │
│                                       dbt_marts             │
│                                     ╱    │     ╲           │
│                              dbt_test  freshness  volume    │
└─────────────────────────────────────────────────────────────┘
      │                         │
      │ producer.py             │ consumer.py
      ▼                         ▼
┌──────────────┐       ┌─────────────────────┐
│ Apache Kafka │       │ Pandas + SQLAlchemy  │
│              │──────▶│                     │
│ mlb.teams    │       │ Batch writes to PG  │
│ mlb.standings│       └──────────┬──────────┘
│ mlb.schedule │                  │
│ mlb.game_    │                  ▼
│   events     │   ┌──────────────────────────────┐
└──────────────┘   │         PostgreSQL            │
                   │                              │
                   │  public.*   (raw / bronze)   │
                   │  analytics.stg_* (silver)    │
                   │  analytics.mart_* (gold)     │
                   └──────────┬───────────────────┘
                              │
                    ┌─────────┴──────────┐
                    │                    │
                    ▼                    ▼
           ┌──────────────┐    ┌──────────────────┐
           │  Plotly Dash │    │ Prometheus +     │
           │  Dashboard   │    │ Grafana          │
           │  :8050       │    │ :9090 / :3000    │
           └──────────────┘    └──────────────────┘
```

---

## Quick Start

**Prerequisites:** Docker Desktop

```bash
git clone https://github.com/<your-username>/mlb-diamondpipeline.git
cd mlb-diamondpipeline
cp .env.example .env
docker compose up -d
```

That's it. Airflow picks up the DAG automatically and runs the full pipeline. Within ~90 seconds all services are live and the dashboard has data.

---

## Services

| Service | URL | Credentials |
|---|---|---|
| **Dash Dashboard** | http://localhost:8050 | — |
| **Airflow UI** | http://localhost:8090 | admin / admin |
| **Kafka UI** | http://localhost:8080 | — |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | — |
| PostgreSQL | localhost:5433 | mlb / mlbpassword |

---

## Dashboard Views

**🏆 Standings** — Live AL/NL division standings with win percentage chart and division/league rank.

**📊 Player Stats** — Batting leaderboard (AVG, OBP, SLG, OPS) aggregated from play-by-play events. Sortable and filterable.

**🎮 Game Results** — Completed game log with scores, winners, run differentials, and wins-by-team chart.

**⚔️ Pitcher vs Batter** — Head-to-head matchup stats. Select any pitcher to see their performance against every batter faced.

---

## How the Pipeline Works

### 1 — Ingest & Stream (Kafka)

`ingestion/producer.py` fetches live data from the [MLB Stats API](https://statsapi.mlb.com/api/) and publishes each record as a JSON message to Kafka topics (`mlb.teams`, `mlb.standings`, `mlb.schedule`, `mlb.game_events`).

`ingestion/consumer.py` subscribes to all topics, batches messages in groups of 50, and writes them to PostgreSQL raw tables using Pandas `DataFrame.to_sql()`.

### 2 — Transform (dbt · ELT pattern)

Raw data lands in the `public` schema untouched. dbt then runs two layers of SQL transformations:

| Layer | Schema | Models | Purpose |
|---|---|---|---|
| **Staging (Silver)** | `analytics` | `stg_*` | Type casting, renaming, deduplication |
| **Marts (Gold)** | `analytics` | `mart_*` | Business aggregations, window functions |

`stg_game_events` uses an **incremental model** — only new plays are processed on each run, not the entire table.

### 3 — Validate (dbt tests + volume checks)

- **44 schema tests** (not_null, unique) on every key column across sources, staging, and marts
- **Source freshness** check on `raw_game_events.end_time` — alerts if data is older than 24 hours
- **Volume check** Airflow task — asserts minimum row counts on all critical tables
- **Elementary** data observability package installed in dbt for anomaly detection

### 4 — Orchestrate (Airflow)

The `mlb_pipeline` DAG runs hourly with:
- Automatic retry on failure (1 retry, 3-minute delay)
- Failure callback for alerting
- Parallel execution of final validation tasks
- Full visibility in the Airflow UI

### 5 — Observe (Prometheus + Grafana)

Prometheus scrapes metrics from dedicated exporters:
- **postgres-exporter** → table row counts, active connections, DB size
- **kafka-exporter** → consumer group lag, topic throughput

Pre-built Grafana dashboard auto-provisions on startup.

---

## Project Structure

```
mlb-diamondpipeline/
├── ingestion/
│   ├── mlb_api.py        # MLB Stats API client
│   ├── db.py             # SQLAlchemy engine + load_dataframe()
│   ├── producer.py       # Kafka producer
│   └── consumer.py       # Kafka consumer
├── dbt/
│   ├── models/
│   │   ├── staging/      # Silver layer: typed, cleaned views
│   │   └── marts/        # Gold layer: business aggregations
│   ├── profiles.yml      # DB connection (env-var driven)
│   └── packages.yml      # Elementary + dbt_utils
├── airflow/
│   ├── Dockerfile        # Extends apache/airflow:2.10.2
│   ├── requirements.txt  # Pipeline deps for Airflow's Python env
│   └── dags/
│       └── mlb_pipeline.py  # Main DAG definition
├── dashboard/
│   ├── Dockerfile        # python:3.11-slim + requirements.txt
│   ├── data.py           # PostgreSQL query functions
│   └── app.py            # Plotly Dash app (4 tabs, 8 charts)
├── observability/
│   ├── prometheus/       # Scrape config
│   └── grafana/          # Dashboard JSON + provisioning
├── postgres/
│   └── init.sql          # Creates airflow database on first start
├── docker-compose.yml    # All 11 services wired together
├── requirements.txt      # Python deps (for local dev)
├── Makefile              # Convenience commands
└── ARCHITECTURE.md       # Deep-dive: how everything works
```

---

## Data Model

```
raw_game_events (819 plays/day from ~11 games)
       │
       ▼ dbt incremental (unique_key: game_pk + at_bat_index)
stg_game_events
       │
       ▼ dbt aggregation
mart_player_stats          → batting_avg, OBP, SLG, OPS per player
mart_pitcher_matchups      → head-to-head stats per pitcher/batter pair

raw_standings (30 teams)
       │
       ▼ dbt view → stg_standings
       ▼ dbt window functions
mart_standings             → wins, losses, win_pct, division_rank, league_rank

raw_schedule + raw_game_events
       │
       ▼ dbt join + CASE
mart_game_results          → winning_team, losing_team, run_differential
```

---

## Makefile Reference

```bash
make up              # Start all Docker services
make down            # Stop all services
make clean           # Stop + wipe all volumes (full reset)
make ps              # Show service health
make logs            # Follow logs

# Local dev only (requires: cp .env.example .env && python3 -m venv .venv && pip install -r requirements.txt)
make produce         # Run Kafka producer locally
make consume         # Run Kafka consumer locally
make dbt-run         # Run all dbt models locally
make dbt-test        # Run dbt schema tests locally
```

---

## Key Concepts Demonstrated

| Concept | Implementation |
|---|---|
| ELT pattern | Raw data loaded first, transformed in-DB by dbt |
| Event streaming | Kafka producer/consumer with 4 topics |
| Incremental loading | `stg_game_events` only processes new plays |
| Data quality | 44 dbt tests + source freshness + volume checks |
| Orchestration | Airflow DAG with retries, alerting, parallel tasks |
| Data observability | Elementary + Prometheus/Grafana |
| Medallion architecture | Bronze (raw) → Silver (staging) → Gold (marts) |
| Idempotency | `DROP CASCADE` + `distinct on` + dbt `unique_key` |

---

## Resume Bullet

> *"Built an end-to-end MLB data pipeline processing live game events through Kafka, dbt, and PostgreSQL — automating hourly ingestion and ELT transformation via Airflow DAGs, validating data quality with 44 dbt schema tests and source freshness checks, and serving standings, player analytics, and pitcher-batter matchups through a deployed Plotly Dash dashboard. Full stack runs on docker compose up."*

---

## Deep Dive

For a full explanation of how every tool works, Django-to-DE analogies, code walkthroughs, and interview talking points → **[ARCHITECTURE.md](ARCHITECTURE.md)**

---

## Data Source

Live data from the [MLB Stats API](https://statsapi.mlb.com/api/) — free, public, no authentication required.
