# ⚾ MLB DiamondPipeline

A production-style, end-to-end data engineering portfolio project that ingests full-season MLB data, streams it through Apache Kafka, transforms it with dbt, orchestrates everything with Airflow, and serves it through an interactive Plotly Dash dashboard — all running locally with a single command.

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
MLB Stats API  (free · no auth · full season data)
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
┌──────────────────┐   ┌─────────────────────┐
│  Apache Kafka    │   │ Pandas + SQLAlchemy  │
│                  │──▶│                     │
│ mlb.teams        │   │ Batch writes to PG  │
│ mlb.standings    │   └──────────┬──────────┘
│ mlb.schedule     │              │
│ mlb.game_events  │              ▼
│ mlb.hitting_stats│  ┌──────────────────────────────┐
│ mlb.pitching_    │  │         PostgreSQL            │
│   stats          │  │                              │
└──────────────────┘  │  public.*   (raw / bronze)   │
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

**🏆 Standings** — Live AL/NL division standings with win percentage chart and division/league rank for all 30 teams.

**🏃 Batting** — Full season batting leaderboard (AVG, OBP, SLG, OPS, HR, RBI) from official MLB stats for 400+ qualified hitters. Sortable and filterable.

**⚾ Pitching** — Full season pitching leaderboard (ERA, WHIP, K, K/9) from official MLB stats. Starters and relievers separated, with an ERA leaderboard chart.

**🎮 Game Results** — Complete season game log with 650+ results, scores, winners, run differentials, and a wins-by-team chart for the full season.

**🔮 HR Predictions** — Daily home run probability leaderboard. For each batter facing a probable pitcher, the model computes a regressed HR rate × pitcher HR-allowed rate ÷ league average × park factor. Probable pitchers fetched live from the MLB Stats API. Batters matched to games by team ID (not name) for reliable cross-API consistency.

---

## How the Pipeline Works

### 1 — Ingest & Stream (Kafka)

`ingestion/producer.py` fetches data from the [MLB Stats API](https://statsapi.mlb.com/api/) and publishes each record as a JSON message across 6 Kafka topics:

| Topic | Content | API Endpoint |
|---|---|---|
| `mlb.teams` | 30 team records | `/api/v1/teams` |
| `mlb.standings` | Current AL/NL standings | `/api/v1/standings` |
| `mlb.schedule` | Full season schedule (650+ games) | `/api/v1/schedule` with date range |
| `mlb.game_events` | Play-by-play for recent games | `/api/v1.1/game/{pk}/feed/live` |
| `mlb.hitting_stats` | Official season stats for 500+ hitters | `/api/v1/stats?group=hitting` |
| `mlb.pitching_stats` | Official season stats for 600+ pitchers | `/api/v1/stats?group=pitching` |

`ingestion/consumer.py` subscribes to all topics, batches messages, and writes them to PostgreSQL raw tables.

### 2 — Transform (dbt · ELT pattern)

Raw data lands in the `public` schema untouched. dbt runs two layers of SQL transformations:

| Layer | Schema | Models | Purpose |
|---|---|---|---|
| **Staging (Silver)** | `analytics` | `stg_*` | Type casting, renaming, deduplication |
| **Marts (Gold)** | `analytics` | `mart_*` | Business aggregations, window functions |

`stg_game_events` uses an **incremental model** — only new plays are processed on each run.
`stg_hitting_stats` and `stg_pitching_stats` deduplicate by `player_id` to handle mid-season trades.

### 3 — Validate (dbt tests + volume checks)

- **Schema tests** (not_null, unique) on every key column across sources, staging, and marts
- **Source freshness** check on `raw_game_events.end_time` — alerts if data is older than 24 hours
- **Volume check** Airflow task — asserts minimum row counts on all critical tables
- **Elementary** data observability package integrated with dbt

### 4 — Orchestrate (Airflow)

The `mlb_pipeline` DAG runs hourly with:
- Automatic retry on failure (1 retry, 3-minute delay)
- Failure callback for alerting
- Parallel execution of final validation tasks
- Full visibility in the Airflow UI at http://localhost:8090

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
│   ├── mlb_api.py        # MLB Stats API client (6 endpoints)
│   ├── db.py             # SQLAlchemy engine + load_dataframe()
│   ├── producer.py       # Kafka producer (6 topics)
│   └── consumer.py       # Kafka consumer → PostgreSQL
├── dbt/
│   ├── models/
│   │   ├── staging/      # Silver layer: 6 typed, cleaned models
│   │   └── marts/        # Gold layer: 4 business aggregations
│   ├── profiles.yml      # DB connection (env-var driven)
│   └── packages.yml      # Elementary + dbt_utils
├── airflow/
│   ├── Dockerfile        # Extends apache/airflow:2.10.2
│   ├── requirements.txt  # Pipeline deps for Airflow's Python env
│   └── dags/
│       └── mlb_pipeline.py  # Main DAG definition (8 tasks)
├── dashboard/
│   ├── Dockerfile        # python:3.11-slim + requirements.txt
│   ├── data.py           # PostgreSQL query functions
│   ├── predictions.py    # HR prediction engine (log5 model + park factors)
│   └── app.py            # Plotly Dash app (5 tabs)
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
MLB Stats API (/api/v1/stats · official season stats)
       │
       ├── raw_hitting_stats (518 players)
       │         │
       │         ▼ dbt deduplicate by player_id
       │   stg_hitting_stats
       │         │
       │         ▼ dbt filter ≥10 PA
       │   mart_player_stats    → AVG, OBP, SLG, OPS, HR, RBI (400+ hitters)
       │
       └── raw_pitching_stats (603 pitchers)
                 │
                 ▼ dbt deduplicate by player_id
           stg_pitching_stats
                 │
                 ▼ dbt classify starter/reliever
           mart_pitching_leaders → ERA, WHIP, K/9, saves (starters + relievers)

MLB Stats API (/api/v1/schedule · full season)
       │
       ▼ append across days, deduplicate by game_pk in dbt
raw_schedule (650+ games)
       │
       ▼ dbt CASE for winner/loser/run_differential
mart_game_results → 650+ completed game results for the full season

raw_standings (30 teams)
       │
       ▼ dbt view → stg_standings
       ▼ dbt window functions (rank() OVER partition)
mart_standings → division_rank, league_rank for all 30 teams
```

---

## Makefile Reference

```bash
make up              # Start all Docker services
make down            # Stop all services
make clean           # Stop + wipe all volumes (full reset)
make ps              # Show service health
make logs            # Follow logs

# Local dev only (requires: cp .env.example .env && make setup)
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
| Event streaming | Kafka producer/consumer with 6 topics |
| Full season analytics | Official MLB stats API — not sampled or approximated |
| Incremental loading | `stg_game_events` only processes new plays each run |
| Snapshot deduplication | `stg_hitting_stats` / `stg_pitching_stats` deduplicate on `player_id` |
| Schedule accumulation | `raw_schedule` appends across days, dbt keeps best status per game |
| Data quality | dbt schema tests + source freshness + Airflow volume checks |
| Orchestration | Airflow DAG with retries, alerting, parallel tasks |
| Data observability | Elementary + Prometheus/Grafana |
| Medallion architecture | Bronze (raw) → Silver (staging) → Gold (marts) |
| Idempotency | `DROP CASCADE` + `distinct on` + dbt `unique_key` |


## Deep Dive

For a full explanation of how every tool works, Django-to-DE analogies, code walkthroughs, → **[ARCHITECTURE.md](ARCHITECTURE.md)**

---

## Data Source

Live data from the [MLB Stats API](https://statsapi.mlb.com/api/) — free, public, no authentication required.
