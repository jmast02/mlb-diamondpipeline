# MLB DiamondPipeline — Architecture & How It All Works

> **Who this is for:** A Python/Django developer learning data engineering.
> Every concept is explained with a Django analogy first, then the DE reality.

---

## Table of Contents

1. [The Big Picture](#1-the-big-picture)
2. [Full Architecture Diagram](#2-full-architecture-diagram)
3. [The Data Flow — Step by Step](#3-the-data-flow--step-by-step)
4. [The Medallion Architecture (Bronze → Silver → Gold)](#4-the-medallion-architecture)
5. [Tool Deep Dives](#5-tool-deep-dives)
   - [MLB Stats API — The Data Source](#51-mlb-stats-api)
   - [Pandas — Your In-Memory QuerySet](#52-pandas)
   - [Apache Kafka — The Message Broker](#53-apache-kafka)
   - [PostgreSQL — The Data Warehouse](#54-postgresql)
   - [dbt — The Transformation Layer](#55-dbt)
   - [Apache Airflow — The Orchestrator](#56-apache-airflow)
   - [Plotly Dash — The Dashboard](#57-plotly-dash)
   - [Prometheus + Grafana — Observability](#58-prometheus--grafana)
6. [File-by-File Breakdown](#6-file-by-file-breakdown)
7. [ETL vs ELT — What We're Actually Doing](#7-etl-vs-elt)
8. [Key Data Engineering Concepts](#8-key-data-engineering-concepts)
9. [What Happens on docker compose up](#9-what-happens-on-docker-compose-up)

---

## 1. The Big Picture

In web development you build apps that **serve users requests in real time**.
In data engineering you build pipelines that **move, clean, and transform data reliably over time**.

The end goal is the same — deliver value from data. But the workflow is different:

| Web Dev (Django)          | Data Engineering (This Project)        |
|---------------------------|----------------------------------------|
| User hits a URL           | A schedule triggers a pipeline run     |
| View pulls from DB        | Pipeline pulls from an external API    |
| Serializer cleans data    | dbt model cleans and types data        |
| ORM query aggregates data | dbt mart model aggregates data         |
| Template renders HTML     | Plotly Dash renders interactive charts |
| Celery handles async jobs | Airflow orchestrates the pipeline      |
| Redis queues Celery tasks | Kafka queues streaming events          |

This project mirrors what a **production data pipeline looks like at a real company** — just with MLB data instead of business data.

---

## 2. Full Architecture Diagram

```
                        ┌─────────────────────────────┐
                        │      MLB Stats API          │
                        │   statsapi.mlb.com/api/v1   │
                        │   (free, no auth required)  │
                        └─────────────┬───────────────┘
                                      │ HTTP GET (JSON)
                                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Apache Airflow                              │
│                    Runs on schedule: @hourly                        │
│                                                                     │
│  mlb_pipeline DAG:                                                  │
│                                                                     │
│  [produce_to_kafka]──▶[consume_from_kafka]──▶[dbt_deps]            │
│                                                     │               │
│                                              [dbt_staging]          │
│                                                     │               │
│                                              [dbt_marts]            │
│                                            ╱    │      ╲           │
│                               [dbt_test] [freshness] [volume_check]│
└─────────────────────────────────────────────────────────────────────┘
         │                           │
         │ producer.py               │ consumer.py
         ▼                           ▼
┌──────────────────────┐    ┌─────────────────────┐
│    Apache Kafka      │    │   Apache Kafka       │
│                      │    │                      │
│  Topics:             │───▶│  Reads messages,     │
│  mlb.teams           │    │  batches them,       │
│  mlb.standings       │    │  writes to Postgres  │
│  mlb.schedule        │    └──────────┬──────────┘
│  mlb.game_events     │               │
│  mlb.hitting_stats   │               │ Pandas DataFrame
│  mlb.pitching_stats  │               ▼ .to_sql()
└──────────────────────┘
┌─────────────────────────────────────────────────────────────────────┐
│                         PostgreSQL                                  │
│                                                                     │
│  ┌── public schema (RAW / BRONZE layer) ────────────────────────┐  │
│  │                                                               │  │
│  │  raw_teams          raw_standings     raw_schedule            │  │
│  │  raw_game_events    raw_hitting_stats raw_pitching_stats      │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│                              │  dbt run                             │
│                              ▼                                      │
│  ┌── analytics schema (STAGING / SILVER layer) ─────────────────┐  │
│  │                                                               │  │
│  │  stg_teams (view)         stg_standings (view)               │  │
│  │  stg_schedule (view)      stg_game_events (table, incr.)     │  │
│  │  stg_hitting_stats (view) stg_pitching_stats (view)          │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│                              │  dbt run                             │
│                              ▼                                      │
│  ┌── analytics schema (MARTS / GOLD layer) ─────────────────────┐  │
│  │                                                               │  │
│  │  mart_standings        mart_game_results (650+ games)         │  │
│  │  mart_player_stats     mart_pitching_leaders                  │  │
│  │  (400+ hitters)        (600+ pitchers)                        │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
         │                                    │
         │ Prometheus exporters               │ SQL queries
         ▼                                    ▼
┌──────────────────┐               ┌──────────────────────┐
│  Prometheus      │               │    Plotly Dash        │
│  + Grafana       │               │    localhost:8050     │
│  localhost:3000  │               │                       │
│                  │               │  🏆 Standings          │
│  Kafka lag       │               │  🏃 Batting            │
│  PG connections  │               │  ⚾ Pitching           │
│  Table row counts│               │  🎮 Game Results       │
└──────────────────┘               └──────────────────────┘
```

---

## 3. The Data Flow — Step by Step

Here's exactly what happens every hour when Airflow runs the pipeline:

### Step 1 — `produce_to_kafka`
```
ingestion/producer.py
```

The producer is like a **Django management command** that fetches data from an API and queues it. Instead of sending emails to a Celery queue, it sends MLB game data to Kafka topics.

```python
# It calls the MLB API (like requests.get() in Django)
client = MLBApiClient()
teams    = client.get_teams()           # GET /api/v1/teams          → 30 records
standings = client.get_standings()      # GET /api/v1/standings      → 30 records
schedule  = client.get_season_schedule() # GET /api/v1/schedule      → 650+ games
hitting   = client.get_hitting_stats()  # GET /api/v1/stats?group=hitting → 518 players
pitching  = client.get_pitching_stats() # GET /api/v1/stats?group=pitching → 603 players

# Then publishes each row as a JSON message to Kafka
for _, row in hitting.iterrows():
    producer.produce(
        topic="mlb.hitting_stats",
        key=str(row["player_id"]),
        value=json.dumps(row.to_dict()).encode("utf-8"),
    )
```

**Why Kafka?** In a real streaming pipeline, game events happen live and continuously. Kafka decouples the producer (fetching data) from the consumer (writing to the database). If the DB is slow, Kafka buffers the events. If the producer crashes, Kafka holds the messages until the consumer catches up.

**Key design decision:** Snapshot topics (teams, standings, hitting stats, pitching stats) accumulate all messages in memory before writing — a single `replace` at the end. Append topics (schedule, game events) flush every 50 messages to keep memory low. This prevents the "last batch wins" problem where mid-stream flushes would overwrite earlier data.

---

### Step 2 — `consume_from_kafka`
```
ingestion/consumer.py
```

The consumer reads messages off the Kafka topics and writes them to PostgreSQL in batches.

```python
# Subscribe to all MLB topics
consumer.subscribe([
    "mlb.teams", "mlb.standings", "mlb.schedule",
    "mlb.game_events", "mlb.hitting_stats", "mlb.pitching_stats",
])

# Poll for messages (like Django's request loop, but for messages)
while True:
    msg = consumer.poll(timeout=1.0)
    payload = json.loads(msg.value())
    buffer[msg.topic()].append(payload)

    # Every 50 messages, flush to Postgres
    if len(buffer[topic]) >= 50:
        df = pd.DataFrame(buffer[topic])
        df.to_sql("raw_standings", engine, if_exists="replace")
```

This creates the **raw tables** — unmodified data exactly as the API returned it.

---

### Step 3 — `dbt_deps`
Downloads dbt packages (Elementary, dbt_utils) from the dbt Hub. Like `pip install -r requirements.txt` but for dbt.

---

### Step 4 — `dbt_staging`
```
dbt/models/staging/stg_*.sql
```

dbt runs SQL models to **clean and type the raw data**. Think of staging models like **Django serializers** — they take raw input and make it properly typed and validated.

```sql
-- stg_standings.sql
-- Before: raw_standings has team_id as BIGINT, win_pct as DOUBLE PRECISION
-- After:  proper types, renamed columns, clean data

select
    team_id::integer     as team_id,
    team_name            as team_name,
    wins::integer        as wins,
    losses::integer      as losses,
    win_pct::numeric(4,3) as win_pct,   -- 0.667 not 0.6666666666
    season::integer      as season
from {{ source('raw', 'raw_standings') }}
```

The `{{ source('raw', 'raw_standings') }}` is dbt's Jinja templating — it resolves to the actual table name and lets dbt track data lineage (which model depends on which table).

---

### Step 5 — `dbt_marts`
```
dbt/models/marts/mart_*.sql
```

Mart models are the **final business-ready tables** — like a **Django view that does a complex queryset annotation**. These are what the dashboard actually reads.

```sql
-- mart_standings.sql
-- Adds division_rank and league_rank using SQL window functions

with standings as (
    select * from {{ ref('stg_standings') }}  -- dbt ref() = FK relationship
),
ranked as (
    select
        *,
        -- Window function: rank each team within their division by wins
        rank() over (
            partition by division_name
            order by wins desc, losses asc
        ) as division_rank
    from standings
)
select * from ranked
```

**Window functions** (`rank() over (partition by ...)`) are the SQL equivalent of Django's `annotate()` with a subquery — they compute a value for each row relative to a group of rows.

---

### Step 6 — `dbt_test`, `source_freshness`, `volume_check`

These run in parallel after marts complete:

- **dbt_test**: Runs assertions like `not_null`, `unique` on every key column. Like Django's `clean()` method but for warehouse data.
- **source_freshness**: Checks that `raw_game_events.end_time` is within 24 hours. Catches cases where the API is down or the pipeline didn't run.
- **volume_check**: Custom Python task that asserts minimum row counts per table. If `raw_standings` suddenly has 5 rows instead of 30, something broke upstream.

---

## 4. The Medallion Architecture

This is the standard pattern for organizing data in a warehouse. Think of it like Django's URL → View → Serializer → Model flow, but for data layers.

```
┌─────────────────────────────────────────────────────────────┐
│  BRONZE (raw)          public schema                        │
│                                                             │
│  Exact copy of source data. Never transformed.              │
│  "What did the API actually return?"                        │
│                                                             │
│  raw_teams, raw_standings, raw_schedule, raw_game_events    │
└───────────────────────────┬─────────────────────────────────┘
                            │  dbt staging models
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER (staging)      analytics schema                     │
│                                                             │
│  Cleaned, typed, deduplicated. Source of truth.             │
│  "What does the data mean, with proper types?"              │
│                                                             │
│  stg_teams, stg_standings, stg_schedule, stg_game_events    │
└───────────────────────────┬─────────────────────────────────┘
                            │  dbt mart models
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD (marts)          analytics schema                     │
│                                                             │
│  Business-ready aggregations. What dashboards read.         │
│  "What is the answer to the business question?"             │
│                                                             │
│  mart_standings, mart_player_stats,                         │
│  mart_game_results, mart_pitcher_matchups                   │
└─────────────────────────────────────────────────────────────┘
```

**Why three layers?**

If the MLB API changes a column name, you only fix it in the staging model — the mart models and dashboard don't change. Each layer has one job and is independently testable. This is the same reason you don't write SQL in Django templates — separation of concerns.

---

## 5. Tool Deep Dives

### 5.1 MLB Stats API

**What it is:** A free, public REST API from MLB. No authentication required.

**Key endpoints used:**
```
GET https://statsapi.mlb.com/api/v1/teams?sportId=1&season=2026
GET https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2026
GET https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=2026-05-15
GET https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live   ← note: v1.1 for game feed
```

**The client** (`ingestion/mlb_api.py`):
```python
class MLBApiClient:
    def __init__(self, season: int = None):
        self.season = season or date.today().year
        self.session = requests.Session()  # Reuses TCP connections (like Django's DB connection pool)

    def _get(self, endpoint: str, params: dict = None) -> dict:
        url = f"https://statsapi.mlb.com/api/v1{endpoint}"
        response = self.session.get(url, params=params, timeout=15)
        response.raise_for_status()  # Raise exception on 4xx/5xx (like DRF's raise_exception)
        return response.json()

    def get_standings(self) -> pd.DataFrame:
        data = self._get("/standings", params={"leagueId": "103,104", "season": self.season})
        # Flatten the nested JSON into a flat list of dicts
        rows = []
        for record in data.get("records", []):
            division_id = record.get("division", {}).get("id")
            for tr in record.get("teamRecords", []):
                lr = tr.get("leagueRecord", {})
                rows.append({
                    "team_id": tr["team"]["id"],
                    "wins":    int(lr.get("wins", 0)),
                    ...
                })
        return pd.DataFrame(rows)  # Returns a DataFrame, not a QuerySet
```

**Django parallel:** This is the same as a `requests.get()` call in a Django view — but instead of returning an HTTP response to a user, we return a DataFrame to be stored.

---

### 5.2 Pandas

**Django analogy:** Pandas DataFrames are like Django QuerySets — but loaded entirely into memory, with NumPy-powered math.

| Django QuerySet                    | Pandas DataFrame                     |
|------------------------------------|--------------------------------------|
| `Player.objects.filter(team="NYY")`| `df[df["team"] == "NYY"]`            |
| `Player.objects.values("name")`    | `df[["name"]]`                       |
| `.annotate(total=Sum("rbi"))`      | `df.groupby("player").agg({"rbi": "sum"})` |
| `.order_by("-batting_avg")`        | `df.sort_values("batting_avg", ascending=False)` |
| `qs.count()`                       | `len(df)`                            |
| Lazy (hits DB on eval)             | Eager (data already in memory)       |

**Key uses in our pipeline:**

```python
# Reading API response → DataFrame
df = pd.DataFrame([
    {"team_id": 147, "team_name": "Yankees", "wins": 27},
    {"team_id": 139, "team_name": "Rays",    "wins": 28},
])

# Filtering
al_teams = df[df["league_name"] == "American League"]

# Computing derived columns
df["win_pct"] = df["wins"] / (df["wins"] + df["losses"])

# Writing to PostgreSQL (one line — handles CREATE TABLE + INSERT)
df.to_sql("raw_standings", engine, if_exists="replace", index=False, schema="public")
```

**Why Pandas instead of raw SQL inserts?**
`df.to_sql()` infers column types from the DataFrame dtypes, creates the table if needed, and handles batched inserts — all in one line. In Django you'd write a migration + `bulk_create()`.

---

### 5.3 Apache Kafka

**Django analogy:** Kafka is like **Celery + Redis**, but designed for massive scale and permanent message storage.

```
Django:  HTTP request → Django View → Celery task → Redis queue → Worker → Result
MLB:     API fetch   → Producer   → Kafka topic → Consumer  → PostgreSQL
```

**Core concepts:**

```
┌─────────────────────────────────────────────────────┐
│                    Kafka Cluster                     │
│                                                     │
│  Topic: mlb.standings                               │
│  ┌──────────────────────────────────────────┐       │
│  │ Partition 0                              │       │
│  │ [msg 0][msg 1][msg 2][msg 3][msg 4] ...  │       │
│  │  team_id=147  team_id=139  team_id=121   │       │
│  └──────────────────────────────────────────┘       │
│                                                     │
│  Topic: mlb.game_events                             │
│  ┌──────────────────────────────────────────┐       │
│  │ Partition 0                              │       │
│  │ [play 0][play 1][play 2][play 3] ...     │       │
│  │  strikeout  single  home_run  groundout  │       │
│  └──────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────┘
         ▲ produce                  ▼ consume
   producer.py                  consumer.py
```

**Key differences from Celery/Redis:**
- Messages are **stored permanently** (we set 7 days retention). In Redis, processed tasks are gone.
- Multiple consumers can read the same messages independently.
- Messages are ordered within a partition.
- Consumers track their position (called **offset**) — like a bookmark.

**Our producer** (`ingestion/producer.py`):
```python
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092"})

# Publish one message per team
producer.produce(
    topic="mlb.teams",
    key="147".encode(),            # Message key — used for partitioning
    value=json.dumps({             # Message value — the actual data
        "team_id": 147,
        "team_name": "Yankees",
        "wins": 27,
    }).encode("utf-8"),
)
producer.flush()  # Wait for all messages to be acknowledged
```

**Our consumer** (`ingestion/consumer.py`):
```python
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "mlb-pipeline-consumer",  # Consumer group — tracks offset per group
    "auto.offset.reset": "earliest",       # Start from beginning if no offset saved
})

consumer.subscribe(["mlb.teams", "mlb.standings", "mlb.schedule", "mlb.game_events"])

while True:
    msg = consumer.poll(timeout=1.0)  # Wait up to 1s for a message
    if msg is None:
        break  # No messages — we're caught up

    payload = json.loads(msg.value().decode("utf-8"))
    buffer[msg.topic()].append(payload)

    if len(buffer[msg.topic()]) >= 50:
        flush_to_postgres(buffer[msg.topic()])  # Write batch of 50 rows
```

**Why Kafka for this project?**
We're simulating an event-driven architecture where game play-by-play events stream in real time. Each at-bat is an event. In production, the producer would be a live-feed listener rather than a scheduled API poller.

---

### 5.4 PostgreSQL

**What it is:** A relational database used as our **data warehouse**. Same Postgres you'd use with Django — but organized differently.

**How we connect** (`ingestion/db.py`):
```python
from sqlalchemy import create_engine

# SQLAlchemy engine — like Django's DATABASES setting but explicit
engine = create_engine(
    "postgresql+psycopg2://mlb:mlbpassword@localhost:5433/mlb_pipeline"
)

# Write a DataFrame to a table (creates table if doesn't exist)
df.to_sql("raw_standings", engine, if_exists="replace", schema="public")

# Read a table into a DataFrame
df = pd.read_sql("SELECT * FROM analytics.mart_standings", engine)
```

**Schema layout:**
```
mlb_pipeline database
│
├── public schema          ← Raw data (Bronze layer)
│   ├── raw_teams
│   ├── raw_standings
│   ├── raw_schedule
│   └── raw_game_events
│
├── analytics schema       ← dbt outputs (Silver + Gold layers)
│   ├── stg_teams          ← view (Silver)
│   ├── stg_standings      ← view (Silver)
│   ├── stg_schedule       ← view (Silver)
│   ├── stg_game_events    ← table, incremental (Silver)
│   ├── mart_standings     ← table (Gold)
│   ├── mart_game_results  ← table (Gold)
│   ├── mart_player_stats  ← table (Gold)
│   └── mart_pitcher_matchups ← table (Gold)
│
└── analytics_elementary schema ← Elementary monitoring tables
```

**Why not use Django's ORM?**
Django's ORM is designed for application logic (CRUD on normalized models). Data warehouses are designed for analytical queries (aggregations across millions of rows). The two have very different access patterns. In DE, you work with SQL directly — it's more expressive for analytics.

---

### 5.5 dbt

**Django analogy:** dbt is like **Django migrations + model managers + serializers**, all rolled into SQL files.

dbt takes SQL `SELECT` statements and turns them into tables or views in your database. You write the transformation logic, dbt handles the execution order, creates/replaces tables, and runs tests.

**Project structure:**
```
dbt/
├── dbt_project.yml        ← Like Django's settings.py for dbt
├── profiles.yml           ← Database connection (like DATABASES in settings.py)
├── packages.yml           ← Like requirements.txt but for dbt packages
└── models/
    ├── staging/
    │   ├── _sources.yml   ← Declares raw tables as "sources" (like Django model declarations)
    │   ├── _staging.yml   ← Schema tests for staging models
    │   ├── stg_teams.sql
    │   ├── stg_standings.sql
    │   ├── stg_schedule.sql
    │   └── stg_game_events.sql   ← INCREMENTAL model
    └── marts/
        ├── _marts.yml     ← Schema tests for mart models
        ├── mart_standings.sql
        ├── mart_game_results.sql
        ├── mart_player_stats.sql
        └── mart_pitcher_matchups.sql
```

**How dbt refs work:**
```sql
-- mart_standings.sql
-- {{ ref('stg_standings') }} is like a Django ForeignKey
-- dbt resolves this to the actual table name AND tracks the dependency

with standings as (
    select * from {{ ref('stg_standings') }}   -- depends on stg_standings
)
select
    *,
    rank() over (partition by division_name order by wins desc) as division_rank
from standings
```

dbt builds a **DAG of dependencies** from these `ref()` calls, then runs models in the correct order. If `stg_standings` hasn't run yet, `mart_standings` waits.

**The incremental model** (`stg_game_events.sql`):
```sql
-- This model only processes NEW events, not the whole table every time
{{
    config(
        materialized='incremental',
        unique_key='event_id',     -- Like Django's unique_together
    )
}}

with source as (
    select
        game_pk::text || '_' || at_bat_index::text as event_id,
        game_pk, at_bat_index, inning, ...
    from {{ source('raw', 'raw_game_events') }}

    {% if is_incremental() %}
    -- On subsequent runs, only process events not already in the table
    -- Like Django's .exclude(id__in=already_processed_ids)
    where (game_pk::text || '_' || at_bat_index::text) not in (
        select event_id from {{ this }}  -- {{ this }} = the existing table
    )
    {% endif %}
)

-- Deduplicate: if same event appears twice, keep one
select distinct on (event_id) *
from source
order by event_id
```

**dbt tests** (defined in `_staging.yml`):
```yaml
models:
  - name: stg_standings
    columns:
      - name: team_id
        tests:
          - not_null      # Like Django's null=False
          - unique        # Like Django's unique=True
      - name: wins
        tests:
          - not_null
```

When you run `dbt test`, it generates and runs SQL like:
```sql
-- not_null test on team_id
select count(*) from analytics.stg_standings where team_id is null
-- Fails if count > 0

-- unique test on team_id
select team_id, count(*) from analytics.stg_standings
group by 1 having count(*) > 1
-- Fails if any rows returned
```

---

### 5.6 Apache Airflow

**Django analogy:** Airflow is like **cron + Celery + a monitoring dashboard**, all in one. It schedules jobs, handles retries, and gives you a UI to see what ran and when.

**Core concepts:**

```
DAG (Directed Acyclic Graph)
= A pipeline definition
= Like a Django management command that knows its dependencies

Task
= One step in the pipeline
= Like a single Celery task

Operator
= The type of task (BashOperator, PythonOperator, etc.)
= Like a Celery task decorator (@app.task)

Schedule
= When the DAG runs ("@hourly", "0 9 * * *", etc.)
= Like a cron expression in crontab
```

**Our DAG** (`airflow/dags/mlb_pipeline.py`):
```python
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define the DAG — like a Django URLconf registering routes
with DAG(
    dag_id="mlb_pipeline",
    schedule="@hourly",           # Run every hour
    start_date=days_ago(1),       # Start scheduling from yesterday
    catchup=False,                # Don't backfill missed runs
) as dag:

    # Each task is like a URL pattern — named, ordered, connected
    produce = BashOperator(
        task_id="produce_to_kafka",
        bash_command="cd /opt/airflow && python -m ingestion.producer",
    )

    consume = BashOperator(
        task_id="consume_from_kafka",
        bash_command="cd /opt/airflow && python -m ingestion.consumer",
    )

    volume_check = PythonOperator(
        task_id="volume_check",
        python_callable=_volume_check,   # Runs Python directly, not bash
    )

    # Task dependencies — like Django URL includes() but for execution order
    produce >> consume >> dbt_deps >> dbt_staging >> dbt_marts >> [
        dbt_test, source_freshness, volume_check
    ]
    # >> means "this must succeed before next task starts"
    # [list] means "these run in parallel"
```

**The DAG graph looks like this:**
```
produce_to_kafka
      │
      ▼
consume_from_kafka
      │
      ▼
   dbt_deps
      │
      ▼
  dbt_staging
      │
      ▼
  dbt_marts
   ╱   │   ╲
  ╱    │    ╲
dbt_ source_ volume_
test  fresh-  check
      ness
```

**Why Airflow for this?** Without Airflow, you'd have a cron job running a bash script. If it fails, you don't know why. If a task fails halfway through, you have to re-run everything. Airflow gives you:
- **Retry logic** — failed tasks retry automatically
- **Visibility** — see every task's logs and status in the UI
- **Dependencies** — dbt only runs after Kafka consumer finishes
- **Alerting** — `on_failure_callback` triggers when something breaks

---

### 5.7 Plotly Dash

**Django analogy:** Dash is like **Django views + templates + AJAX**, but written entirely in Python. No HTML, no JavaScript, no Jinja2.

```python
# Instead of this Django view:
def standings_view(request):
    standings = Standings.objects.all().order_by("league", "division_rank")
    return render(request, "standings.html", {"standings": standings})

# You write this Dash callback:
@callback(Output("tab-content", "children"), Input("tabs", "active_tab"))
def render_tab(active_tab):
    df = get_standings()  # Reads from PostgreSQL mart table
    fig = px.bar(df, x="team_name", y="win_pct", color="division_name")
    return dcc.Graph(figure=fig)
```

**How Dash works:**
```
User clicks tab in browser
        │
        │ WebSocket message
        ▼
  Dash server (Flask under the hood)
        │
        │ calls @callback function
        ▼
  Python function runs
  (queries Postgres, builds Plotly chart)
        │
        │ returns HTML/JSON
        ▼
  Browser updates without page reload
```

**The four tabs and what powers them:**

| Tab | Reads From | What It Shows |
|---|---|---|
| 🏆 Standings | `mart_standings` | Division/league rank, win% chart for all 30 teams |
| 🏃 Batting | `mart_player_stats` | Full season AVG, OBP, SLG, OPS for 400+ hitters |
| ⚾ Pitching | `mart_pitching_leaders` | Full season ERA, WHIP, K/9 — starters and relievers |
| 🎮 Game Results | `mart_game_results` | 650+ completed games with scores and winners |

**Key components used:**
```python
import dash_bootstrap_components as dbc
from dash import dcc, html, dash_table

dbc.Tabs(...)           # Like Bootstrap's nav tabs (no HTML needed)
dcc.Graph(figure=fig)   # Renders a Plotly chart
dash_table.DataTable()  # Sortable, filterable table (like Django admin list view)
dcc.Interval(...)       # Triggers a callback every N milliseconds (auto-refresh)
```

---

### 5.8 Prometheus + Grafana

**Django analogy:** Prometheus is like Django's logging framework — but for numbers instead of text. Grafana is the log viewer.

```
Prometheus scrapes metrics from:
├── postgres-exporter  → pg_up, pg_stat_activity_count, pg_stat_user_tables_n_live_tup
├── kafka-exporter     → kafka_consumer_group_lag, kafka_topic_partitions
└── prometheus itself  → scrape_duration_seconds

Grafana queries Prometheus and renders dashboards.
```

**Why this matters for interviews:** Knowing that your pipeline has **infrastructure observability** (Prometheus/Grafana) AND **data observability** (Elementary/dbt tests) shows you think about production operations, not just "does it run locally."

---

## 6. File-by-File Breakdown

```
diamond-pipeline/
│
├── ingestion/                     ← Data ingestion package
│   ├── __init__.py                ← Makes it a Python package (for python -m ingestion.x)
│   ├── mlb_api.py                 ← API client — all MLB API calls live here
│   ├── db.py                      ← Database utilities — engine creation, load_dataframe()
│   ├── ingest.py                  ← Direct ingest script (dev tool, bypasses Kafka)
│   ├── producer.py                ← Kafka producer — fetches API data, publishes to topics
│   └── consumer.py                ← Kafka consumer — reads topics, writes to PostgreSQL
│
├── dbt/                           ← dbt project (SQL transformation layer)
│   ├── dbt_project.yml            ← Project config: name, model materialization defaults
│   ├── profiles.yml               ← DB connection (reads from env vars)
│   ├── packages.yml               ← dbt package dependencies (Elementary, dbt_utils)
│   └── models/
│       ├── staging/               ← Silver layer: clean, type, deduplicate
│       │   ├── _sources.yml       ← Declares raw_* tables as dbt sources + freshness config
│       │   ├── _staging.yml       ← Schema tests (not_null, unique) for staging models
│       │   ├── stg_teams.sql      ← View: typed team reference data
│       │   ├── stg_standings.sql  ← View: typed standings snapshot
│       │   ├── stg_schedule.sql   ← View: deduplicates by game_pk, best status wins
│       │   ├── stg_game_events.sql ← Incremental table: complete plays only
│       │   ├── stg_hitting_stats.sql  ← View: official season hitting stats, dedup by player
│       │   └── stg_pitching_stats.sql ← View: official season pitching stats, dedup by player
│       └── marts/                 ← Gold layer: business-ready aggregations
│           ├── _marts.yml         ← Schema tests for mart models
│           ├── mart_standings.sql      ← Standings + division/league rank (window functions)
│           ├── mart_game_results.sql   ← 650+ season results with winner/loser/run diff
│           ├── mart_player_stats.sql   ← Full season batting: AVG, OBP, SLG, OPS (official)
│           └── mart_pitching_leaders.sql ← Full season ERA/WHIP leaders, starters + relievers
│
├── airflow/                       ← Airflow configuration
│   ├── Dockerfile                 ← Extends apache/airflow with our requirements
│   ├── requirements.txt           ← Packages installed in Airflow's Python env
│   ├── scripts/
│   │   └── init.sh                ← Runs on first start: db migrate + create admin user
│   └── dags/
│       └── mlb_pipeline.py        ← THE DAG: defines all tasks and their order
│
├── dashboard/                     ← Plotly Dash app
│   ├── Dockerfile                 ← Python 3.11-slim + requirements.txt
│   ├── __init__.py
│   ├── data.py                    ← All database queries (reads from analytics schema)
│   └── app.py                     ← Full Dash app: layout, callbacks, charts
│
├── observability/
│   ├── prometheus/
│   │   └── prometheus.yml         ← Scrape config: which exporters to monitor
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/       ← Auto-provisions Prometheus as Grafana datasource
│       │   └── dashboards/        ← Tells Grafana where to find dashboard JSON files
│       └── dashboards/
│           └── mlb_pipeline.json  ← Pre-built Grafana dashboard definition
│
├── postgres/
│   └── init.sql                   ← Runs on fresh postgres start: CREATE DATABASE airflow
│
├── docker-compose.yml             ← Defines and wires together all 11 services
├── requirements.txt               ← Python deps for LOCAL development only
├── Makefile                       ← Convenience commands (make up, make produce, etc.)
├── .env.example                   ← Template for environment variables
└── .gitignore                     ← Excludes .env, .venv, dbt/target, dbt/dbt_packages
```

---

## 7. ETL vs ELT

You'll hear both terms in DE interviews. Here's the difference and which one we're doing:

### ETL (Extract → Transform → Load)
Transform data **before** loading into the database.
Used when: storage is expensive, old-school data warehouses, strict schema enforcement upfront.

```
API → [Python transforms data in memory] → Database
```

### ELT (Extract → Load → Transform)
Load raw data first, then transform it **inside** the database.
Used when: storage is cheap (it is now), SQL is more expressive than Python for transforms, you want to preserve raw data.

```
API → Database (raw) → [dbt transforms inside DB] → Database (clean)
```

**We're doing ELT.** Raw data lands in `public.raw_*` tables first. Then dbt transforms it inside PostgreSQL into `analytics.stg_*` and `analytics.mart_*`. This is the modern standard.

**Why ELT wins today:**
- You can always re-run dbt on the raw data if you discover a bug in your transformation
- SQL is purpose-built for data transformation (window functions, CTEs, aggregations)
- Your raw data is an audit trail — you can always see what the API actually returned

---

## 8. Key Data Engineering Concepts

### Incremental Loading

Instead of reprocessing ALL game events every hour, we only process **new** ones:

```sql
-- stg_game_events.sql (simplified)
select * from raw_game_events
{% if is_incremental() %}
where event_id not in (select event_id from this_table_already)
{% endif %}
```

**Django analogy:** Like `MyModel.objects.filter(created_at__gt=last_sync_time)` instead of fetching all records every time.

**Why it matters:** With 100,000 game events, reprocessing all of them every hour wastes time and compute. Incremental models only process the delta.

---

### Consumer Groups (Kafka)

Our consumer has `group.id = "mlb-pipeline-consumer"`. Kafka tracks which messages this group has read (the **offset**). If the consumer crashes and restarts, it picks up where it left off — it doesn't re-process messages it already consumed.

```
Topic: mlb.standings [msg0][msg1][msg2][msg3][msg4][msg5]
                                              ▲
                                    offset = 3 (consumer has read msgs 0-2)
                                    Next poll returns msg3
```

**Django analogy:** Like Django's `django_migrations` table — it tracks which migrations have run so you don't run them twice.

---

### Idempotency

A critical DE concept: running the pipeline twice should produce the same result as running it once.

We achieve this by:
- `if_exists="replace"` for snapshot tables (standings, schedule, teams) — always overwrites
- `distinct on (event_id)` in `stg_game_events` — deduplicates before inserting
- dbt's `unique_key` in incremental models — upserts, not blind inserts

**Django analogy:** Like `update_or_create()` instead of `create()`.

---

### Data Lineage

dbt tracks exactly which table feeds which model, so you can trace data from source to dashboard:

```
raw_game_events (public)
      │
      │  dbt ref()
      ▼
stg_game_events (analytics)
      │
      │  dbt ref()
      ▼
mart_player_stats (analytics)
      │
      │  pd.read_sql()
      ▼
Dashboard: Player Stats tab
```

If the MLB API changes the `event_type` field name, you can trace exactly which models and dashboard views are affected.

---

## 9. What Happens on `docker compose up`

Here's the exact startup sequence, with timing:

```
t=0s    postgres starts
        → Runs postgres/init.sql: CREATE DATABASE airflow
        → Creates mlb_pipeline database
        → Health check: pg_isready every 10s

t=0s    zookeeper starts (required by Kafka)

t=0s    kafka starts
        → Waits for zookeeper
        → Health check: kafka-broker-api-versions every 15s

t=~30s  postgres HEALTHY ✓
        kafka HEALTHY ✓

t=~30s  airflow-init starts (waits for both)
        → Runs airflow/scripts/init.sh
        → airflow db migrate  (creates Airflow metadata tables in airflow database)
        → Creates admin user (admin/admin)
        → EXITS with code 0 (success)

t=~60s  airflow-init COMPLETED ✓

t=~60s  airflow-webserver starts (waits for init)
        → Serves UI at localhost:8090

t=~60s  airflow-scheduler starts (waits for init)
        → Scans dags/ directory, finds mlb_pipeline.py
        → Schedules first run for current hour

t=0s    kafka-ui starts (waits for kafka)
        → Serves topic browser at localhost:8080

t=0s    postgres-exporter starts (waits for postgres)
        → Exposes PostgreSQL metrics at localhost:9187

t=0s    kafka-exporter starts (waits for kafka)
        → Exposes Kafka metrics at localhost:9308

t=0s    prometheus starts
        → Scrapes exporters every 15s

t=0s    grafana starts
        → Auto-provisions Prometheus datasource
        → Auto-loads MLB Pipeline dashboard

t=~30s  dashboard starts (waits for postgres)
        → Runs dashboard/app.py via python -m dashboard.app
        → Serves Plotly Dash app at localhost:8050

t=~70s  airflow-scheduler triggers first DAG run
        → produce_to_kafka → consume_from_kafka → dbt_deps
        → dbt_staging → dbt_marts
        → dbt_test + source_freshness + volume_check (parallel)

t=~90s  Pipeline run completes
        → 30 teams, 30 standings, 650+ season games, official stats for 500+ hitters + 600+ pitchers
        → 6 raw tables, 6 staging models, 4 mart tables populated
        → Dashboard shows full 2026 season data
```

**Services and their ports at a glance:**
```
localhost:8050  → Plotly Dash dashboard    (your analytics frontend)
localhost:8090  → Airflow UI               (pipeline monitoring, admin/admin)
localhost:8080  → Kafka UI                 (message browser)
localhost:3000  → Grafana                  (infra observability, admin/admin)
localhost:9090  → Prometheus               (raw metrics)
localhost:5433  → PostgreSQL               (psql -U mlb -d mlb_pipeline -h localhost -p 5433)
localhost:9092  → Kafka broker             (for local producer/consumer dev)
```

---

## Closing Thoughts — The Interview Story

When a DE interviewer asks "walk me through your project," here's the narrative:

> *"I built an end-to-end ELT pipeline processing full 2026 MLB season data. The producer fetches from six MLB Stats API endpoints and publishes across six Kafka topics — teams, standings, full season schedules, play-by-play events, and official season stats for 500+ hitters and 600+ pitchers. A consumer reads those topics and writes batched DataFrames to PostgreSQL as the raw layer, using snapshot semantics for stats and append semantics for event data.*
>
> *I then built a dbt transformation layer on top: staging models clean, type, and deduplicate the raw data — handling mid-season trade records and in-progress game plays — and mart models compute full season leaderboards using SQL window functions. dbt schema tests validate data quality on every run.*
>
> *The entire pipeline is orchestrated by Airflow on an hourly schedule, with retry logic, failure alerting, and volume/freshness checks as downstream tasks. Infrastructure metrics — Kafka consumer lag, PostgreSQL connections, table row counts — are scraped by Prometheus and visualized in Grafana. dbt run results feed into Elementary for data observability.*
>
> *Finally, a Plotly Dash app reads directly from the mart tables and serves four dashboard views: division standings, a full season batting leaderboard with official MLB stats, a pitching leaderboard separating starters and relievers, and a complete game results log for the season. The whole stack runs on docker compose up — no local setup required."*

That story hits: Kafka, Airflow, dbt, PostgreSQL, Pandas, Prometheus, Grafana, ELT, incremental loading, snapshot vs append semantics, data quality, observability, and Docker. Every keyword from the job description — with real season-scale data to back it up.
