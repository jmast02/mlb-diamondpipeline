# MLB GameFlow — Data Engineering Portfolio Project

A real-time MLB data pipeline built to demonstrate end-to-end data engineering skills including ingestion, streaming, transformation, orchestration, and visualization.

---

## Project Goal

Build a production-style data pipeline that ingests live MLB game data, normalizes and transforms it through multiple layers, stores it in a relational database, and serves it through an interactive dashboard. The project is designed to mirror real-world data engineering workflows and highlight skills relevant to mid-level DE roles.

---

## Architecture Overview

```
MLB Stats API
     ↓
Kafka (message broker / event streaming)
     ↓
Python Consumer (Pandas / NumPy transformations)
     ↓
dbt (data modeling and normalization)
     ↓
PostgreSQL (data warehouse)
     ↓
Plotly Dash (interactive dashboard)
     ↓
Airflow (pipeline orchestration and scheduling)
     ↓
Prometheus + Grafana (infrastructure observability)
Elementary (data observability)
```

---

## Tech Stack

### Data Source
- **MLB Stats API** — free, no API key required, provides live scores, player stats, play-by-play, standings, and team data

### Ingestion & Streaming
- **Apache Kafka** — event-driven message broker for streaming game events in real time, run locally via Docker

### Transformation
- **Python** — core language for all pipeline logic
- **Pandas** — data manipulation and transformation
- **NumPy** — numerical computations and statistical analysis
- **dbt (data build tool)** — SQL-based data modeling, normalization, and transformation layer on top of PostgreSQL

### Storage
- **PostgreSQL** — relational data warehouse for storing normalized game and player data

### Orchestration
- **Apache Airflow** — pipeline scheduling and orchestration, runs ingestion and transformation jobs on a defined schedule

### Visualization
- **Plotly Dash** — Python-based interactive dashboard, publicly deployable

### Observability
- **Prometheus** — metrics collection for Kafka, PostgreSQL, and Airflow (pipeline health monitoring)
- **Grafana** — visualization layer for Prometheus metrics; Datadog-equivalent dashboard for pipeline observability
- **Elementary** — dbt-native data observability; monitors model freshness, volume anomalies, and schema changes

### Infrastructure
- **Docker / Docker Compose** — local containerization for Kafka, PostgreSQL, Airflow, Prometheus, and Grafana

---

## Dashboard Views

1. **Live Standings** — current AL/NL standings updated on schedule
2. **Player Performance Trends** — batting average, ERA, strikeouts over time
3. **Team Win Probability** — historical win/loss trends by team
4. **Pitcher vs Batter Matchups** — head-to-head stats for key matchups

---

## Project Milestones

### Week 1 — Foundation
- Set up project repo and folder structure
- Connect to MLB Stats API
- Ingest live game scores and player stats with Python
- Load raw data into PostgreSQL using Pandas
- Validate data quality and schema

### Week 2 — Streaming Layer
- Set up Kafka locally via Docker
- Build a Kafka producer that publishes MLB game events
- Build a Kafka consumer that reads events and writes to PostgreSQL
- Test end-to-end message flow

### Week 3 — Transformation Layer
- Set up dbt project connected to PostgreSQL
- Build dbt models for:
  - Normalized player stats (incremental model with `unique_key`)
  - Game results and standings
  - Pitcher/batter matchup aggregations
- Add dbt schema tests and `dbt source freshness` checks
- Install and configure Elementary for data observability dashboard

### Week 4 — Orchestration + Infrastructure Observability
- Set up Apache Airflow via Docker
- Build DAGs to:
  - Schedule hourly MLB API ingestion
  - Trigger dbt transformations after ingestion
  - Alert on pipeline failures
  - Assert data volume and freshness thresholds
- Add Prometheus + Grafana to Docker Compose
- Configure Grafana dashboards for Kafka consumer lag, Airflow task durations, and PostgreSQL metrics
- Test full scheduled pipeline run

### Week 5 — Visualization
- Set up Plotly Dash app
- Connect to PostgreSQL
- Build 3-4 dashboard views

### Week 6 — Polish
- Clean up code and add docstrings
- Write a thorough README with architecture diagram
- Add sample screenshots to README
- Push to GitHub with a clean commit history

---

## Folder Structure

```
mlb-gameflow/
├── ingestion/
│   ├── mlb_api.py          # MLB Stats API client
│   ├── producer.py         # Kafka producer
│   └── consumer.py         # Kafka consumer
├── dbt/
│   ├── models/
│   │   ├── staging/        # Raw data models
│   │   └── marts/          # Transformed business models (incl. incremental)
│   └── tests/              # dbt data quality tests
├── airflow/
│   └── dags/
│       └── mlb_pipeline.py # Main pipeline DAG
├── dashboard/
│   └── app.py              # Plotly Dash app
├── observability/
│   └── grafana/
│       └── dashboards/     # Grafana dashboard JSON configs
├── docker-compose.yml       # Kafka + PostgreSQL + Airflow + Prometheus + Grafana
├── Makefile                 # make run, make test, make dbt-run, make airflow-up
├── .env.example
├── requirements.txt
└── README.md
```

---

## Resume Bullet (once complete)

*"Built an end-to-end MLB data pipeline using Kafka, Airflow, dbt, and PostgreSQL — ingesting live game data from the MLB Stats API, normalizing it through a dbt transformation layer, and serving real-time standings and player analytics through a Plotly Dash dashboard."*

---

## Key Resume Keywords This Project Covers

- Apache Kafka (event streaming)
- Apache Airflow (orchestration)
- dbt (data transformation)
- ETL / ELT pipelines
- PostgreSQL
- Pandas / NumPy
- REST API ingestion
- Docker
- Data modeling
- Data quality
- Prometheus (metrics collection)
- Grafana (observability dashboards)
- Elementary (data observability)
- Incremental data loading

---

## Resources

- [MLB Stats API Docs](https://statsapi.mlb.com/api/) — no auth required
- [dbt Documentation](https://docs.getdbt.com/)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Kafka Python Client (confluent-kafka)](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [Plotly Dash](https://dash.plotly.com/)
