"""
MLB Pipeline DAG
Flow: produce → consume → dbt staging → dbt marts → [tests | freshness | volume check]
Schedule: hourly
"""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

PROJECT_ROOT = "/opt/airflow"
DBT_DIR = f"{PROJECT_ROOT}/dbt"


def _on_failure(context) -> None:
    ti = context["task_instance"]
    print(
        f"[PIPELINE ALERT] Task '{ti.task_id}' failed "
        f"in DAG '{ti.dag_id}' | run: {ti.execution_date}"
    )
    # Replace with Slack/PagerDuty hook in production


def _volume_check(**_) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(
        "postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "mlb"),
            pw=os.getenv("POSTGRES_PASSWORD", "mlbpassword"),
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "mlb_pipeline"),
        )
    )

    thresholds: dict[str, int] = {
        "raw_teams":                     25,
        "raw_standings":                 25,
        "raw_game_events":               50,
        "analytics.mart_player_stats":   10,
        "analytics.mart_standings":      25,
    }

    failures: list[str] = []
    with engine.connect() as conn:
        for table, min_rows in thresholds.items():
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            status = "ok" if count >= min_rows else "FAIL"
            print(f"  [{status}] {table}: {count} rows (threshold: {min_rows})")
            if count < min_rows:
                failures.append(f"{table}: {count} < {min_rows}")

    if failures:
        raise ValueError("Volume checks failed:\n" + "\n".join(failures))


default_args = {
    "owner": "mlb-pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": _on_failure,
}

with DAG(
    dag_id="mlb_pipeline",
    description="MLB data pipeline: Kafka ingest → dbt transform → validated analytics",
    default_args=default_args,
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["mlb", "pipeline"],
) as dag:

    produce = BashOperator(
        task_id="produce_to_kafka",
        bash_command=f"cd {PROJECT_ROOT} && python -m ingestion.producer",
    )

    consume = BashOperator(
        task_id="consume_from_kafka",
        bash_command=f"cd {PROJECT_ROOT} && python -m ingestion.consumer",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir .",
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --select staging",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --select marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --select mlb_pipeline",
    )

    freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --profiles-dir .",
    )

    volume_check = PythonOperator(
        task_id="volume_check",
        python_callable=_volume_check,
    )

    # DAG flow
    produce >> consume >> dbt_deps >> dbt_staging >> dbt_marts >> [dbt_test, freshness, volume_check]
