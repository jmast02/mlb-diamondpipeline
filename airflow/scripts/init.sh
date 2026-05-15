#!/usr/bin/env bash
set -e

echo "==> Running airflow db migrate..."
airflow db migrate

echo "==> Creating admin user (skips if already exists)..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    2>/dev/null || echo "  user already exists, skipping"

echo "==> Airflow init complete."
