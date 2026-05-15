import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def get_engine():
    user = os.getenv("POSTGRES_USER", "mlb")
    password = os.getenv("POSTGRES_PASSWORD", "mlbpassword")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "mlb_pipeline")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True,
    )


def load_dataframe(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> int:
    engine = get_engine()
    if if_exists == "replace":
        # Pandas DROP TABLE doesn't cascade, which breaks dbt views that reference raw tables.
        # Drop with CASCADE first, then let pandas create and insert fresh.
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE'))
        df.to_sql(table_name, engine, if_exists="append", index=False, schema="public")
    else:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, schema="public")
    return len(df)


def run_query(sql: str) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)
