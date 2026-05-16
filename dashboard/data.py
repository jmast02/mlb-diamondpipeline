import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


def _engine():
    return create_engine(
        "postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "mlb"),
            pw=os.getenv("POSTGRES_PASSWORD", "mlbpassword"),
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5433"),
            db=os.getenv("POSTGRES_DB", "mlb_pipeline"),
        ),
        pool_pre_ping=True,
    )


def _query(sql: str) -> pd.DataFrame:
    try:
        with _engine().connect() as conn:
            return pd.read_sql(text(sql), conn)
    except Exception as exc:
        print(f"[data] query failed: {exc}")
        return pd.DataFrame()


def get_standings() -> pd.DataFrame:
    return _query("""
        SELECT * FROM analytics.mart_standings
        ORDER BY league_name, division_name, division_rank
    """)


def get_player_stats() -> pd.DataFrame:
    return _query("""
        SELECT * FROM analytics.mart_player_stats
        ORDER BY ops DESC NULLS LAST
    """)


def get_game_results() -> pd.DataFrame:
    return _query("""
        SELECT * FROM analytics.mart_game_results
        ORDER BY game_date DESC
    """)


def get_pitching_leaders() -> pd.DataFrame:
    return _query("""
        SELECT * FROM analytics.mart_pitching_leaders
        ORDER BY role, era ASC NULLS LAST
    """)
