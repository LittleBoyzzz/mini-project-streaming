import os
import sys
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

def get_db_engine() -> Engine:
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASS")

    if not all([pg_db, pg_user, pg_pass]):
        raise ValueError("Missing required database credentials: PG_DB, PG_USER, PG_PASS")

    connection_url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    engine = create_engine(
        connection_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False
    )
    return engine

def test_connection(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection successful.")
        return True
    except OperationalError as e:
        print(f"Database connection failed: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def load_raw_data_chunked(
    csv_path: str = "100k_a.csv", 
    chunk_size: int = 10_000,
    table_name: str = "raw_data_200k"
) -> int:
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        sys.exit(1)

    print(f"Reading CSV: {csv_path} and loading to {table_name} in chunks...")

    column_names = [
        'session_id', 'timestamp_ms', 'user_name', 'start_metric', 'end_metric'
    ]

    chunk_iter = pd.read_csv(
        csv_path, 
        header=None, 
        names=column_names, 
        chunksize=chunk_size, 
        low_memory=False
    )

    engine = get_db_engine()

    if not test_connection(engine):
        print("Connection failed. Exiting.")
        sys.exit(1)

    first_chunk = True
    total_rows = 0

    for chunk in chunk_iter:
        rows = len(chunk)
        total_rows += rows

        if_exists = "replace" if first_chunk else "append"

        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=chunk_size
        )

        print(f"Loaded {rows:,} rows (total: {total_rows:,}) into {table_name}")
        first_chunk = False

    print(f"Raw data ingestion complete. Total rows: {total_rows:,}")
    return total_rows

def transform_and_aggregate(
    raw_table: str = "raw_data_200k",
    final_table: str = "user_metrics"
) -> int:
    engine = get_db_engine()

    if not test_connection(engine):
        print("Database connection not available. Cannot perform transformation/aggregation.")
        sys.exit(1)

    print(f"Reading all data from {raw_table} for processing...")
    
    try:
        df = pd.read_sql(f'SELECT * FROM "{raw_table}"', con=engine)
    except Exception as e:
        print(f"Error reading data from {raw_table}: {e}")
        sys.exit(1)
        
    print(f"Data read complete. Starting transformation on {len(df):,} rows.")

    df['user_name'] = df['user_name'].str.lower()
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_ms'], unit='ms', origin='unix')
    df['duration'] = df['end_metric'] - df['start_metric']
    
    print("Performing aggregation by user...")
    df_agg = df.groupby('user_name').agg(
        total_duration=('duration', 'sum'),
        action_count=('session_id', 'count'),
        first_timestamp_utc=('timestamp_utc', 'min'),
        last_timestamp_utc=('timestamp_utc', 'max')
    ).reset_index()

    print(f"Aggregation complete. Total unique users: {len(df_agg):,}")
    print(f"Loading final metrics to table: {final_table}")

    try:
        df_agg.to_sql(
            name=final_table,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi"
        )
        print("Final aggregated data load complete. Table created/updated successfully.")
    except Exception as e:
        print(f"Error loading final data to database: {e}")
        return len(df_agg)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="ETL script for loading raw CSV data and transforming it into user metrics."
    )
    parser.add_argument("--csv", type=str, default="100k_a.csv",
                        help="Path to CSV file to ingest raw data from.")
    parser.add_argument("--chunksize", type=int, default=10000,
                        help="Chunk size for raw data ingestion.")
    parser.add_argument("--raw_table", type=str, default="raw_data_200k",
                        help="Temporary table name for raw data ingestion.")
    parser.add_argument("--final_table", type=str, default="user_metrics",
                        help="Destination table name for aggregated data.")
    args = parser.parse_args()

    load_raw_data_chunked(args.csv, args.chunksize, args.raw_table)
    
    transform_and_aggregate(args.raw_table, args.final_table)