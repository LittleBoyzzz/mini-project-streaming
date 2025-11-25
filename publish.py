import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

import gspread
from gspread_dataframe import set_with_dataframe


load_dotenv()


def get_db_engine() -> Engine:
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASS")

    if not all([pg_db, pg_user, pg_pass]):
        raise ValueError("Missing required database credentials: PG_DB, PG_USER, PG_PASS")

    connection_url = (
        f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )
    return create_engine(connection_url)


def read_from_production(final_table: str) -> pd.DataFrame:
    engine = get_db_engine()
    query = f'SELECT * FROM "{final_table}"'
    print(f"Reading data from production schema → table: {final_table}")
    df = pd.read_sql(query, engine)
    print(f"Loaded {len(df):,} rows.")
    return df


def publish_to_gsheet(df: pd.DataFrame, sheet_name: str, worksheet_index: int = 0):
    print("Authenticating with Google Sheets...")
    gc = gspread.service_account(filename="google_service_account.json")

    print(f"Opening Google Sheet: {sheet_name}")
    sh = gc.open(sheet_name)

    worksheet = sh.get_worksheet(worksheet_index)
    worksheet.clear()

    print("Uploading DataFrame to Google Sheets...")
    set_with_dataframe(worksheet, df)

    print("Upload complete!")


def main():
    FINAL_TABLE = "user_metrics"        # ตารางที่ ETL ของคุณสร้างไว้
    GSHEET_NAME = "streaming"    # ชื่อ Google Sheet ของคุณ

    df = read_from_production(FINAL_TABLE)

    total_cells = df.shape[0] * df.shape[1]
    print(f"Total cells to upload: {total_cells:,}")

    if total_cells > 10_000_000:
        raise ValueError("❌ Google Sheets limit exceeded (10 million cells)")

    publish_to_gsheet(df, GSHEET_NAME)


if __name__ == "__main__":
    main()
