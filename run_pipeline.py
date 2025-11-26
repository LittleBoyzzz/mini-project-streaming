# run_pipeline.py
import logging
import sys
from datetime import datetime
from pathlib import Path
# import functions
from transform import load_raw_data_chunked, transform_and_aggregate
from publish import publish_to_gsheet


LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True) 


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    start_time = datetime.now()
    logger.info("=" * 70)
    logger.info(" FULL AUTOMATED ETL PIPELINE STARTED ")
    logger.info(f" Project path: {Path.cwd()}")
    logger.info("=" * 70)

    try:
        logger.info("1/3 INGEST RAW DATA → PostgreSQL")
        from ingest import main_ingest
        total_raw = main_ingest(
            csv_path="100k_a.csv",
            table_name="raw_data_200k",
            chunk_size=20_000
        )

        logger.info("2/3 TRANSFORM & AGGREGATE → user_metrics")
        df_metrics = transform_and_aggregate()
        
        unique_users = len(df_metrics)

        logger.info("3/3 PUBLISH → Google Sheets")
        publish_to_gsheet(df=df_metrics, sheet_name='streaming')

        duration = datetime.now() - start_time
        logger.info(" PIPELINE 100% SUCCESS! ")
        logger.info(f"   • Raw rows     : {total_raw:,}")
        logger.info(f"   • Unique users : {unique_users:,}")
        logger.info(f"   • Duration     : {duration}")
        logger.info(f"   • Log saved    : {LOG_DIR / 'pipeline.log'}")

    except Exception as e:
        logger.error("PIPELINE FAILED!", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    main()