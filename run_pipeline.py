# run_pipeline.py
import logging
import sys
from datetime import datetime
from pathlib import Path
from transform import load_raw_data_chunked, transform_and_aggregate
from publish import main as publish_to_gsheet   # หรือ publish_to_gsheet

# ──────────────────────────────────────────────────────────────
# 1. สร้างโฟลเดอร์ logs ก่อนตั้งค่า logging (สำคัญมาก!)
# ──────────────────────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True) 

# ──────────────────────────────────────────────────────────────
# 2. ตั้งค่า logging หลังจากสร้างโฟลเดอร์แล้ว → ไม่ error แน่นอน
# ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# ฟังก์ชันหลัก
# ──────────────────────────────────────────────────────────────
def main():
    start_time = datetime.now()
    logger.info("=" * 70)
    logger.info(" FULL AUTOMATED ETL PIPELINE STARTED ")
    logger.info(f" Project path: {Path.cwd()}")
    logger.info("=" * 70)

    try:
        # 1. INGEST
        logger.info("1/3 INGEST RAW DATA → PostgreSQL")
        from ingest import main_ingest
        total_raw = main_ingest(
            csv_path="100k_a.csv",
            table_name="raw_data_200k",
            chunk_size=20_000
        )

        # 2. TRANSFORM
        logger.info("2/3 TRANSFORM & AGGREGATE → user_metrics")
        from transform import transform_and_aggregate
        unique_users = transform_and_aggregate()

        # 3. PUBLISH
        logger.info("3/3 PUBLISH → Google Sheets")
        from publish import publish_to_gsheet
        publish_to_gsheet()

        # สรุป
        duration = datetime.now() - start_time
        logger.info("=" * 70)
        logger.info(" PIPELINE 100% SUCCESS! ")
        logger.info(f"   • Raw rows     : {total_raw:,}")
        logger.info(f"   • Unique users : {unique_users:,}")
        logger.info(f"   • Duration     : {duration}")
        logger.info(f"   • Log saved    : {LOG_DIR / 'pipeline.log'}")
        logger.info("=" * 70)

    except Exception as e:
        logger.error("PIPELINE FAILED!", exc_info=True)
        sys.exit(1)


# ──────────────────────────────────────────────────────────────
# รัน
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # สร้างโฟลเดอร์ที่จำเป็นทั้งหมด
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    main()