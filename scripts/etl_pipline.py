import os
import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
import logging
from datetime import datetime

# SETUP LOGGING
BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

# PATHS & CONFIG
RAW_DIR = BASE_DIR / "data_raw"
PROCESSED_DIR = BASE_DIR / "data_processed"
PROCESSED_DIR.mkdir(exist_ok=True)

DB_NAME = "olist_db"
DB_USER = "root"
DB_PASS = "root"
DB_HOST = "localhost"
DB_PORT = "3306"

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# EXTRACT → TRANSFORM → LOAD FUNCTIONS

def extract():
    """Convert raw CSV → JSONL"""
    logging.info("Starting extraction process...")

    files = [
        "olist_orders_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "product_category_name_translation.csv",
    ]

    for file in files:
        csv_path = RAW_DIR / file
        if csv_path.exists():
            df = pd.read_csv(csv_path, low_memory=False)
            json_path = PROCESSED_DIR / f"{csv_path.stem}.jsonl"
            df.to_json(json_path, orient="records", lines=True, date_format="iso")
            logging.info(f"Extracted {file} → {json_path.name} ({len(df):,} rows)")
        else:
            logging.warning(f"Missing file skipped: {file}")

def load():
    """Load JSONL files into MySQL"""
    logging.info("Starting loading process...")

    json_files = list(PROCESSED_DIR.glob("*.jsonl"))
    for json_file in json_files:
        table_name = json_file.stem
        try:
            df = pd.read_json(json_file, lines=True)
            df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=1000)
            logging.info(f"Loaded {table_name} → MySQL ({len(df):,} rows)")
        except Exception as e:
            logging.error(f"Failed to load {table_name}: {e}")

def run_pipeline():
    """Main ETL runner"""
    logging.info("ETL Pipeline Started")
    extract()
    load()
    logging.info("ETL Pipeline Completed Successfully")

# ENTRY POINT
if __name__ == "__main__":
    run_pipeline()
