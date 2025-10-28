import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data_raw"
PROCESSED_DIR = BASE_DIR / "data_processed"

# Ensure output folder exists
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

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
        print(f"Converted {file} → {json_path.name} ({len(df):,} rows)")
    else:
        print(f"Skipped missing file: {file}")
