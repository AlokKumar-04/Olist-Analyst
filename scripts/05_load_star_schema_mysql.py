import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# MySQL connection settings
USER = "root"
PASSWORD = "root"
HOST = "localhost"
PORT = 3306
DB = "olist_db"

# Create SQLAlchemy connection
engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

# File paths
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data_processed"

# List of dimension + fact tables
tables = {
    "dim_customers": "dim_customers.csv",
    "dim_products": "dim_products.csv",
    "dim_sellers": "dim_sellers.csv",
    "dim_payments": "dim_payments.csv",
    "dim_dates": "dim_dates.csv",
    "fact_orders": "fact_orders.csv",
}

print("Starting to load star schema tables into MySQL\n")

#Load each CSV into MySQL
for table_name, file_name in tables.items():
    file_path = PROCESSED_DIR / file_name
    print(f"Loading {file_name} into {table_name} ")

    df = pd.read_csv(file_path)
    df.to_sql(table_name, con=engine, if_exists="replace", index=False, chunksize=1000)

    print(f"{table_name} loaded successfully → {len(df):,} rows.\n")

print("All star schema tables loaded into MySQL database successfully!")
