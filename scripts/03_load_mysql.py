import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# MySQL connection details
USER = "root"
PASSWORD = "root"
HOST = "localhost"
PORT = 3306
DB = "olist_db"

# Create SQLAlchemy connection
engine = create_engine(f"mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

# Load cleaned CSV
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data_processed"
file_path = PROCESSED_DIR / "olist_master_cleaned.csv"

print("Reading cleaned dataset...")
df = pd.read_csv(file_path)
print(f"Loaded {len(df):,} rows.")

# Load into MySQL table
table_name = "olist_master"
print(f"Loading data into MySQL table: {table_name} ...")

df.to_sql(table_name, con=engine, if_exists="replace", index=False, chunksize=1000)

print(f"Data loaded successfully into `{DB}` database, table `{table_name}`.")
