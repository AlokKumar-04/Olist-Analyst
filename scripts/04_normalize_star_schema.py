import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data_processed"

# Load the master dataset
df = pd.read_csv(PROCESSED_DIR / "olist_master_cleaned.csv")
print(f"Loaded dataset: {df.shape[0]:,} rows, {df.shape[1]:,} columns")

# DIM CUSTOMERS
dim_customers = df[["customer_id", "customer_unique_id", "customer_city", "customer_state"]].drop_duplicates()
dim_customers.to_csv(PROCESSED_DIR / "dim_customers.csv", index=False)
print(f"Saved dim_customers → {dim_customers.shape}")

# DIM PRODUCTS
dim_products = df[["product_id", "product_category_name", "product_name_lenght",
                   "product_description_lenght", "product_photos_qty", "product_weight_g",
                   "product_length_cm", "product_height_cm", "product_width_cm"]].drop_duplicates()
dim_products.to_csv(PROCESSED_DIR / "dim_products.csv", index=False)
print(f"Saved dim_products → {dim_products.shape}")

# DIM SELLERS
dim_sellers = df[["seller_id", "seller_city", "seller_state"]].drop_duplicates()
dim_sellers.to_csv(PROCESSED_DIR / "dim_sellers.csv", index=False)
print(f"Saved dim_sellers → {dim_sellers.shape}")

# DIM PAYMENTS
dim_payments = df[["order_id", "payment_type", "payment_installments", "payment_value"]].drop_duplicates()
dim_payments.to_csv(PROCESSED_DIR / "dim_payments.csv", index=False)
print(f"Saved dim_payments → {dim_payments.shape}")

# DIM DATES
dim_dates = df[["order_purchase_timestamp", "order_approved_at", "order_delivered_customer_date"]].drop_duplicates()
dim_dates.to_csv(PROCESSED_DIR / "dim_dates.csv", index=False)
print(f"Saved dim_dates → {dim_dates.shape}")

# FACT ORDERS
fact_orders = df[[
    "order_id", "customer_id", "product_id", "seller_id",
    "order_status", "order_purchase_timestamp", "price", "freight_value",
    "payment_value"
]]
fact_orders.to_csv(PROCESSED_DIR / "fact_orders.csv", index=False)
print(f"Saved fact_orders → {fact_orders.shape}")

print("\nStar schema files created successfully in data_processed/")
