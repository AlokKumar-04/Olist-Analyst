import pandas as pd
from pathlib import Path

# Define folders
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data_processed"

# Load data from processed JSON files
orders = pd.read_json(PROCESSED_DIR / "olist_orders_dataset.jsonl", lines=True)
customers = pd.read_json(PROCESSED_DIR / "olist_customers_dataset.jsonl", lines=True)
order_items = pd.read_json(PROCESSED_DIR / "olist_order_items_dataset.jsonl", lines=True)
products = pd.read_json(PROCESSED_DIR / "olist_products_dataset.jsonl", lines=True)
sellers = pd.read_json(PROCESSED_DIR / "olist_sellers_dataset.jsonl", lines=True)
payments = pd.read_json(PROCESSED_DIR / "olist_order_payments_dataset.jsonl", lines=True)
reviews = pd.read_json(PROCESSED_DIR / "olist_order_reviews_dataset.jsonl", lines=True)

print("Loaded all data successfully")

# Data cleaning
orders.drop_duplicates(inplace=True)
customers.drop_duplicates(inplace=True)
order_items.drop_duplicates(inplace=True)
print("Removed duplicates from main datasets.")

# Join key tables
# Join Orders → Customers
orders_customers = pd.merge(orders, customers, on="customer_id", how="left")

# Join Orders → Order Items
orders_items = pd.merge(orders_customers, order_items, on="order_id", how="left")

# Join Order Items → Products
orders_products = pd.merge(orders_items, products, on="product_id", how="left")

# Join Order Items → Sellers
orders_sellers = pd.merge(orders_products, sellers, on="seller_id", how="left")

# Join Orders → Payments
final_df = pd.merge(orders_sellers, payments, on="order_id", how="left")

print("All main tables joined successfully!")

# Handle missing values
final_df.fillna({"product_category_name": "unknown"}, inplace=True)

# Save transformed file
output_path = PROCESSED_DIR / "olist_master_cleaned.csv"
final_df.to_csv(output_path, index=False)
print(f"Saved cleaned master dataset → {output_path}")
print(f"Final dataset shape: {final_df.shape}")
