import configparser
import os
import pandas as pd
import json
import logging
from pgdb import PGDatabase

dirname = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

DATA_PATH = config["Files"]["DATA_PATH"]
DATABASE_CREDS = config["Database"]
LOG_PATH = config["Files"]["LOG_PATH"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=LOG_PATH,
    filemode="a"
)

try:
    with open(DATA_PATH, "r", encoding="utf-8") as file:
        data = json.load(file)
        df = pd.DataFrame(data)
    logging.info("Data successfully loaded from JSON file.")
except Exception as e:
    logging.error(f"Error loading data from JSON: {e}")
    raise

try:
    os.remove(DATA_PATH)
    logging.info(f"File {DATA_PATH} successfully deleted.")
except Exception as e:
    logging.error(f"Error deleting file {DATA_PATH}: {e}")

try:
    df['purchase_datetime'] = pd.to_datetime(df['purchase_datetime']).dt.date
    df['purchase_time'] = (pd.to_datetime('00:00:00', format='%H:%M:%S') + 
                           pd.to_timedelta(df['purchase_time_as_seconds_from_midnight'], unit='s')).dt.time
    logging.info("Data successfully transformed.")
except Exception as e:
    logging.error(f"Error transforming data: {e}")
    raise

clients_df = df[['client_id', 'gender']].drop_duplicates().sort_values(by='client_id')
products_df = df[['product_id', 'price_per_item', 'discount_per_item']].drop_duplicates().sort_values(by='product_id')
df = df.sort_values(by='purchase_time')

try:
    database = PGDatabase(
        host=DATABASE_CREDS["HOST"],
        database=DATABASE_CREDS["DATABASE"],
        user=DATABASE_CREDS["USER"],
        password=DATABASE_CREDS["PASSWORD"],
    )
    logging.info("Successfully connected to the database.")
except Exception as e:
    logging.error(f"Error connecting to the database: {e}")
    raise

try:
    for i, row in clients_df.iterrows():
        query = f"INSERT INTO clients (client_id, gender) VALUES ({row['client_id']}, '{row['gender']}')"
        database.post(query)
    logging.info("Client data successfully inserted into the table.")
except Exception as e:
    logging.error(f"Error inserting client data: {e}")

try:
    for i, row in products_df.iterrows():
        query = f"INSERT INTO products (product_id, price_per_item, discount_per_item) VALUES ({row['product_id']}, {row['price_per_item']}, {row['discount_per_item']})"
        database.post(query)
    logging.info("Product data successfully inserted into the table.")
except Exception as e:
    logging.error(f"Error inserting product data: {e}")

try:
    for i, row in df.iterrows():
        query = f"INSERT INTO purchases (client_id, product_id, purchase_datetime, purchase_time, quantity, total_price) VALUES ({row['client_id']}, {row['product_id']}, '{row['purchase_datetime']}', '{row['purchase_time']}', {row['quantity']}, {row['total_price']})"
        database.post(query)
    logging.info("Purchase data successfully inserted into the table.")
except Exception as e:
    logging.error(f"Error inserting purchase data: {e}")