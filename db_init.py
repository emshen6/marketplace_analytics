import configparser
import os
import pandas as pd
import logging
import requests
from datetime import timedelta
from pgdb import PGDatabase
import json

dirname = os.path.dirname(__file__)
config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

API_URL = config["API"]["API_URL"]
DATABASE_CREDS = config["Database"]
LOG_PATH = config["Files"]["LOG_PATH"]
DATA_PATH = config["Files"]["DATA_PATH"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=LOG_PATH,
    filemode="a"
)

try:
    database = PGDatabase(
        host=DATABASE_CREDS["HOST"],
        database=DATABASE_CREDS["DATABASE"],
        user=DATABASE_CREDS["USER"],
        password=DATABASE_CREDS["PASSWORD"],
    )
    logging.info("Successfully connected to the database.")
except Exception as e:
    logging.error(f"Database connection error: {e}")
    raise

def check_data_available(date):
    params = {"date": date.strftime("%Y-%m-%d")}
    try:
        response = requests.get(API_URL, params=params)
        return response.status_code == 200 and bool(response.json())
    except requests.RequestException as e:
        logging.error(f"API request error: {e}")
        return False

def find_earliest_available_date():
    end_date = pd.to_datetime("today").normalize()
    start_date = (end_date - timedelta(days=365 * 5)).normalize()

    while start_date < end_date:
        middle_date = start_date + (end_date - start_date) // 2
        middle_date = middle_date.normalize()

        if check_data_available(middle_date):
            end_date = middle_date
        else:
            start_date = middle_date + timedelta(days=1)

    logging.info(f"Earliest available data found on: {start_date}")
    return start_date

def insert_data_to_db(df):
    try:
        df = df.sort_values(by='purchase_time')
        clients_df = df[['client_id', 'gender']].drop_duplicates()
        products_df = df[['product_id', 'price_per_item', 'discount_per_item']].drop_duplicates()

        for _, row in clients_df.iterrows():
            query = (
                "INSERT INTO clients (client_id, gender) VALUES (%s, %s) "
                "ON CONFLICT (client_id) DO NOTHING"
            )
            database.post(query, (row['client_id'], row['gender']))

        for _, row in products_df.iterrows():
            query = (
                "INSERT INTO products (product_id, price_per_item, discount_per_item) VALUES (%s, %s, %s) "
                "ON CONFLICT (product_id, price_per_item, discount_per_item) DO NOTHING"
            )
            database.post(query, (row['product_id'], row['price_per_item'], row['discount_per_item']))
        
        for _, row in df.iterrows():
            query = (
                "INSERT INTO purchases (client_id, product_id, price_per_item, discount_per_item, purchase_datetime, "
                "purchase_time, quantity, total_price) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            )
            database.post(
                query, 
                (row['client_id'], row['product_id'], row['price_per_item'], row['discount_per_item'],
                 row['purchase_datetime'], row['purchase_time'], row['quantity'], row['total_price'])
            )

        logging.info("All data successfully inserted into the database.")

    except Exception as e:
        logging.error(f"Data insertion error: {e}")

def fill_database(date):
    params = {"date": date.strftime("%Y-%m-%d")}

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if not data:
            logging.warning(f"No data available for {date}.")
            return

        df = pd.DataFrame(data)
        df['purchase_datetime'] = pd.to_datetime(df['purchase_datetime']).dt.date
        df['purchase_time'] = (pd.to_datetime('00:00:00') + 
                               pd.to_timedelta(df['purchase_time_as_seconds_from_midnight'], unit='s')).dt.time

        insert_data_to_db(df)

    except requests.RequestException as e:
        logging.error(f"API request error: {e}")
    except Exception as e:
        logging.error(f"Data processing error: {e}")

earliest_date = find_earliest_available_date()
today = pd.to_datetime("today").normalize()

for single_date in pd.date_range(earliest_date, today):
    fill_database(single_date)

database.close()
logging.info("Database population completed.")