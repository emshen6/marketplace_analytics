import configparser
import os
import pandas as pd
import logging
import requests
from datetime import datetime, timedelta
from pgdb import PGDatabase

dirname = os.path.dirname(__file__)
config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

API_URL = config["API"]["API_URL"]
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

def load_and_insert_data_for_date(date_str):
    try:
        response = requests.get(f"{API_URL}?date={date_str}")
        response.raise_for_status()
        data = response.json()
        if not data:
            logging.info(f"No data returned for {date_str}. Stopping.")
            return False

        df = pd.DataFrame(data)
        logging.info(f"Data for {date_str} successfully loaded from API.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error loading data from API for {date_str}: {e}")
        return False

    try:
        df['purchase_datetime'] = pd.to_datetime(df['purchase_datetime']).dt.date
        df['purchase_time'] = (
            pd.to_datetime('00:00:00', format='%H:%M:%S') + 
            pd.to_timedelta(df['purchase_time_as_seconds_from_midnight'], unit='s')
        ).dt.time
        logging.info(f"Data for {date_str} successfully transformed.")
    except Exception as e:
        logging.error(f"Error transforming data for {date_str}: {e}")
        return False
    
    clients_df = df[['client_id', 'gender']].drop_duplicates().sort_values(by='client_id')
    products_df = df[['product_id', 'price_per_item', 'discount_per_item']].drop_duplicates().sort_values(by='product_id')

    try:
        for _, row in clients_df.iterrows():
            query = f"INSERT INTO clients (client_id, gender) VALUES ({row['client_id']}, '{row['gender']}') ON CONFLICT (client_id) DO NOTHING"
            database.post(query)
        logging.info(f"Client data for {date_str} successfully inserted into the table.")
    except Exception as e:
        logging.error(f"Error inserting client data for {date_str}: {e}")

    try:
        for _, row in products_df.iterrows():
            query = f"INSERT INTO products (product_id, price_per_item, discount_per_item) VALUES ({row['product_id']}, {row['price_per_item']}, {row['discount_per_item']}) ON CONFLICT (product_id) DO NOTHING"
            database.post(query)
        logging.info(f"Product data for {date_str} successfully inserted into the table.")
    except Exception as e:
        logging.error(f"Error inserting product data for {date_str}: {e}")

    try:
        for _, row in df.iterrows():
            query = f"INSERT INTO purchases (client_id, product_id, purchase_datetime, purchase_time, quantity, total_price) VALUES ({row['client_id']}, {row['product_id']}, '{row['purchase_datetime']}', '{row['purchase_time']}', {row['quantity']}, {row['total_price']})"
            database.post(query)
        logging.info(f"Purchase data for {date_str} successfully inserted into the table.")
    except Exception as e:
        logging.error(f"Error inserting purchase data for {date_str}: {e}")

    return True

def find_start_date():
    check_date = datetime.now() - timedelta(days=1)
    while True:
        date_str = check_date.strftime("%Y-%m-%d")
        try:
            response = requests.get(f"{API_URL}?date={date_str}")
            response.raise_for_status()
            if response.json():
                logging.info(f"Earliest date with available data: {date_str}")
                return check_date
        except requests.exceptions.RequestException:
            logging.error(f"Failed to retrieve data for {date_str}")

        check_date -= timedelta(days=1)
        if check_date < datetime.now() - timedelta(days=365 * 5):
            logging.error("No data found within 5 years range.")
            raise ValueError("No data available in the past 5 years.")

start_date = find_start_date()
print(start_date)
#end_date = datetime.now()  # до текущего дня

# Цикл по каждому дню в диапазоне дат от start_date до end_date
'''
current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime("%Y-%m-%d")
    if not load_and_insert_data_for_date(date_str):
        break  # Остановка, если данных нет или произошла ошибка
    current_date += timedelta(days=1)
'''