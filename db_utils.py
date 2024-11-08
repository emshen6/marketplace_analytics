from typing import Optional
from pgdb import PGDatabase
from config import DATABASE_CREDS, API_URL
from logger import logger
import requests
import pandas as pd
from datetime import datetime, timedelta


try:
    database = PGDatabase(
        host=DATABASE_CREDS["HOST"],
        database=DATABASE_CREDS["DATABASE"],
        user=DATABASE_CREDS["USER"],
        password=DATABASE_CREDS["PASSWORD"],
    )
    logger.info("Successfully connected to the database.")
except Exception as e:
    logger.error(f"Database connection error: {e}")
    raise


def check_data_available(date: datetime) -> bool:
    params = {"date": date.strftime("%Y-%m-%d")}
    try:
        response = requests.get(API_URL, params=params)
        return response.status_code == 200 and bool(response.json())
    except requests.RequestException as e:
        logger.error(f"API request error: {e}")
        return False


def load_data_from_api(date: datetime) -> Optional[pd.DataFrame]:
    params = {"date": date.strftime("%Y-%m-%d")}
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        data = response.json()

        if data:
            df = pd.DataFrame(data)
            df['purchase_datetime'] = pd.to_datetime(df['purchase_datetime']).dt.date
            df['purchase_time'] = (pd.to_datetime('00:00:00') +
                                   pd.to_timedelta(df['purchase_time_as_seconds_from_midnight'], unit='s')).dt.time
            return df
        else:
            logger.warning(f"No data available for {date.strftime('%Y-%m-%d')}")
            return None
    except requests.RequestException as e:
        logger.error(f"API request error: {e}")
        return None
    except Exception as e:
        logger.error(f"Data processing error: {e}")
        return None


def insert_data_to_db(df: pd.DataFrame) -> None:
    try:
        df = df.sort_values(by='purchase_time')

        clients_df = df[['client_id', 'gender']].drop_duplicates()
        for _, row in clients_df.iterrows():
            query = (
                "INSERT INTO clients (client_id, gender) VALUES (%s, %s) "
                "ON CONFLICT (client_id) DO NOTHING"
            )
            database.post(query, (int(row['client_id']), row['gender']))

        products_df = df[['product_id', 'price_per_item', 'discount_per_item']].drop_duplicates()
        for _, row in products_df.iterrows():
            query = (
                "INSERT INTO products (product_id, price_per_item, discount_per_item) VALUES (%s, %s, %s) "
                "ON CONFLICT (product_id, price_per_item, discount_per_item) DO NOTHING"
            )
            database.post(query, (int(row['product_id']), int(row['price_per_item']), int(row['discount_per_item'])))

        for _, row in df.iterrows():
            query = (
                "INSERT INTO purchases (client_id, product_id, price_per_item, discount_per_item, purchase_datetime, "
                "purchase_time, quantity, total_price) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            )
            database.post(
                query, 
                (int(row['client_id']), int(row['product_id']),
                 int(row['price_per_item']), int(row['discount_per_item']),
                 row['purchase_datetime'], row['purchase_time'], int(row['quantity']), float(row['total_price']))
            )

        logger.info("All data successfully inserted into the database.")
    except Exception as e:
        logger.error(f"Data insertion error: {e}")