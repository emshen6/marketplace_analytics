import configparser
import os
from datetime import datetime, timedelta
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

with open(DATA_PATH, "r", encoding="utf-8") as file:
    data = json.load(file)
    df = pd.DataFrame(data)

os.remove(DATA_PATH)

df['purchase_datetime'] = pd.to_datetime(df['purchase_datetime']).dt.date

df['purchase_time'] = (pd.to_datetime('00:00:00', format='%H:%M:%S') + 
                       pd.to_timedelta(df['purchase_time_as_seconds_from_midnight'], unit='s')).dt.time

clients_df = df[['client_id', 'gender']].drop_duplicates()
products_df = df[['product_id', 'price_per_item', 'discount_per_item']].drop_duplicates()

database = PGDatabase(
    host=DATABASE_CREDS["HOST"],
    database=DATABASE_CREDS["DATABASE"],
    user=DATABASE_CREDS["USER"],
    password=DATABASE_CREDS["PASSWORD"],
)

for i, row in clients_df.iterrows():
    query = f"insert into clients values ('{row['client_id']}', '{row['gender']}')"
    database.post(query)

for i, row in products_df.iterrows():
    query = f"insert into products values ('{row['product_id']}', 'NULL', '{row['price_per_item']}', '{row['discount_per_item']}')"
    database.post(query)

for i, row in df.iterrows():
    query = f"insert into products values ('{row['client_id']}', '{row['product_id']}', '{row['purchase_datetime']}', '{row['purchase_time']}', '{row['quantity']}', '{row['total_price']}')"
    database.post(query)