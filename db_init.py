import configparser
import os
import pandas as pd
import logging
import requests
from datetime import datetime, timedelta
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
    logging.error(f"Error connecting to the database: {e}")
    raise

date = "2023-12-01"

params = {
    "date": date.strftime("%Y-%m-%d")
}
    
try:
    response = requests.get(API_URL, params=params)

    if response.status_code == 200:
        with open(DATA_PATH, "w") as json_file:
            json.dump(response.json(), json_file, ensure_ascii=False, indent=4)
        logging.info("Data loaded successfully.")
    else:
        logging.error(f"Error {response.status_code}: {response.text}")
except requests.exceptions.RequestException as e:
    logging.error(f"Connection error: {e}")