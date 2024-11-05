import configparser
import os
import json
import requests
import logging
from datetime import datetime, timedelta


dirname = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

API_URL = config["API"]["API_URL"]
DATA_PATH = config["Files"]["DATA_PATH"]
LOG_PATH = config["Files"]["LOG_PATH"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=LOG_PATH,
    filemode="a"
)

def get_data_for_date(date: datetime) -> None:
    params = {
        "date": date.strftime("%Y-%m-%d")  # Форматируем дату в строку для запроса
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

today = datetime.now()
yesterday = today - timedelta(days=1)

get_data_for_date(yesterday)
