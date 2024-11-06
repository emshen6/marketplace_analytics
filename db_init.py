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

def check_data_available(date):
    """Проверяет наличие данных для указанной даты."""
    params = {
        "date": date.strftime("%Y-%m-%d")
    }

    try:
        response = requests.get(API_URL, params=params)
        if response.status_code == 200 and response.json():
            return True
        else:
            return False
    except requests.exceptions.RequestException as e:
        logging.error(f"Connection error: {e}")
        return False

'''
def find_earliest_available_date():
    """Использует бинарный поиск для нахождения самой ранней доступной даты с данными."""

    end_date = pd.to_datetime("today")  # сегодня
    start_date = end_date - timedelta(days=365 * 5)  # пять лет назад, например

    while start_date < end_date:
        middle_date = start_date + (end_date - start_date) // 2

        if check_data_available(middle_date):
            end_date = middle_date  # данные есть, ищем в более ранних датах
        else:
            start_date = middle_date + timedelta(days=1)  # данных нет, ищем в более поздних датах

    logging.info(f"Earliest available data found on: {start_date}")
    return start_date
'''

date = pd.to_datetime(input())
print(check_data_available(date))


'''
params = {
    "date": earliest_date.strftime("%Y-%m-%d")
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
'''
