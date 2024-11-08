import configparser
import os


config = configparser.ConfigParser()
dirname = os.path.dirname(__file__)
config.read(os.path.join(dirname, "config.ini"))

API_URL = config["API"]["API_URL"]
DATABASE_CREDS = config["Database"]
LOG_PATH = config["Files"]["LOG_PATH"]
DATA_PATH = config["Files"]["DATA_PATH"]