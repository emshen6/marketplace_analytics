from datetime import datetime, timedelta
from db_utils import load_data_from_api, insert_data_to_db
from logger import logger


def fill_database_for_day(date: datetime) -> None:
    data = load_data_from_api(date)

    if data is not None:
        insert_data_to_db(data)
        logger.info(f"Data for {date.strftime('%Y-%m-%d')} successfully inserted into database.")
    else:
        logger.warning(f"No data available for {date.strftime('%Y-%m-%d')}.")


if __name__ == "__main__":
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    fill_database_for_day(yesterday)
