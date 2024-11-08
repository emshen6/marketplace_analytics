import pandas as pd
from datetime import timedelta
from tqdm import tqdm
from db_utils import check_data_available, insert_data_to_db
from logger import logger


def find_earliest_available_date() -> pd.Timestamp:
    end_date = pd.to_datetime("today").normalize()
    start_date = (end_date - timedelta(days=365 * 5)).normalize()

    while start_date < end_date:
        middle_date = start_date + (end_date - start_date) // 2
        middle_date = middle_date.normalize()

        if check_data_available(middle_date):
            end_date = middle_date
        else:
            start_date = middle_date + timedelta(days=1)

    logger.info(f"Earliest available data found on: {start_date}")
    return start_date


def fill_database(date: pd.Timestamp) -> None:

    from db_utils import load_data_from_api

    data = load_data_from_api(date)

    if data is not None:
        insert_data_to_db(data)
        logger.info(f"Data for {date.strftime('%Y-%m-%d')} successfully inserted into database.")
    else:
        logger.warning(f"No data available for {date.strftime('%Y-%m-%d')}.")


if __name__ == "__main__":
    earliest_date = find_earliest_available_date()
    today = pd.to_datetime("today").normalize()

    for single_date in tqdm(pd.date_range(earliest_date, today)):
        fill_database(single_date)

    logger.info("Database population completed.")