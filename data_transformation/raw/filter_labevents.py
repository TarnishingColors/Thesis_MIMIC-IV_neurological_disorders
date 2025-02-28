"""Module to create a filtered raw table of chartevents"""
import concurrent
import configparser
import pandas as pd
from sqlalchemy import text
from ..data_transfer.utils import ConnectionDetails, DataTransfer, filter_matching_rows


# pylint: disable=duplicate-code

def process_chunk(chunk, i, result):
    """
    :param chunk: chunk size
    :param i: is used to track chunk id
    :param result: result of a fetch query with relevant patients
    """
    print(f"Processing chunk {i}")

    config = configparser.ConfigParser()
    config.read('config.ini')

    db_config = [x[1] for x in config.items('database')]

    dt = DataTransfer(ConnectionDetails(*db_config))

    matching_rows = filter_matching_rows(chunk, ['subject_id', 'hadm_id'], result)

    dt.read_data(matching_rows)
    dt.transfer_data('raw', 'labevents')

    dt.get_connection().close()


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('config.ini')

    file_folder = config['raw_data']['file_folder']

    db_config = [x[1] for x in config.items('database')]

    dt = DataTransfer(ConnectionDetails(*db_config))

    conn = dt.get_connection()

    filter_query = text("""
    SELECT p.subject_id, a.hadm_id
    FROM raw.patients p
    JOIN raw.admissions a ON p.subject_id = a.subject_id
    JOIN raw.diagnoses_icd d ON p.subject_id = d.subject_id AND a.hadm_id = d.hadm_id
    WHERE d.icd_version = 10
    AND (
        d.icd_code LIKE 'I61%' OR
        d.icd_code LIKE 'I63%' OR
        d.icd_code LIKE 'G41%'
    );
    """)

    result = set(conn.execute(filter_query).fetchall())

    df = pd.read_csv(f'{file_folder}/hosp/labevents.csv', chunksize=1000000)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_chunk, chunk, i, result)
            for i, chunk in enumerate(df)
        ]

        # Ensure all tasks complete
        concurrent.futures.wait(futures)

    conn.close()
