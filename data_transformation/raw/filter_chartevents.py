import pandas as pd
import configparser
from ..data_transfer.utils import Connection, DataTransfer, filter_matching_rows
from sqlalchemy import text


config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']

dt = DataTransfer(Connection(*(x[1] for x in config.items('database'))))

with dt.target_connection.connect() as conn:
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

    df = pd.read_csv(f'{file_folder}/icu/chartevents.csv', chunksize=1000000)

    for i, chunk in enumerate(df):
        matching_rows = filter_matching_rows(chunk, ['subject_id', 'hadm_id'], result)

        dt.read_data(matching_rows)
        dt.transfer_data('raw', 'chartevents')
