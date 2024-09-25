import psycopg2
import pandas as pd
import configparser


config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']

conn = psycopg2.connect(
    dbname   = config['database']['dbname'],
    user     = config['database']['user'],
    password = config['database']['password'],
    host     = config['database']['host'],
    port     = config['database']['port']
)

cur = conn.cursor()

sql_query = """
SELECT p.subject_id, a.hadm_id
FROM mimic_data.patients p
JOIN mimic_data.diagnoses_icd d ON p.subject_id = d.subject_id
JOIN mimic_data.admissions a ON p.subject_id = a.subject_id
WHERE d.icd_version = 10
AND (
    d.icd_code LIKE 'I61%' OR
    d.icd_code LIKE 'I63%' OR
    d.icd_code LIKE 'G41%'
);
"""

cur.execute(sql_query)

results = set(cur.fetchall())


def filter_matching_rows(df_partition):
    return df_partition[df_partition.apply(lambda row: (row['subject_id'], row['hadm_id']) in results, axis=1)]


df = pd.read_csv(f'{file_folder}/hosp/labevents.csv', chunksize=1000000)

matching_rows = df.map_partitions(filter_matching_rows)

matching_rows.to_csv('processed_data/prescriptions.csv', single_file=True, header=True)

cur.close()
conn.close()
