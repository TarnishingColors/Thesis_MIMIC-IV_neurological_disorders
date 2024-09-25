import dask.dataframe as dd
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']
# Read the CSV with Dask
df = dd.read_csv(f'{file_folder}/hosp/prescriptions.csv', dtype={
    'dose_val_rx': 'object',
    'form_rx': 'object',
    'form_val_disp': 'object',
    'gsn': 'object',
    'ndc': 'float64',
    'poe_seq': 'float64'
})

results = [(10000032, 22595853)]


def filter_matching_rows(df_partition):
    return df_partition[df_partition.apply(lambda row: (row['subject_id'], row['hadm_id']) in results, axis=1)]


df = df.map_partitions(filter_matching_rows)

# Get the first 100 rows
first_100_rows = df.head(100)

# Convert to Pandas dataframe or process as needed
print(first_100_rows)