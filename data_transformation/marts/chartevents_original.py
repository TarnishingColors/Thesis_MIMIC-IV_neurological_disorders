import configparser
from ..data_transfer.utils import Connection, DataTransfer

config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']

dt = DataTransfer(Connection(*(x[1] for x in config.items('database'))))

dt.run_query(
    """
    CREATE TABLE IF NOT EXISTS mart.chartevents_original
    (LIKE ods.chartevents_grouped INCLUDING ALL);
    ALTER TABLE mart.chartevents_original
    ADD COLUMN IF NOT EXISTS previous_charttime TIMESTAMP;
    ALTER TABLE mart.chartevents_original
    ADD COLUMN IF NOT EXISTS minutes_since_last_test INT;
    
    INSERT INTO mart.chartevents_original
    SELECT *
        , LAG(charttime) OVER (PARTITION BY hadm_id ORDER BY charttime) AS previous_charttime
        , EXTRACT(MINUTES FROM charttime - LAG(charttime) OVER (PARTITION BY hadm_id ORDER BY charttime)) AS minutes_since_last_test
    FROM ods.chartevents_grouped c;
    """
)
