"""Module to create valueuom dictionary"""

import configparser
from ..data_transfer.utils import ConnectionDetails, DataTransfer

config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']

dt = DataTransfer(ConnectionDetails(*(x[1] for x in config.items('database'))))

dt.run_query(
    """
    CREATE VIEW mart.d_valueuom AS (
        SELECT DISTINCT l.valueuom
            , dl.label
        FROM raw.d_labitems dl
        JOIN raw.labevents l
        ON dl.itemid = l.itemid
    )
    """
)
