import configparser
from ..data_transfer.utils import Connection, DataTransfer

config = configparser.ConfigParser()
config.read('config.ini')

file_folder = config['raw_data']['file_folder']

dt = DataTransfer(Connection(*(x[1] for x in config.items('database'))))

dt.run_query(
    """
    
    """
)
