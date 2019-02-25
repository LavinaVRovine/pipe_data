import logging
import os

PIPE_TOKEN = os.getenv("PIPE_TOKEN", "supersweettoken")
PIPE_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
BASE_URL = "https://api.pipedrive.com/v1/"
RESPONSE_LIMIT = 250

DB_HOST = os.getenv("DB_HOST", 'localhost')
PIPE_DATABASE = os.getenv("PIPE_DATABASE", 'pipedrive')
USER = os.getenv("USER", 'postgres')
DB_PASSWORD = os.getenv("DB_PASSWORD", 'supersweet password')
DATABASE_URI = f"postgresql+psycopg2://{USER}:{DB_PASSWORD}@{DB_HOST}:5432/{PIPE_DATABASE}"

# logging.basicConfig(level=logging.DEBUG)
MY_LOGGER = logging.getLogger(__name__)

# c_handler = logging.StreamHandler()
# c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# c_handler.setFormatter(c_format)
# MY_LOGGER.addHandler(c_handler)

file_handler = logging.FileHandler("log.log")
file_format = logging.Formatter('[%(asctime)s] --- %(levelname)s - %(message)s')
file_handler.setFormatter(file_format)
MY_LOGGER.addHandler(file_handler)
