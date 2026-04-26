import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
DB_DIR = DATA_DIR / "db"
DB_PATH = DB_DIR / "openfda.duckdb"

OPENFDA_API_KEY: str = os.getenv("OPENFDA_API_KEY", "")
OPENFDA_BASE_URL = "https://api.fda.gov"

REQUESTS_PER_MINUTE = 200
MAX_RECORDS_PER_CHUNK = 25_000  # openFDA hard limit with skip pagination
DEFAULT_YEARS_BACK = 3
