import os
from pathlib import Path
from dotenv import load_dotenv
import yaml

# Load .env file once
load_dotenv()

# Define project settings
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# --- Google --- 
GOOGLE_MAP_API_KEY = os.getenv("GOOGLE_MAP_API_KEY")
GOOGLE_GEMINI_API_KEY = os.getenv("GOOGLE_GEMINI_API_KEY")
GOOGLE_PIC_SAVING_DIR = os.getenv(
    "GOOGLE_PIC_SAVING_DIR",
    os.path.join(PROJECT_ROOT, "data", "Google_route")
)

# --- Bright Data ---
ZILLOW_DATASET_ID = os.getenv("ZILLOW_DATASET_ID")
BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY")

BRIGHTDATA_CONFIG_PATH = os.getenv("BRIGHTDATA_CONFIG_PATH", os.path.join(PROJECT_ROOT, "configs", "brightdata_config.yaml"))
BRIGHTDATA_SNAPSHOT_SAVING_PATH = os.getenv("BRIGHTDATA_SNAPSHOT_SAVING_PATH", os.path.join(PROJECT_ROOT, "data"))
BRIGHTDATA_LISTING_SNAPSHOT_KEYS = []
BRIGHTDATA_LISTING_SNAPSHOT_DESCP = {}

cfg_path = Path(BRIGHTDATA_CONFIG_PATH)

with cfg_path.open("r", encoding="utf-8") as f:
    _conf = yaml.safe_load(f) or {}
    BRIGHTDATA_LISTING_SNAPSHOT_KEYS = _conf.get("BRIGHTDATA_LISTING_SNAPSHOT_KEYS", [])
    BRIGHTDATA_LISTING_SNAPSHOT_DESCP = _conf.get("BRIGHTDATA_LISTING_SNAPSHOT_DESCP", {})

# --- PostgreSQL ---
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/appdb"
)