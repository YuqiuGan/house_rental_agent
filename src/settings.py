import os
from dotenv import load_dotenv

# Load .env file once
load_dotenv()

# Define project settings
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GOOGLE_MAP_API_KEY = os.getenv("GOOGLE_MAP_API_KEY")
GOOGLE_GEMINI_API_KEY = os.getenv("GOOGLE_GEMINI_API_KEY")

GOOGLE_PIC_SAVING_DIR = os.getenv(
    "GOOGLE_PIC_SAVING_DIR",
    os.path.join(PROJECT_ROOT, "data", "Google_route")
)