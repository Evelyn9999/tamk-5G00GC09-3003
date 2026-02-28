"""Pipeline configuration."""

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

DATABASE_URL = f"sqlite:///{DATA_DIR / 'jobs.db'}"

SESSION_FILE = BASE_DIR / "linkedin_session.json"
CSV_FILE = DATA_DIR / "my_saved_jobs.csv"
REPORT_DIR = DATA_DIR / "reports"
REPORT_DIR.mkdir(exist_ok=True)

SEARCHES = [
    {"keywords": "software engineer intern", "location": "Finland"},
    {"keywords": "software developer internship", "location": "Finland"},
    {"keywords": "software engineering", "location": "Tampere"},
    {"keywords": "software intern", "location": "Helsinki"},
    {"keywords": "software developer junior", "location": "Finland"},
]

TIME_FILTER = "week"
LIMIT_PER_SEARCH = 25
DELAY_BETWEEN_JOBS = 3
MAX_RETRIES = 3
