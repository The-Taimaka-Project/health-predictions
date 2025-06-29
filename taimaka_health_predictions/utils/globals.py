import logging

# create a logger for the inference workflow
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
logger = logging.getLogger("inference workflow")

# set constants used throughtout the inference workflow
DO_SPACE_URL = "https://taimaka-health-predictions-storage.lon1.digitaloceanspaces.com"
DO_SPACE_PREFIX = "inference-workflow"
DO_DIRECTORY = f"{DO_SPACE_URL}/{DO_SPACE_PREFIX}"

ETL_DIR = 'etl/'
MODEL_DIR = 'model/'
ADMIT_ONLY = '1'
NOT_ADMIT_ONLY = 'not1'
