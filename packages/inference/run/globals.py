import logging

# create a logger for the inference workflow
logger = logging.getLogger("inference_workflow")
logger.setLevel(logging.INFO)

# set constants used throughtout the inference workflow
DO_SPACE_URL = "https://taimaka-health-predictions-storage.lon1.digitaloceanspaces.com"
DO_SPACE_PREFIX = "inference"
DO_DIRECTORY = f"{DO_SPACE_URL}/{DO_SPACE_PREFIX}"