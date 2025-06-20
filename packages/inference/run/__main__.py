"""
Just testing that a DO Function can load an Autogluon model from DO Spaces.
"""

from digitalocean import DigitalOceanStorage
from globals import logger

MODEL_PATH = (
    "https://taimaka-health-predictions-storage.lon1.digitaloceanspaces.com/"
    "inference-workflow/model/new_onset_medical_complicationnot1/0.1.0/model.tar.gz"
)


def main(args):
    try:
        name = args.get("name", "stranger")
        logger.info("Starting inference function.")
        storage = DigitalOceanStorage()
        predictor = storage.read_autogluon_tarball(MODEL_PATH)
        logger.info("Model successfully loaded.")
        return {"body": f"Hi, {name}. Model loaded successfully."}
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {"body": f"An error occurred: {e}"}
