"""
Just testing that a DO Function can load an Autogluon model from DO Spaces.
"""

from digitalocean import DigitalOceanStorage
from globals import logger

MODEL_PATH = (
    "https://taimaka-health-predictions-storage.lon1.digitaloceanspaces.com/"
    "inference-workflow/model/new_onset_medical_complicationnot1/0.1.0/model.tar.gz"
)


def main():
    logger.info("Starting inference function.")
    storage = DigitalOceanStorage()
    predictor = storage.read_autogluon_tarball(MODEL_PATH)
    logger.info("Model successfully loaded.")
    return {"body": "Model loaded successfully."}
