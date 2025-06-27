"""
Just testing that a DO Function can load an Autogluon model from DO Spaces.
"""

import argparse

from taimaka_health_predictions.utils.digitalocean import DigitalOceanStorage
from taimaka_health_predictions.utils.globals import DO_SPACE_URL, logger

MODEL_PATH = (
    f"{DO_SPACE_URL}/inference-workflow/model/new_onset_medical_complicationnot1/0.1.0/model.tar.gz"
)
DATA_PATH = "etl/new_onset_medical_complication.pkl"


def run_inference():
    storage = DigitalOceanStorage()

    # load the model
    predictor, metadata = storage.read_autogluon_tarball(MODEL_PATH)
    logger.info("Model successfully loaded.")

    # load inference data and make predictions
    input_data = storage.read_pickle(DATA_PATH)
    logger.info("Data successfully loaded.")

    predictions = predictor.predict_proba(input_data[predictor.features()])
    logger.info("Predictions successfully made.")

    return predictions


def main():
    parser = argparse.ArgumentParser(
        description="Inference function for Taimaka Health Predictions."
    )
    parser.add_argument("--name", type=str, default="stranger", help="Name of the person to greet.")
    args = vars(parser.parse_args())

    name = args.get("name", "stranger")
    logger.info(f"Hi, {name}. Starting inference pipeline.")

    try:
        predictions = run_inference()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
