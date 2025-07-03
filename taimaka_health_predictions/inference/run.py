"""
Entry point script for the server.

When built, the main function here becomes the `infer` CLI command that is run by the server.

TODO: covert all code in `etl.py`, `etl_deterioration.py`, and `infer.py` to functions; add
a main function to each of those scripts; and call those functions here.
"""

import subprocess
from pathlib import Path

from taimaka_health_predictions.utils.globals import logger

path = Path(__file__).parent


def main():
    try:
        subprocess.call(f"python {str(path / 'etl.py')}", shell=True)
        subprocess.call(f"python {str(path / 'etl_deterioration.py')}", shell=True)
        subprocess.call(f"python {str(path / 'infer.py')}", shell=True)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
