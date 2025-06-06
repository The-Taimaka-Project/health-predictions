"""
This is the entry point for the inference function. The DigitalOcean Function will call
this function when it is invoked. (The schedule is set in the `project.yml` file.)
"""

from typing import Dict

# TODO: Import the necessary functions. E.g.,
#   from util import some_util_function
#   from etl import get_cleaned_data, create_weekly_data
#   from etl_deterioration import create_time_series_data
#   from inference import run_inference, save_predictions


def main() -> Dict[str, str]:
    """
    The main function that will be called by the DigitalOcean Function.

    Currently, it returns a simple greeting message.

    In the future, this function will serve as a wrapper around five other functions:
    - `get_cleaned_data`: to fetch cleaned data from the Postgres database.
    - `create_weekly_data`: to process the cleaned data into one row per patient-week.
    - `create_time_series_data`: to create time series data from the weekly data.
        (This is the model-ready data.)
    - `run_inference`: This function takes the time series data as an argument, loads
        a stored model object from DigitalOceans Spaces, runs the data through the model,
        and returns the predictions.
    - `save_predictions`: to save the predictions and SHAP values to the Postgres database.
    """

    # TODO: Call the necessary functions in the correct order. It will look something like this:
    # cleaned_data = get_cleaned_data()
    # weekly_data = create_weekly_data(cleaned_data)
    # time_series_data = create_time_series_data(weekly_data)
    # predictions, shap_values = run_inference(time_series_data)
    # save_predictions(predictions, shap_values)

    greeting = "Hello from the inference function"
    print(greeting)
    return {"body": greeting}
