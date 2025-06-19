# !git clone -b brian-etl-code https://github.com/The-Taimaka-Project/health-predictions.git

# %cd health-predictions/packages/inference/run

from packages.inference.run import util
import unittest
from unittest.mock import MagicMock
import numpy as np


# prompt: unittest calculate_aic mock model.predict to return an array

class TestAIC(unittest.TestCase):

    def test_calculate_aic_with_mocked_model_predict(self):
        # Example test case with mocking
        n = 100  # Number of observations
        k = 5    # Number of parameters

        # Create a mock object for a model
        mock_model = MagicMock()

        # Define the return value for the 'predict' method of the mock object
        # Mock predict to return an array of predictions.
        # We'll define a simple set of true values and let the predicted values lead to a known SSE.
        y_true = np.random.rand(n) # True values
        # Let's make the predicted values such that the sum of squared errors (SSE) is a known value.
        # SSE = sum((y_true - y_pred)^2)
        # Let's aim for a simple SSE value, e.g., SSE = 100.
        # This means sum((y_true - y_pred)^2) = 100
        # We can construct y_pred to satisfy this. A simple way is to have a fixed difference.
        # (y_true - y_pred)^2 = 100/n for all i, so y_true - y_pred = sqrt(100/n) or -sqrt(100/n)
        # Let's just add a constant error term for simplicity in the mock:
        y_pred_mock = y_true - np.sqrt(100/n) # Example where the error is constant

        mock_model.predict.return_value = y_pred_mock
        mock_model.feature_importances_ = np.ones(k) # Mock feature_importances_ to get k

        # Calculate the expected SSE based on the mocked predictions
        expected_sse = np.sum((y_true - y_pred_mock) ** 2)

        # Calculate the expected AIC based on the formula: AIC = 2*k - 2*log(SSE)
        expected_aic = 2 * k - 2 * np.log(expected_sse)

        # Call the function under test with the mock model and true values
        calculated_aic = calculate_aic(mock_model, np.zeros((n, k)), y_true) # X is not used in this simplified calculate_aic, pass a placeholder

        self.assertAlmostEqual(calculated_aic, expected_aic, places=5)


# If you run this separately or in a new Colab cell:
if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False) # Necessary for running in Colab
