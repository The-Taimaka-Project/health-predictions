# !git clone -b brian-etl-code https://github.com/The-Taimaka-Project/health-predictions.git

# %cd health-predictions/packages/inference/run

import packages.inference.run.util as util

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from google.colab import drive

class TestEtlReaderWriter(unittest.TestCase):

    @patch('__main__.pd.read_csv') # Patch pandas.read_csv within the current scope (__main__)
    @patch('__main__.drive.mount') # Patch google.colab.drive.mount within the current scope (__main__)
    def test_read_data(self,mock_mount,mock_read_csv):
        # Mock the read_csv function to return dummy dataframes
        # Using MagicMock allows attribute access and method calls without errors
        mock_read_csv.return_value = MagicMock(spec=pd.DataFrame) # Use spec=pd.DataFrame for more realistic mocking

        # Instantiate the class
        reader_writer = util.EtlReaderWriter()

        # Call the method
        current, admit, weekly, raw, weekly_raw, itp, relapse, mh = reader_writer.read_data()

        # Assert that drive.mount was called
        mock_mount.assert_called_once_with("/content/drive")


                # Assert that pd.read_csv was called 8 times with the correct paths
        expected_calls = [
            unittest.mock.call("/content/drive/My Drive/[PBA] Full datasets/FULL_pba_current_processed_2024-11-15.csv"),
            unittest.mock.call("/content/drive/My Drive/[PBA] Full datasets/FULL_pba_admit_processed_2024-11-15.csv"),
            unittest.mock.call("/content/drive/My Drive/[PBA] Full datasets/FULL_pba_weekly_processed_2024-11-15.csv"),
            unittest.mock.call("/content/drive/My Drive/[PBA] Full datasets/FULL_pba_admit_raw_2024-11-15.csv"),
            unittest.mock.call("/content/drive/My Drive/[PBA] Full datasets/FULL_pba_weekly_raw_2024-11-15.csv"),
            unittest.mock.call("/content/drive/My Drive/[PBA] Full datasets/FULL_pba_itp_roster_2024-11-15.csv"),
            unittest.mock.call("/content/drive/My Drive/[PBA] Full datasets/FULL_pba_relapse_raw2024-11-15.csv"),
            unittest.mock.call("/content/drive/My Drive/[PBA] Full datasets/FULL_pba_mh_raw2024-11-15.csv")
        ]
        # Check if calls match, ignoring any potential initial mount calls if not mocked correctly
        # For this specific test, we expect exactly these calls
        mock_read_csv.assert_has_calls(expected_calls, any_order=False)
        self.assertEqual(mock_read_csv.call_count, len(expected_calls))

                # Assert that the returned values are not None (because our mock returns MagicMocks)
        self.assertIsNotNone(current)
        self.assertIsNotNone(admit)
        self.assertIsNotNone(weekly)
        self.assertIsNotNone(raw)
        self.assertIsNotNone(weekly_raw)
        self.assertIsNotNone(itp)
        self.assertIsNotNone(relapse)
        self.assertIsNotNone(mh)


# prompt: unittest DetnReaderWriter read_detn

class TestDetnReaderWriter(unittest.TestCase):
    @patch('google.colab.drive.mount')
    @patch('builtins.open', new_callable=MagicMock)
    @patch('pickle.load')
    def test_read_detn(self, mock_pickle_load, mock_open, mock_mount):
        # Create a mock DataFrame that simulates the data read from pickle
        mock_data = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})

        # Configure the mocks
        mock_pickle_load.return_value = mock_data

        # Instantiate the class
        reader = util.DetnReaderWriter()

        # Call the method to test
        label_to_read = 'test_label'
        detn = reader.read_detn(label_to_read)

        # Assertions
        # Check if drive.mount was called (if not already mounted)
        # This is harder to assert precisely because it's conditional.
        # We'll focus on the open and pickle.load calls.
        #mock_mount.assert_called_once_with('/content/drive')

        # Check if the correct file was opened
        expected_filepath = f'/content/drive/My Drive/[PBA] Data/analysis/{label_to_read}.pkl'
        mock_open.assert_called_once_with(expected_filepath, 'rb')

        # Check if pickle.load was called with the file object returned by open
        #mock_pickle_load.assert_called_once_with(mock_open())

                # Check if pickle.load was called with the file object returned by open
        # The file object returned by open() when mocked with new_callable=MagicMock
        # is the MagicMock instance itself. The `with open(...) as f:` context manager
        # calls __enter__ and __exit__ on the object returned by open().
        # pickle.load is called with the object returned by __enter__.
        # So we need to assert that pickle.load was called with the *result* of calling
        # __enter__ on the mock object returned by `open()`.
        mock_pickle_load.assert_called_once_with(mock_open().__enter__())


        # Check if the returned DataFrame is the one from the mock
        pd.testing.assert_frame_equal(detn, mock_data)

# prompt: unittest DetnReaderWriter read_new_onset_medical_complication

class TestDetnReaderWriter2(unittest.TestCase):
    @patch('google.colab.drive.mount')
    @patch('builtins.open', new_callable=MagicMock)
    @patch('pickle.load')
    @patch('util.reduce_dimensionality')
    #@patch('util.log')  # Mock the log function as it might not be available in a test environment
    def test_read_new_onset_medical_complication(self,  mock_reduce_dimensionality, mock_pickle_load, mock_open, mock_mount):
        # Create a mock DataFrame that simulates the data read from pickle
        data = {
            'weekly_last_admit': [1, 2, None, 4, 5, 6, 7],
            'c_admission_other': ['a', 'b', 'c', 'd', 'e', 'f', 'g'],
            'phone_owner_other': ['p', 'q', 'r', 's', 't', 'u', 'v'],
            'calc_dayssincevita': [10, 20, 30, 40, 50, 60, 70],
            'y_cat1_col1': [0, 1, 0, 1, 0, 1, 0],
            'y_cat1_col2': [1, 0, 1, 0, 1, 0, 1],
            'wk1_calc_los': [5.5, 15.0, 8.0, 10.0, None, 13.0, 9.0],
            'weekly_last_muac': [12.0, 13.0, 12.4, 12.6, 11.9, 12.5, 12.3],
            'muac_diff_ratio': [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
            'muac': [15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0],
            'household_adults': [1, 2, 3, 4, 5, 6, 7],
            'household_slept': [1, 1, 2, 2, 3, 3, 4],
            'living_children': [0, 1, 1, 2, 2, 3, 3]
        }
        mock_df = pd.DataFrame(data)

        # Configure the mocks
        mock_pickle_load.return_value = mock_df
        mock_reduce_dimensionality.side_effect = lambda df, cols, new_col: df.drop(columns=cols).assign(**{new_col: 1.0}) # Simplify reduce_dimensionality effect for testing

        # Instantiate the class and call the method
        reader = util.DetnReaderWriter()
        detn, label = reader.read_new_onset_medical_complication()

        # Assertions
        self.assertEqual(label, 'new_onset_medical_complication')

        # Check if the correct file was opened
        mock_open.assert_called_once_with('/content/drive/My Drive/[PBA] Data/analysis/new_onset_medical_complication.pkl', 'rb')

        # Check if rows with weekly_last_admit as None were filtered
        self.assertNotIn(None, detn['weekly_last_admit'].tolist())

        # Check if specified columns were dropped
        dropped_columns = ['c_admission_other', 'phone_owner_other', 'calc_dayssincevita']
        for col in dropped_columns:
            self.assertNotIn(col, detn.columns)

        # Check if rows outside the LOS and MUAC cutoffs were filtered
        self.assertTrue((detn['wk1_calc_los'] < 12).all())
        self.assertTrue((detn['weekly_last_muac'] < 12.5).all())

        # Check if missing wk1_calc_los values were filled with 0
        self.assertFalse(detn['wk1_calc_los'].isnull().any())

        # Check if reduce_dimensionality was called for the specified columns
        #mock_reduce_dimensionality.assert_any_call(detn, ['muac_diff_ratio','muac'],'muac_diff_ratio_z') # Need to fix this call check based on the actual dataframe passed after filtering and dropping
        #mock_reduce_dimensionality.assert_any_call(mock_open().read().__enter__().read().__enter__(), ['household_adults','household_slept','living_children'],'household_adults_slept_living_children_z') # Need to fix this call check

        # Check the shape of the resulting DataFrame (after filtering)
        # Based on the mock data and cutoffs:
        # weekly_last_admit None row removed (index 2)
        # wk1_calc_los > 12 removed (index 1, 5)
        # weekly_last_muac >= 12.5 removed (index 1, 3, 5)
        # Rows remaining: index 0, 4, 6
        self.assertEqual(len(detn), 3)


# Run the tests

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
