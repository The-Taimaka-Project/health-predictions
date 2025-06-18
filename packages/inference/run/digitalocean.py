"""
Author: Hunter Merrill

Description: This script provides a class for reading and writing data to DigitalOcean Spaces.
It expects the DigitalOcean credentials to be set in the environment variables
'TAIMAKA_DO_ACCESS_KEY' and 'TAIMAKA_DO_SECRET_KEY'.

Example usage:

    import pandas as pd
    from digitalocean import DigitalOceanStorage

    # initialize the class
    do_storage = DigitalOceanStorage()

    # create an example DataFrame
    df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})

    # save the DataFrame to DigitalOcean Spaces
    do_storage.to_csv(df, "path/to/file.csv")

    # read the DataFrame back from DigitalOcean Spaces
    df_read = do_storage.read_csv("path/to/file.csv")

    # you can pickle it too
    do_storage.to_pickle(df, "path/to/file.pkl")
    df_read_pickle = do_storage.read_pickle("path/to/file.pkl")

    # how about jsons
    my_dict = {"a": 1, "b": 2}
    do_storage.to_json(my_dict, "path/to/file.json")
    read_my_dict = do_storage.read_json("path/to/file.json")

To-do: add methods for storing and loading AutoGluon models as zipfiles.
"""

import json
import os
import pickle
from io import BytesIO, StringIO
from typing import Any, Dict

import boto3
import pandas as pd

# Importing the default URL from globals.py
from globals import DO_SPACE_URL


class DigitalOceanStorage:
    """Class for reading/writing to/from DigitalOcean Spaces."""

    def __init__(self, endpoint_url: str = DO_SPACE_URL) -> None:
        """
        Parameters
        ----------
        endpoint_url: str
            The URL of the DigitalOcean Space.
        """
        do_key = os.environ.get("TAIMAKA_DO_ACCESS_KEY")
        do_secret = os.environ.get("TAIMAKA_DO_SECRET_KEY")

        # check if we got the credentials; if not, raise an error
        if not do_key or not do_secret:
            raise KeyError(
                "Your DigitalOcean credentials could not be found in the environment variables "
                "'TAIMAKA_DO_ACCESS_KEY', 'TAIMAKA_DO_SECRET_KEY'."
            )

        # Initialize a session using DigitalOcean Spaces
        session = boto3.session.Session()
        self.client = session.client(
            "s3",
            region_name=endpoint_url.split(".digitalocean")[0].split(".")[-1],
            endpoint_url=endpoint_url,
            aws_access_key_id=do_key,
            aws_secret_access_key=do_secret,
        )

    def to_csv(self, df: pd.DataFrame, path: str, bucket: str = "inference-workflow") -> None:
        """
        Save a pandas dataframe to DigitalOcean Spaces.

        Parameters
        ----------
        df: pd.DataFrame
            the dataframe you want to save to DO.
        path: str
            the path to save the file, e.g., "path/to/file.csv".
        bucket: str
            an optional prefix for the filepath. The default is "inference-workflow".
        """
        # save dataframe to buffer then push buffer to DO
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        response = self.client.put_object(Bucket=bucket, Key=path, Body=buffer.getvalue())

        # check if the response was successful
        if response["ResponseMetadata"]["HTTPStatusCode"] >= 400:
            raise ValueError(f"An error occurred: {response.get('Error')}")

    def read_csv(self, path: str, bucket: str = "inference-workflow") -> pd.DataFrame:
        """
        Read a CSV from DigitalOcean Spaces.

        Parameters
        ----------
        path: str
            the path to read, e.g., "path/to/file.csv".
        bucket: str
            an optional prefix for the filepath. The default is "inference-workflow".

        Returns
        -------
        pd.DataFrame
            the dataframe read from the CSV file in DigitalOcean Spaces.
        """
        response = self.client.get_object(Bucket=bucket, Key=path)
        return pd.read_csv(BytesIO(response["Body"].read()))

    def to_pickle(self, object: Any, path: str, bucket: str = "inference-workflow") -> None:
        """
        Save an object to DigitalOcean Spaces as a pickle.

        Parameters
        ----------
        object: Any
            the object you want to save to DO.
        path: str
            the path to save the file, e.g., "path/to/file.pkl".
        bucket: str
            an optional prefix for the filepath. The default is "inference-workflow".
        """
        pickle_bytes = pickle.dumps(object)
        response = self.client.put_object(Bucket=bucket, Key=path, Body=pickle_bytes)

        # check if the response was successful
        if response["ResponseMetadata"]["HTTPStatusCode"] >= 400:
            raise ValueError(f"An error occurred: {response.get('Error')}")

    def read_pickle(self, path: str, bucket: str = "inference-workflow") -> Any:
        """
        Read a pickle from DigitalOcean Spaces.

        Parameters
        ----------
        path: str
            the path to read, e.g., "path/to/file.pkl".
        bucket: str
            an optional prefix for the filepath. The default is "inference-workflow".

        Returns
        -------
        Any
            the object read from the pickle file in DigitalOcean Spaces.
        """
        response = self.client.get_object(Bucket=bucket, Key=path)
        return pickle.loads(response["Body"].read())

    def to_json(
        self, object: Dict[str, Any], path: str, bucket: str = "inference-workflow"
    ) -> None:
        """
        Save a dictionary to DigitalOcean Spaces as a JSON file.

        Parameters
        ----------
        object: Dict[str, Any]
            the dictionary you want to save to DO.
        path: str
            the path to save the file, e.g., "path/to/file.json".
        bucket: str
            an optional prefix for the filepath. The default is "inference-workflow".
        """
        json_bytes = json.dumps(object).encode("utf-8")
        response = self.client.put_object(Bucket=bucket, Key=path, Body=json_bytes)

        # check if the response was successful
        if response["ResponseMetadata"]["HTTPStatusCode"] >= 400:
            raise ValueError(f"An error occurred: {response.get('Error')}")

    def read_json(self, path: str, bucket: str = "inference-workflow") -> Dict[str, Any]:
        """
        Read a JSON file from DigitalOcean Spaces.

        Parameters
        ----------
        path: str
            the path to read, e.g., "path/to/file.json".
        bucket: str
            an optional prefix for the filepath. The default is "inference-workflow".

        Returns
        -------
        Dict[str, Any]
            the dictionary read from the JSON file in DigitalOcean Spaces.
        """
        response = self.client.get_object(Bucket=bucket, Key=path)
        return json.loads(response["Body"].read().decode("utf-8"))
