"""
Author: Hunter Merrill

Description: This script provides a class for reading and writing data to DigitalOcean Spaces.
It expects the DigitalOcean credentials to be set in the environment variables
'TAIMAKA_DO_ACCESS_KEY' and 'TAIMAKA_DO_SECRET_KEY'.

Example usage:

    from digitalocean import DigitalOceanStorage

    # initialize the class
    do_storage = DigitalOceanStorage()

    # create an example DataFrame
    import pandas as pd
    df = pd.DataFrame({"col1": [0, 1, 1], "col2": [3, 4, 5]})

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

    # and you can save and load AutoGluon models, with custom metadata
    from autogluon.tabular import TabularPredictor

    predictor = TabularPredictor(label="col1")
    predictor.fit(df)
    do_storage.to_autogluon_tarball(
        predictor, model_metadata={"something": "anything"}, path="path/to/model.tar.gz"
    )
    loaded_model, metadata = do_storage.read_autogluon_tarball(path="path/to/model.tar.gz")
"""

import json
import os
import pickle
import tarfile
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Tuple, Optional

import boto3
import pandas as pd
from autogluon.tabular import TabularPredictor

# Importing the default URL from globals.py
from taimaka_health_predictions.utils.globals import DO_SPACE_URL


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

    def to_autogluon_tarball(
        self,
        predictor: TabularPredictor,
        path: str,
        bucket: str = "inference-workflow",
        model_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Save an AutoGluon TabularPredictor to DigitalOcean Spaces as a TAR file.

        Parameters
        ----------
        predictor: TabularPredictor
            the AutoGluon TabularPredictor you want to save to DO.
        path: str
            the path to save the file, e.g., "path/to/file.tar.gz".
        bucket: str
            an optional prefix for the filepath. The default is "inference-workflow".
        model_metadata: Dict[str, Any] | None
            optional metadata to include in the saved model. If provided, it will be saved as a
            JSON file alongside the model files. This is useful for versioning, e.g.:

            model_metadata = {
                "version": "0.1.0",
                "inputs": [<list_of_input_features>],
                "outputs": [<list_of_output_features>],
                "description": (
                    "Predicts chance of health complication given 2 weeks of patient metrics."
                ),
                "feature_engineering": (
                    "<feature_1> is normalized, <feature_2> is one-hot encoded, etc.",
                ),
                "contact": "Brain Chaplin",
            }
        """
        with TemporaryDirectory() as tempdir:
            tempdir_path = Path(tempdir)

            # save the predictor if it hasn't been saved already
            predictor.save()

            # list the files we want to save
            files_to_save = [str(Path(predictor.path) / fn) for fn in os.listdir(predictor.path)]

            # save the model metadata if provided
            if model_metadata is not None:
                metadata_path = tempdir_path / "model_metadata.json"
                with open(str(metadata_path), "w") as json_file:
                    json.dump(model_metadata, json_file)

                # add the metadata file to the list of files to save
                files_to_save.append(str(metadata_path))

            # zip up the contents of the temporary directory
            with tarfile.open(str(tempdir_path / "model.tar.gz"), "w:gz") as tar:
                for file in files_to_save:
                    tar.add(file, arcname=file.split("/")[-1], recursive=True)

            # upload the tar file to DigitalOcean Spaces
            self.client.upload_file(
                Bucket=bucket, Key=path, Filename=str(tempdir_path / "model.tar.gz")
            )

    def read_autogluon_tarball(
        self, path: str, bucket: str = "inference-workflow", local_path: Optional[str] = None
    ) -> Tuple[TabularPredictor, Dict[str, Any]]:
        """
        Read an AutoGluon TabularPredictor from DigitalOcean Spaces.

        Parameters
        ----------
        path: str
            the path to read, e.g., "path/to/file.tar.gz".
        bucket: str
            an optional prefix for the filepath. The default is "inference-workflow".
        local_path: str | None
            the local path where the TAR file will be downloaded. Defaults to {working_directory}/model.

        Returns
        -------
        Tuple[TabularPredictor, Dict[str, Any]]
            A tuple containing:
            - predictor: TabularPredictor
                the AutoGluon TabularPredictor read from the TAR file in DigitalOcean Spaces.
            - metadata: Dict[str, Any]
                the metadata read from the JSON file in the TAR file, if it exists.
        """
        # create local directory if not provided
        if local_path is None:
            local_path = "model"

        # download the tar file from DigitalOcean Spaces
        with TemporaryDirectory() as tempdir:
            filename = str(Path(tempdir) / "model.tar.gz")
            self.client.download_file(Bucket=bucket, Key=path, Filename=filename)

            # extract the contents of the tar file
            with tarfile.open(filename, "r:gz") as tar:
                tar.extractall(path=local_path)

        # load the predictor from the extracted files
        predictor = TabularPredictor.load(local_path)

        # if metadata was saved, load it
        metadata = {}
        metadata_path = Path(local_path) / "model_metadata.json"
        if metadata_path.exists():
            with open(metadata_path, "r") as json_file:
                metadata = json.load(json_file)

        return predictor, metadata
