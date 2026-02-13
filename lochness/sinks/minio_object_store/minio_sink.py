"""
Implementation of a data sink for MinIO object storage.
"""
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime
from urllib.parse import urlparse

from minio import Minio

from lochness.helpers.timer import Timer
from lochness.models.data_push import DataPush
from lochness.models.keystore import KeyStore
from lochness.sinks.data_sink_i import DataSinkI
from lochness.models.logs import Logs
from lochness.helpers import hash as hash_helper

logger = logging.getLogger(__name__)


class MinioSink(DataSinkI):
    """
    A concrete implementation of a data sink for MinIO.

    This class implements the abstract methods defined in DataSinkI
    to push data to a MinIO server.
    """

    def push(
        self,
        file_to_push: Path,
        push_metadata: Dict[str, str],
        config_file: Path,
    ) -> DataPush:
        """
        Pushes data to the MinIO sink.

        Args:
            file_to_push (Path): The path to the file to be pushed.
            push_metadata (Dict[str, str]): Metadata associated with the push.
            data_sink (DataSink): The DataSink object containing MinIO configuration.
            config_file (str): Path to the configuration file.

        Returns:
            DataPush: An instance of DataPush if successful, None otherwise.
        """

        minio_metadata = self.data_sink.data_sink_metadata
        bucket_name: Optional[str] = minio_metadata.get("bucket_name")
        keystore_name: Optional[str] = minio_metadata.get("keystore_name")

        if not all([bucket_name, keystore_name]):
            logger.error(
                f"Missing MinIO configuration (bucket_name, or "
                f"keystore_name) for sink {self.data_sink.data_sink_name}"
            )
            logger.debug(
                f"Data sink metadata: {minio_metadata}"
            )
            raise ValueError(
                "Missing MinIO configuration in data sink metadata."
            )

        keystore_data = KeyStore.retrieve_keystore(
            key_name=keystore_name,  # type: ignore
            project_id=self.data_sink.project_id,
            config_file=config_file,
        )

        if not keystore_data:
            logger.error(
                f"Failed to retrieve keystore data for {keystore_name} "
                f"in project {self.data_sink.project_id}"
            )
            raise ValueError(
                "Keystore data not found for the specified keystore name."
            )

        logger.debug(
            f"Retrieved keystore data for {keystore_name} "
        )
        logger.debug(
            f"Keystore data: {keystore_data}"
        )

        keystore_value: Dict[str, Any] = json.loads(keystore_data.key_value)
        access_key: Optional[str] = keystore_value.get("access_key", None)
        secret_key: Optional[str] = keystore_value.get("secret_key", None)
        endpoint_url: Optional[str] = keystore_value.get("endpoint_url", None)

        if not all([access_key, secret_key, endpoint_url]):
            logger.error(
                "Missing MinIO credentials (access_key, secret_key, or endpoint_url) in "
                f"KeyStore for sink {self.data_sink.data_sink_name}"
            )
            raise ValueError(
                "Missing MinIO credentials in KeyStore."
            )

        bucket_name = minio_metadata.get("bucket_name", None)
        if not bucket_name:
            logger.error(
                f"Bucket name is not specified in the data sink "
                f"metadata for {self.data_sink.data_sink_name}"
            )
            raise ValueError("Bucket name is not specified in the data sink metadata.")

        project_name_cap = (
            self.data_sink.project_id[:1].upper() + self.data_sink.project_id[1:].lower()
        )

        object_name = (
            f"{project_name_cap}/PHOENIX/PROTECTED/"
            f"{project_name_cap}{self.data_sink.site_id}/raw/"
            f"{push_metadata.get('subject_id', 'unknown')}/"
            f"{push_metadata.get('modality', 'unknown')}/"
            f"{file_to_push.name}"
        )

        try:
            with Timer() as timer:
                parsed_url = urlparse(endpoint_url)
                endpoint = parsed_url.hostname

                if parsed_url.port:
                    endpoint = f"{endpoint}:{parsed_url.port}"

                client = Minio(
                    endpoint=endpoint,  # type: ignore
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=parsed_url.scheme == "https",
                )
                client.fput_object(bucket_name, object_name, str(file_to_push))
        except Exception as e:  # pylint: disable=broad-except
            log_message = (
                f"Failed to push file {file_to_push} to MinIO bucket {bucket_name}: {e}"
            )
            logger.error(log_message)
            logger.debug(
                f"endpoint_url: {endpoint_url}"
            )
            Logs(
                log_level="ERROR",
                log_message={
                    "event": "minio_push_error",
                    "message": log_message,
                    "data_sink_name": self.data_sink.data_sink_name,
                    "project_id": self.data_sink.project_id,
                    "site_id": self.data_sink.site_id,
                    "file_path": file_to_push,
                },
            ).insert(config_file)
            raise e

        data_push = DataPush(
            data_sink_id=self.data_sink.get_data_sink_id(config_file=config_file),  # type: ignore
            file_path=str(file_to_push),
            file_md5=hash_helper.compute_fingerprint(file_to_push),
            push_time_s=int(timer.duration) if timer.duration is not None else 0,
            push_metadata={
                "object_name": object_name,
                "bucket_name": bucket_name,
                "endpoint_url": endpoint_url,
            },
            push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        return data_push

    def pull(
        self,
        data_push: DataPush,
        destination_path: Path,
        config_file: Path,
    ) -> Optional[Path]:
        """
        Pulls data from the MinIO sink.

        Args:
            data_push (DataPush): The DataPush object containing push metadata.
            destination_path (Path): The local path where the file will be saved.
            config_file (Path): Path to the configuration file.

        Returns:
            Optional[Path]: The path to the downloaded file if successful, None otherwise.
        """
        # Validate that the DataPush is associated with this DataSink
        current_data_sink_id = self.data_sink.get_data_sink_id(config_file=config_file)
        if data_push.data_sink_id != current_data_sink_id:
            error_msg = (
                f"DataPush (data_sink_id={data_push.data_sink_id}) is not associated "
                f"with this DataSink (data_sink_id={current_data_sink_id}, "
                f"name='{self.data_sink.data_sink_name}'). Cannot pull file."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        minio_metadata = self.data_sink.data_sink_metadata
        bucket_name: Optional[str] = minio_metadata.get("bucket_name")
        keystore_name: Optional[str] = minio_metadata.get("keystore_name")

        if not all([bucket_name, keystore_name]):
            logger.error(
                f"Missing MinIO configuration (bucket_name, or "
                f"keystore_name) for sink {self.data_sink.data_sink_name}"
            )
            raise ValueError("Missing MinIO configuration in data sink metadata.")

        keystore_data = KeyStore.retrieve_keystore(
            key_name=keystore_name,  # type: ignore
            project_id=self.data_sink.project_id,
            config_file=config_file,
        )

        if not keystore_data:
            logger.error(
                f"Failed to retrieve keystore data for {keystore_name} "
                f"in project {self.data_sink.project_id}"
            )
            raise ValueError("Keystore data not found for the specified keystore name.")

        keystore_value: Dict[str, Any] = json.loads(keystore_data.key_value)
        access_key: Optional[str] = keystore_value.get("access_key", None)
        secret_key: Optional[str] = keystore_value.get("secret_key", None)
        endpoint_url: Optional[str] = keystore_value.get("endpoint_url", None)

        if not all([access_key, secret_key, endpoint_url]):
            logger.error(
                "Missing MinIO credentials (access_key, secret_key, or endpoint_url) in "
                f"KeyStore for sink {self.data_sink.data_sink_name}"
            )
            raise ValueError("Missing MinIO credentials in KeyStore.")

        # Extract object name from push metadata
        object_name: Optional[str] = data_push.push_metadata.get("object_name")

        if not object_name:
            logger.error(
                f"Missing object_name in DataPush metadata for data_push {data_push}"
            )
            raise ValueError("Missing object_name in DataPush metadata.")

        # Ensure the destination directory exists
        destination_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with Timer() as timer:
                parsed_url = urlparse(endpoint_url)
                endpoint = parsed_url.hostname

                if parsed_url.port:
                    endpoint = f"{endpoint}:{parsed_url.port}"

                client = Minio(
                    endpoint=endpoint,  # type: ignore
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=parsed_url.scheme == "https",
                )
                client.fget_object(
                    bucket_name,  # type: ignore
                    object_name,
                    str(destination_path),
                )

            logger.info(
                f"Successfully pulled file from MinIO to "
                f"{destination_path} in {timer.duration:.2f}s"
            )
            return destination_path

        except Exception as e:  # pylint: disable=broad-except
            log_message = (
                f"Failed to pull file from MinIO bucket {bucket_name}, "
                f"object {object_name}: {e}"
            )
            logger.error(log_message)

            Logs(
                log_level="ERROR",
                log_message={
                    "event": "minio_pull_error",
                    "message": log_message,
                    "data_sink_name": self.data_sink.data_sink_name,
                    "project_id": self.data_sink.project_id,
                    "site_id": self.data_sink.site_id,
                    "destination_path": str(destination_path),
                },
            ).insert(config_file)
            raise e
