"""
Implementation of a data sink for Azure Blob Storage.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime

from lochness.sinks.azure_blob_storage import api as azure_api
from lochness.helpers.timer import Timer
from lochness.models.data_push import DataPush
from lochness.models.keystore import KeyStore
from lochness.sinks.data_sink_i import DataSinkI
from lochness.models.logs import Logs
from lochness.helpers import hash as hash_helper

logger = logging.getLogger(__name__)


class AzureBlobSink(DataSinkI):
    """
    A concrete implementation of a data sink for Azure Blob Storage.

    This class implements the abstract methods defined in DataSinkI
    to push data to an Azure Blob Storage account.
    """

    def push(
        self,
        file_to_push: Path,
        push_metadata: Dict[str, str],
        config_file: Path,
    ) -> DataPush:
        """
        Pushes data to the Azure Blob Storage sink.

        Args:
            file_to_push (Path): The path to the file to be pushed.
            push_metadata (Dict[str, str]): Metadata associated with the push.
            data_sink (DataSink): The DataSink object containing Azure Blob Storage configuration.
            config_file (str): Path to the configuration file.

        Returns:
            DataPush: An instance of DataPush if successful, None otherwise.
        """

        azure_metadata = self.data_sink.data_sink_metadata
        container_name: Optional[str] = azure_metadata.get("container_name")
        keystore_name: Optional[str] = azure_metadata.get("keystore_name")

        if not all([container_name, keystore_name]):
            logger.error(
                f"Missing Azure Blob Storage configuration (container_name, or "
                f"keystore_name) for sink {self.data_sink.data_sink_name}"
            )
            logger.debug(f"Data sink metadata: {azure_metadata}")
            raise ValueError("Incomplete Azure Blob Storage configuration.")

        # Retrieve Azure Blob Storage credentials from the keystore
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
        connection_string: Optional[str] = keystore_value.get("connection_string", None)

        if not all([connection_string]):
            logger.error(
                f"Missing Azure Blob Storage credentials (connection_string) "
                f"for keystore {keystore_name} in project {self.data_sink.project_id}"
            )
            raise ValueError("Incomplete Azure Blob Storage credentials.")

        # Check if Container exists
        container_exists = azure_api.check_if_container_exists(
            connection_string=connection_string,  # type: ignore
            container_name=container_name,  # type: ignore
        )

        if not container_exists:
            logger.error(
                f"Azure Blob Storage container '{container_name}' does not exist. "
                f"Cannot push data to sink '{self.data_sink.data_sink_name}'."
            )
            raise ValueError(f"Azure Blob Storage container '{container_name}' does not exist.")

        project_name_cap = (
            self.data_sink.project_id[:1].upper()
            + self.data_sink.project_id[1:].lower()
        )

        object_name = (
            f"{project_name_cap}/PHOENIX/PROTECTED/"
            f"{project_name_cap}{self.data_sink.site_id}/raw/"
            f"{push_metadata.get('subject_id', 'unknown')}/"
            f"{push_metadata.get('modality', 'unknown')}/"
            f"{file_to_push.name}"
        )
        file_md5 = hash_helper.compute_fingerprint(file_to_push)

        try:
            with Timer() as timer:
                azure_metadata = push_metadata.copy()
                # Drop redundant keys
                azure_metadata.pop("file_name", None)
                # Add additional metadata
                azure_metadata["upload_timestamp"] = datetime.now().isoformat()
                azure_metadata["file_md5"] = file_md5

                azure_api.upload_to_blob(
                    connection_string=connection_string,  # type: ignore
                    container_name=container_name,  # type: ignore
                    blob_name=object_name,
                    source_file_path=file_to_push,
                    metadata=azure_metadata,
                )
        except Exception as e:  # pylint: disable=broad-except
            log_message = (
                f"Failed to push file {file_to_push} to Azure Blob Storage: {e}"
            )
            logger.debug(f"azure_metadata: {azure_metadata}")
            logger.error(log_message)

            Logs(
                log_level="ERROR",
                log_message={
                    "event": "azure_blob_push_error",
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
            file_md5=file_md5,
            push_time_s=int(timer.duration) if timer.duration is not None else 0,
            push_metadata={
                "container_name": container_name,
                "object_name": object_name,
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
        Pulls data from Azure Blob Storage.

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

        azure_metadata = self.data_sink.data_sink_metadata
        container_name: Optional[str] = azure_metadata.get("container_name")
        keystore_name: Optional[str] = azure_metadata.get("keystore_name")

        if not all([container_name, keystore_name]):
            logger.error(
                f"Missing Azure Blob Storage configuration (container_name, or "
                f"keystore_name) for sink {self.data_sink.data_sink_name}"
            )
            raise ValueError("Incomplete Azure Blob Storage configuration.")

        # Retrieve Azure Blob Storage credentials from the keystore
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
        connection_string: Optional[str] = keystore_value.get("connection_string", None)

        if not connection_string:
            logger.error(
                f"Missing Azure Blob Storage connection_string "
                f"for keystore {keystore_name} in project {self.data_sink.project_id}"
            )
            raise ValueError("Incomplete Azure Blob Storage credentials.")

        # Extract object name from push metadata
        object_name: Optional[str] = data_push.push_metadata.get("object_name")

        if not object_name:
            logger.error(
                f"Missing object_name in DataPush metadata for data_push_id {data_push}"
            )
            raise ValueError("Missing object_name in DataPush metadata.")

        try:
            with Timer() as timer:
                azure_api.download_from_blob(
                    connection_string=connection_string,  # type: ignore
                    container_name=container_name,  # type: ignore
                    blob_name=object_name,
                    destination_file_path=destination_path,
                )
            logger.info(
                f"Successfully downloaded file from Azure Blob Storage to "
                f"{destination_path} in {timer.duration:.2f}s"
            )
            return destination_path

        except Exception as e:  # pylint: disable=broad-except
            log_message = (
                f"Failed to pull file from Azure Blob Storage "
                f"(container: {container_name}, blob: {object_name}): {e}"
            )
            logger.error(log_message)

            Logs(
                log_level="ERROR",
                log_message={
                    "event": "azure_blob_pull_error",
                    "message": log_message,
                    "data_sink_name": self.data_sink.data_sink_name,
                    "project_id": self.data_sink.project_id,
                    "site_id": self.data_sink.site_id,
                    "destination_path": str(destination_path),
                },
            ).insert(config_file)
            raise e
