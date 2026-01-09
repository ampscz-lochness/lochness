"""
Implementation of a data sink for local and remote filesystems using rsync.
"""
import json
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

from lochness.helpers.timer import Timer
from lochness.models.data_push import DataPush
from lochness.models.keystore import KeyStore
from lochness.sinks.data_sink_i import DataSinkI
from lochness.models.logs import Logs
from lochness.helpers import hash as hash_helper

logger = logging.getLogger(__name__)


class FilesystemSink(DataSinkI):
    """
    A concrete implementation of a data sink for local/remote filesystems.

    This class implements the abstract methods defined in DataSinkI
    to push data to a filesystem destination using rsync. Rsync is chosen
    because it handles both small and large files efficiently, supports
    incremental transfers, and works seamlessly with both local and
    remote (SSH) destinations.
    """

    def _validate_rsync_available(self) -> None:
        """
        Validates that rsync is available on the system.

        Raises:
            RuntimeError: If rsync is not found in the system PATH.
        """
        if shutil.which("rsync") is None:
            raise RuntimeError(
                "rsync is not available on this system. "
                "Please install rsync to use the FilesystemSink."
            )

    def _build_rsync_command(
        self,
        source_path: Path,
        destination: str,
        ssh_key_path: Optional[str] = None,
        ssh_port: int = 22,
    ) -> List[str]:
        """
        Builds the rsync command with appropriate options.

        Args:
            source_path (Path): The local file path to sync.
            destination (str): The destination path (local or remote).
            ssh_key_path (Optional[str]): Path to SSH private key for remote.
            ssh_port (int): SSH port number for remote destinations.

        Returns:
            List[str]: The rsync command as a list of arguments.
        """
        command = [
            "rsync",
            "-avz",  # archive, verbose, compress
            "--progress",
            "--partial",  # keep partially transferred files
            "--timeout=300",  # 5 minute timeout
        ]

        # Add SSH options if key path is provided
        if ssh_key_path:
            ssh_options = f"-e 'ssh -i {ssh_key_path} -p {ssh_port} -o StrictHostKeyChecking=no'"
            command.append(ssh_options)
        elif ssh_port != 22:
            ssh_options = f"-e 'ssh -p {ssh_port} -o StrictHostKeyChecking=no'"
            command.append(ssh_options)

        command.extend([str(source_path), destination])

        return command

    def _execute_rsync(
        self,
        command: List[str],
    ) -> subprocess.CompletedProcess:
        """
        Executes the rsync command.

        Args:
            command (List[str]): The rsync command to execute.

        Returns:
            subprocess.CompletedProcess: The result of the rsync execution.

        Raises:
            RuntimeError: If rsync command fails.
        """
        logger.debug(f"Executing rsync command: {' '.join(command)}")

        result = subprocess.run(
            " ".join(command),
            shell=True,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            error_msg = (
                f"rsync failed with return code {result.returncode}: "
                f"{result.stderr}"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.debug(f"rsync stdout: {result.stdout}")
        return result

    def push(
        self,
        file_to_push: Path,
        push_metadata: Dict[str, str],
        config_file: Path,
    ) -> DataPush:
        """
        Pushes data to the filesystem sink using rsync.

        Args:
            file_to_push (Path): The path to the file to be pushed.
            push_metadata (Dict[str, str]): Metadata associated with the push.
            config_file (Path): Path to the configuration file.

        Returns:
            DataPush: An instance of DataPush if successful.

        Raises:
            ValueError: If required configuration is missing.
            RuntimeError: If rsync is not available or fails.
        """
        self._validate_rsync_available()

        filesystem_metadata = self.data_sink.data_sink_metadata
        keystore_name: Optional[str] = filesystem_metadata.get("keystore_name")

        if not keystore_name:
            logger.error(
                f"Missing filesystem configuration (keystore_name) "
                f"for sink {self.data_sink.data_sink_name}"
            )
            logger.debug(f"Data sink metadata: {filesystem_metadata}")
            raise ValueError(
                "Missing filesystem configuration in data sink metadata."
            )

        keystore_data = KeyStore.retrieve_keystore(
            key_name=keystore_name,
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

        logger.debug(f"Retrieved keystore data for {keystore_name}")

        keystore_value: Dict[str, Any] = json.loads(keystore_data.key_value)
        destination_path: Optional[str] = keystore_value.get("destination_path")
        ssh_host: Optional[str] = keystore_value.get("ssh_host")
        ssh_user: Optional[str] = keystore_value.get("ssh_user")
        ssh_key_path: Optional[str] = keystore_value.get("ssh_key_path")
        ssh_port: int = keystore_value.get("ssh_port", 22)

        if not destination_path:
            logger.error(
                f"Missing destination_path in keystore for sink "
                f"{self.data_sink.data_sink_name}"
            )
            raise ValueError("Missing destination_path in filesystem credentials.")

        # Build the destination directory structure
        project_name_cap = (
            self.data_sink.project_id[:1].upper()
            + self.data_sink.project_id[1:].lower()
        )

        relative_path = (
            f"{project_name_cap}/PHOENIX/PROTECTED/"
            f"{project_name_cap}{self.data_sink.site_id}/raw/"
            f"{push_metadata.get('subject_id', 'unknown')}/"
            f"{push_metadata.get('modality', 'unknown')}/"
        )

        # Construct full destination path
        if ssh_host and ssh_user:
            # Remote destination via SSH
            full_destination = (
                f"{ssh_user}@{ssh_host}:{destination_path}/{relative_path}"
            )
        else:
            # Local destination
            full_destination_path = Path(destination_path) / relative_path
            full_destination_path.mkdir(parents=True, exist_ok=True)
            full_destination = str(full_destination_path)

        object_name = f"{relative_path}{file_to_push.name}"

        try:
            with Timer() as timer:
                command = self._build_rsync_command(
                    source_path=file_to_push,
                    destination=full_destination,
                    ssh_key_path=ssh_key_path,
                    ssh_port=ssh_port,
                )
                self._execute_rsync(command)

        except Exception as e:
            log_message = (
                f"Failed to push file {file_to_push} to filesystem "
                f"destination {full_destination}: {e}"
            )
            logger.error(log_message)
            logger.debug(f"destination_path: {destination_path}")
            Logs(
                log_level="ERROR",
                log_message={
                    "event": "filesystem_push_error",
                    "message": log_message,
                    "data_sink_name": self.data_sink.data_sink_name,
                    "project_id": self.data_sink.project_id,
                    "site_id": self.data_sink.site_id,
                    "file_path": str(file_to_push),
                },
            ).insert(config_file)
            raise

        data_push = DataPush(
            data_sink_id=self.data_sink.get_data_sink_id(  # type: ignore
                config_file=config_file
            ),
            file_path=str(file_to_push),
            file_md5=hash_helper.compute_fingerprint(file_to_push),
            push_time_s=int(timer.duration) if timer.duration is not None else 0,
            push_metadata={
                "object_name": object_name,
                "destination_path": destination_path,
                "ssh_host": ssh_host,
                "is_remote": ssh_host is not None,
            },
            push_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )

        return data_push
