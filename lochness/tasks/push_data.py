#!/usr/bin/env python
"""
Pushes data from the local file system to configured data sinks.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import argparse
import logging
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

from rich.logging import RichHandler

from lochness.helpers import db, fs, logs, utils
from lochness.models.data_pulls import DataPull
from lochness.models.data_push import DataPush
from lochness.models.data_sinks import DataSink
from lochness.models.files import File
from lochness.models.logs import Logs
from lochness.sinks.azure_blob_storage.blob_sink import AzureBlobSink
from lochness.sinks.data_sink_i import DataSinkI
from lochness.sinks.filesystem.filesystem_sink import FilesystemSink
from lochness.sinks.minio_object_store.minio_sink import MinioSink

MODULE_NAME = "lochness.tasks.push_data"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    # "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)
NOISY_MODULES = [
    "azure.core.pipeline.policies.http_logging_policy",
    "urllib3.connectionpool",
]
logs.silence_logs(
    NOISY_MODULES,
    target_level=logging.WARNING,
)


class FileSourceResult:
    """
    Result of file source resolution.

    Attributes:
        source_type (str): One of "local", "remote", or "skip"
        file_path (Optional[Path]): Path to the local file (for "local" source type)
        source_data_sink (Optional[DataSink]): DataSink to pull from (for "remote" source type)
        source_data_push (Optional[DataPush]): DataPush record for the remote file (for "remote" source type)
        skip_reason (Optional[str]): Reason for skipping (for "skip" source type)
    """

    def __init__(
        self,
        source_type: str,
        file_path: Optional[Path] = None,
        source_data_sink: Optional[DataSink] = None,
        source_data_push: Optional[DataPush] = None,
        skip_reason: Optional[str] = None,
    ):
        """
        Initialize FileSourceResult.

        Args:
            source_type: One of "local", "remote", or "skip"
            file_path: Path to the local file (for "local" source type)
            source_data_sink: DataSink to pull from (for "remote" source type)
            source_data_push: DataPush record for the remote file (for "remote" source type)
            skip_reason: Reason for skipping (for "skip" source type)
        """
        self.source_type = source_type
        self.file_path = file_path
        self.source_data_sink = source_data_sink
        self.source_data_push = source_data_push
        self.skip_reason = skip_reason


def resolve_file_source(
    file_obj: File,
    config_file: Path,
    target_data_sink_id: int,
) -> FileSourceResult:
    """
    Determine where to source a file from based on its available_at metadata.

    Resolution order:
    1. If file is available locally (hn:<current_hostname>), use local path
    2. If file is available in another data sink (ds:<id>), pull from that sink
    3. If no valid sources, skip the file (another instance will handle it)

    Args:
        file_obj: The File object to resolve source for
        config_file: Path to the configuration file
        target_data_sink_id: The ID of the target data sink we're pushing to

    Returns:
        FileSourceResult with source_type "local", "remote", or "skip"
    """
    current_hostname = utils.get_hostname()
    hostname_key = f"hn:{current_hostname}"

    available_at = file_obj.file_metadata.get("available_at", [])
    if isinstance(available_at, str):
        available_at = [available_at]
    elif not isinstance(available_at, list):
        available_at = []

    # Check for local availability first
    if hostname_key in available_at:
        logger.debug(
            f"File {file_obj.file_name} is available locally at {file_obj.file_path}"
        )
        return FileSourceResult(
            source_type="local",
            file_path=file_obj.file_path,
        )

    # Check for availability in other data sinks
    data_sink_ids = []
    for location in available_at:
        if location.startswith("ds:"):
            try:
                sink_id = int(location.split(":")[1])
                # Don't pull from the sink we're pushing to
                if sink_id != target_data_sink_id:
                    data_sink_ids.append(sink_id)
            except (ValueError, IndexError):
                logger.warning(f"Invalid data sink location format: {location}")
                continue

    # Try to find a valid source data sink
    for sink_id in data_sink_ids:
        source_sink = DataSink.get_data_sink_by_id(config_file, sink_id)
        if source_sink is None:
            logger.warning(f"Data sink with ID {sink_id} not found, skipping")
            continue

        if not source_sink.is_active:
            logger.debug(f"Data sink {sink_id} is not active, skipping")
            continue

        # Get the DataPush record for this file from the source sink
        source_push = DataPush.get_data_push(
            config_file=config_file,
            data_sink_id=sink_id,
            file_path=str(file_obj.file_path),
            file_md5=file_obj.md5,  # type: ignore
        )

        if source_push is None:
            logger.warning(
                f"No DataPush record found for file {file_obj.file_name} "
                f"in data sink {sink_id}, skipping"
            )
            continue

        logger.info(
            f"File {file_obj.file_name} will be sourced from "
            f"data sink {source_sink.data_sink_name} (ID: {sink_id})"
        )
        return FileSourceResult(
            source_type="remote",
            source_data_sink=source_sink,
            source_data_push=source_push,
        )

    # No valid sources found - skip file
    skip_reason = (
        f"File {file_obj.file_name} is not available locally "
        f"(hostname: {current_hostname}) and no valid remote sources found. "
        f"available_at: {available_at}"
    )
    logger.info(skip_reason)
    return FileSourceResult(
        source_type="skip",
        skip_reason=skip_reason,
    )


def get_sink_instance(data_sink: DataSink) -> Optional[DataSinkI]:
    """
    Create a DataSinkI instance based on the sink type.

    Args:
        data_sink: The DataSink object

    Returns:
        DataSinkI instance or None if type is not supported
    """
    sink_type = data_sink.data_sink_metadata.get("type")
    if sink_type == "minio":
        return MinioSink(data_sink=data_sink)
    elif sink_type == "azure_blob":
        return AzureBlobSink(data_sink=data_sink)
    elif sink_type == "filesystem":
        return FilesystemSink(data_sink=data_sink)
    return None


def pull_file_from_source(
    source_result: FileSourceResult,
    config_file: Path,
) -> Optional[Path]:
    """
    Pull a file from a remote data sink to a local temporary location.

    Args:
        source_result: FileSourceResult with source_type="remote"
        config_file: Path to the configuration file

    Returns:
        Path to the local temporary file, or None if pull fails
    """
    if source_result.source_type != "remote":
        raise ValueError("source_result must have source_type='remote'")

    if source_result.source_data_sink is None or source_result.source_data_push is None:
        raise ValueError(
            "source_result is missing source_data_sink or source_data_push"
        )

    source_sink = source_result.source_data_sink
    source_push = source_result.source_data_push

    sink_instance = get_sink_instance(source_sink)
    if sink_instance is None:
        sink_type = source_sink.data_sink_metadata.get("type")
        logger.error(f"No pull handler found for sink type: {sink_type}")
        return None

    # Create a temporary file to store the pulled data
    # Use the original file name with Data Sink Name prefix
    original_file_name = Path(source_push.file_path).name
    temp_dir = tempfile.mkdtemp(prefix=f"lochness_pull_{source_sink.data_sink_name}_")
    temp_file_path = Path(temp_dir) / original_file_name

    try:
        pulled_path = sink_instance.pull(
            data_push=source_push,
            destination_path=temp_file_path,
            config_file=config_file,
        )
        if pulled_path and pulled_path.exists():
            logger.info(f"Successfully pulled file to {pulled_path}")
            return pulled_path
        else:
            logger.error(f"Failed to pull file from {source_sink.data_sink_name}")
            return None
    except Exception as e:
        logger.error(f"Error pulling file from {source_sink.data_sink_name}: {e}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "data_pull_from_sink_error",
                "message": f"Failed to pull file from source sink: {e}",
                "source_sink_name": source_sink.data_sink_name,
                "file_path": source_push.file_path,
            },
        ).insert(config_file)
        return None


def push_file_to_sink(
    file_obj: File,
    data_sink: DataSink,
    data_source_name: str,
    project_id: str,
    site_id: str,
    modality: str,
    subject_id: str,
    config_file: Path,
    source_file_path: Optional[Path] = None,
) -> bool:
    """
    Dispatches the file push to the appropriate sink-specific handler.
    Only pushes if the file (by path and md5) has not already been
    pushed to this sink.

    Args:
        file_obj: The File object representing the file to push
        data_sink: The target DataSink
        data_source_name: Name of the data source
        project_id: Project ID
        site_id: Site ID
        modality: Data modality
        subject_id: Subject ID
        config_file: Path to the configuration file
        source_file_path: Optional path to use as source file. If None,
            uses file_obj.file_path. This allows pushing from a temporary
            file pulled from another data sink.
    """
    # Use provided source path or fall back to file_obj.file_path
    actual_file_path = source_file_path if source_file_path else file_obj.file_path

    sink_type = data_sink.data_sink_metadata.get("type")
    if not sink_type:
        msg = (
            f"Data sink {data_sink.data_sink_name} "
            "has no 'type' defined in its metadata."
        )
        logger.error(msg)
        Logs(
            log_level="ERROR",
            log_message={
                "event": "data_push_missing_sink_type",
                "message": msg,
                "data_sink_name": data_sink.data_sink_name,
                "project_id": data_sink.project_id,
                "site_id": data_sink.site_id,
            },
        ).insert(config_file)
        return False

    try:
        data_sink_i: Optional[DataSinkI] = get_sink_instance(data_sink)
        if data_sink_i is None:
            raise ModuleNotFoundError

        start_time = datetime.now()
        data_push: DataPush = data_sink_i.push(
            file_to_push=actual_file_path,
            config_file=config_file,
            push_metadata={
                "data_source_name": data_source_name,
                "subject_id": subject_id,
                "site_id": site_id,
                "project_id": project_id,
                "file_name": file_obj.file_name,
                "file_size_mb": file_obj.file_size_mb,  # type: ignore
                "modality": modality,
            },
        )
        end_time = datetime.now()
        push_time_s = int((end_time - start_time).total_seconds())

        if data_push:
            # Replace data_push's file_path and file_md5 with the original values
            data_push.file_path = str(file_obj.file_path)
            data_push.file_md5 = file_obj.md5  # type: ignore

            # Update file metadata to track data sink availability
            data_sink_id: int = data_sink.get_data_sink_id(config_file)  # type: ignore
            ds_location = f"ds:{data_sink_id}"

            # Build list of queries to execute in a single transaction
            queries: List[str] = []

            # 1. Record successful push in data_pushes table
            queries.append(data_push.to_sql_query())

            # 2. Remove the data sink location from old versions of this file
            # (the previous file at this path is no longer available at this sink)
            remove_old_query = File.remove_availability_query(
                file_path=file_obj.file_path,
                current_md5=file_obj.md5,  # type: ignore
                location=ds_location,
            )
            queries.append(remove_old_query)

            # 3. Add the data sink location to the current file
            update_query = file_obj.add_available_at_query(ds_location)
            if update_query:
                queries.append(update_query)

            # Execute all queries in a single transaction
            db.execute_queries(
                config_file,
                queries,
                show_commands=False,
                silent=True,
            )

            Logs(
                log_level="INFO",
                log_message={
                    "event": "data_push_success",
                    "message": (
                        f"Successfully pushed {file_obj.file_name} to "
                        f"{data_sink.data_sink_name}."
                    ),
                    "file_path": str(file_obj.file_path),
                    "data_sink_name": data_sink.data_sink_name,
                    "project_id": data_sink.project_id,
                    "site_id": data_sink.site_id,
                    "push_time_s": push_time_s,
                },
            ).insert(config_file)
            return True
        else:
            Logs(
                log_level="ERROR",
                log_message={
                    "event": "data_push_failed",
                    "message": (
                        f"Failed to push {file_obj.file_name} to "
                        f"{data_sink.data_sink_name}."
                    ),
                    "file_path": str(file_obj.file_path),
                    "data_sink_name": data_sink.data_sink_name,
                    "project_id": data_sink.project_id,
                    "site_id": data_sink.site_id,
                },
            ).insert(config_file)
            return False

    except ModuleNotFoundError:
        logger.error(f"No push handler found for sink type: {sink_type}")
        Logs(
            log_level="ERROR",
            log_message={
                "event": "data_push_handler_not_found",
                "message": f"No push handler found for sink type: {sink_type}.",
                "sink_type": sink_type,
                "data_sink_name": data_sink.data_sink_name,
            },
        ).insert(config_file)
        return False

    # except Exception as e:  # pylint: disable=broad-except
    #     logger.error(
    #         f"Error pushing file {file_obj.file_name} to "
    #         f"{data_sink.data_sink_name}: {e}"
    #     )
    #     Logs(
    #         log_level="ERROR",
    #         log_message={
    #             "event": "data_push_exception",
    #             "message": (
    #                 f"Exception during push of {file_obj.file_name} to "
    #                 f"{data_sink.data_sink_name}."
    #             ),
    #             "file_path": str(file_obj.file_path),
    #             "data_sink_name": data_sink.data_sink_name,
    #             "error": str(e),
    #         },
    #     ).insert(config_file)
    #     return False


def simple_push_file_to_sink(file_path: Path):
    """
    Push a file to the appropriate data sink.

    This function retrieves the most recent file object and data pull related
    to the specified file path from the configuration file. It then identifies
    the matching data sink for the project's site, encrypts the file using the
    passphrase from the configuration, and pushes the file to the data sink.

    Parameters:
    - file_path (Path): The path of the file.

    Returns:
    - result: The result of the push operation, indicating success or failure.
    """

    config_file = utils.get_config_file_path()

    file_obj = File.get_most_recent_file_obj(config_file, file_path)

    if file_obj is None:
        raise FileNotFoundError(f"File {file_path} not found in the database.")

    data_pull = DataPull.get_most_recent_data_pull(
        config_file, str(file_obj.file_path), file_obj.md5  # type: ignore
    )

    if data_pull is None:
        raise ValueError(
            f"No data pull associated with file {file_obj.file_name} "
            f"(md5={file_obj.md5})."
        )

    data_sink = DataSink.get_matching_data_sink(
        config_file=config_file,
        site_id=data_pull.site_id,
        project_id=data_pull.project_id,
    )

    if data_sink is None:
        raise ValueError(
            f"No matching data sink found for site {data_pull.site_id} "
            f"and project {data_pull.project_id}."
        )

    result = push_file_to_sink(
        file_obj=file_obj,
        modality="unknown",
        data_sink=data_sink,
        data_source_name="unknown",
        project_id=data_pull.project_id,
        site_id=data_pull.site_id,
        subject_id=data_pull.subject_id,
        config_file=config_file,
    )
    return result


def get_matching_data_sink_list(
    config_file: Path, project_id: Optional[str], site_id: Optional[str]
) -> List[DataSink]:
    """
    Retrieves a list of active data sinks based on the provided project and site IDs.

    Args:
        config_file (Path): Path to the configuration file.
        project_id (Optional[str]): Project ID to filter data sinks.
        site_id (Optional[str]): Site ID to filter data sinks.

    Returns:
        List[DataSink]: A list of active DataSink objects that match the criteria.
    """
    active_data_sinks = DataSink.get_all_data_sinks(
        config_file=config_file,
        active_only=True,
    )

    if project_id:
        active_data_sinks = [
            ds for ds in active_data_sinks if ds.project_id == project_id
        ]
    if site_id:
        active_data_sinks = [ds for ds in active_data_sinks if ds.site_id == site_id]

    if not active_data_sinks:
        logger.info("No active data sinks found.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "data_push_no_active_sinks",
                "message": "No active data sinks found for push.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return []

    project_id_display = project_id or "ALL"
    site_id_display = site_id or "ALL"
    logger.info(
        f"Found {len(active_data_sinks)} active data sinks for "
        f"{project_id_display}::{site_id_display}."
    )
    Logs(
        log_level="INFO",
        log_message={
            "event": "data_push_active_sinks_found",
            "message": f"Found {len(active_data_sinks)} active data sinks.",
            "count": len(active_data_sinks),
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

    return active_data_sinks


def push_all_data(
    config_file: Path,
    project_id: Optional[str],
    site_id: Optional[str],
) -> None:
    """
    Function to push data to all active data sinks.

    Args:
        config_file (Path): Path to the configuration file.
        project_id (Optional[str]): Project ID to filter data sinks.
            If None, all projects are processed.
        site_id (Optional[str]): Site ID to filter data sinks.
            If None, all sites are processed.

    Returns:
        None
    """
    Logs(
        log_level="INFO",
        log_message={
            "event": "data_push_start",
            "message": "Starting data push process.",
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

    active_data_sinks = get_matching_data_sink_list(config_file, project_id, site_id)
    if not active_data_sinks:
        logger.info("No active data sinks found, skipping data push.")
        return

    for active_data_sink in active_data_sinks:
        data_sink_id: int = active_data_sink.get_data_sink_id(  # type: ignore
            config_file
        )

        logger.debug(
            f"Processing data sink: {data_sink_id} "
            f"(Project ID: {active_data_sink.project_id}, "
            f"Site ID: {active_data_sink.site_id})"
        )

        files_to_push = File.get_files_to_push(
            config_file=config_file,
            project_id=active_data_sink.project_id,
            site_id=active_data_sink.site_id,
            data_sink_id=data_sink_id,
        )
        if not files_to_push:
            logger.info("No files found to push.")
            Logs(
                log_level="INFO",
                log_message={
                    "event": "data_push_no_files_to_push",
                    "message": "No files found in the database to push.",
                    "project_id": project_id,
                    "site_id": site_id,
                    "data_sink_name": active_data_sink.data_sink_name,
                    "data_sink_id": data_sink_id,
                },
            ).insert(config_file)
            continue

        logger.info(f"Found {len(files_to_push)} files to push.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "data_push_files_found_to_push",
                "message": f"Found {len(files_to_push)} files to push.",
                "count": len(files_to_push),
                "project_id": project_id,
                "site_id": site_id,
                "data_sink_name": active_data_sink.data_sink_name,
                "data_sink_id": data_sink_id,
            },
        ).insert(config_file)

        for file_obj in files_to_push:
            logger.info(
                f"Attempting to push {file_obj.file_name} to "
                f"{active_data_sink.data_sink_name}..."
            )

            # Resolve where to source the file from
            source_result = resolve_file_source(
                file_obj=file_obj,
                config_file=config_file,
                target_data_sink_id=data_sink_id,
            )

            # Handle skip case - file not available on this instance
            if source_result.source_type == "skip":
                logger.info(
                    f"Skipping file {file_obj.file_name}: {source_result.skip_reason}"
                )
                Logs(
                    log_level="INFO",
                    log_message={
                        "event": "data_push_file_skipped",
                        "message": source_result.skip_reason,
                        "file_path": str(file_obj.file_path),
                        "data_sink_name": active_data_sink.data_sink_name,
                        "project_id": project_id,
                        "site_id": site_id,
                    },
                ).insert(config_file)
                continue

            # Determine source file path
            source_file_path: Optional[Path] = None
            temp_file_path: Optional[Path] = None

            if source_result.source_type == "local":
                source_file_path = source_result.file_path
            elif source_result.source_type == "remote":
                # Pull file from remote data sink
                temp_file_path = pull_file_from_source(
                    source_result=source_result,
                    config_file=config_file,
                )
                if temp_file_path is None:
                    logger.error(
                        f"Failed to pull file {file_obj.file_name} from remote source, skipping"
                    )
                    Logs(
                        log_level="ERROR",
                        log_message={
                            "event": "data_push_remote_pull_failed",
                            "message": "Failed to pull file from remote source",
                            "file_path": str(file_obj.file_path),
                            "source_sink_name": (
                                source_result.source_data_sink.data_sink_name
                                if source_result.source_data_sink
                                else "unknown"
                            ),
                            "data_sink_name": active_data_sink.data_sink_name,
                        },
                    ).insert(config_file)
                    continue
                source_file_path = temp_file_path

            associated_data_pull = File.get_recent_data_pull(
                config_file=config_file,
                file_path=file_obj.file_path,
            )

            if associated_data_pull is None:
                logger.warning(
                    f"No associated data pull found for file {file_obj.file_name}."
                )
                Logs(
                    log_level="WARN",
                    log_message={
                        "event": "data_push_no_associated_data_pull",
                        "message": (
                            f"No associated data pull found for file "
                            f"{file_obj.file_name}."
                        ),
                        "file_path": str(file_obj.file_path),
                        "data_sink_name": active_data_sink.data_sink_name,
                        "project_id": project_id,
                        "site_id": site_id,
                    },
                ).insert(config_file)
                subject_id = "unknown"
                associated_modality = "unknown"
                associated_data_source_name = "unknown"
            else:
                associated_data_source = (
                    associated_data_pull.get_associated_data_source(
                        config_file=config_file
                    )
                )
                subject_id = associated_data_pull.subject_id
                associated_data_source_name = associated_data_source.data_source_name
                associated_modality = associated_data_source.data_source_metadata.get(
                    "modality", "unknown"
                )

            try:
                push_file_to_sink(
                    file_obj=file_obj,
                    data_sink=active_data_sink,
                    data_source_name=associated_data_source_name,
                    modality=associated_modality,
                    project_id=active_data_sink.project_id,
                    site_id=active_data_sink.site_id,
                    subject_id=subject_id,
                    config_file=config_file,
                    source_file_path=source_file_path,
                )
            finally:
                # Clean up temporary file if we pulled from remote
                if temp_file_path and temp_file_path.exists():
                    try:
                        temp_dir = temp_file_path.parent
                        fs.remove(temp_dir)
                        logger.debug(f"Cleaned up temporary directory: {temp_dir}")
                    except Exception as cleanup_error:
                        logger.warning(
                            f"Failed to clean up temp file {temp_file_path}: {cleanup_error}"
                        )

        Logs(
            log_level="INFO",
            log_message={
                "event": "data_push_complete",
                "message": "Finished data push process.",
                "project_id": project_id,
                "site_id": site_id,
                "data_sink_name": active_data_sink.data_sink_name,
                "data_sink_id": data_sink_id,
            },
        ).insert(config_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Push data to all or specific data sinks."
    )
    parser.add_argument(
        "--project_id",
        "-p",
        type=str,
        default=None,
        help="Project ID to push data for (optional, defaults to all projects)",
    )
    parser.add_argument(
        "--site_id",
        "-s",
        type=str,
        default=None,
        help="Site ID to push data for (optional, defaults to all sites)",
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"Configuration file {config_file} does not exist.")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file,
        module_name=MODULE_NAME,
        logger=logger,
        noisy_modules=NOISY_MODULES,
    )

    logger.info("Starting data push...")
    logger.debug(f"Using configuration file: {config_file}")

    project_id: Optional[str] = args.project_id
    site_id: Optional[str] = args.site_id

    logger.debug(f"Project ID: {project_id or 'ALL'}, Site ID: {site_id or 'ALL'}")

    push_all_data(config_file=config_file, project_id=project_id, site_id=site_id)

    logger.info("Finished data push.")
