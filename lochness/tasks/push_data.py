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
from typing import Any, Dict, List, Optional
from datetime import datetime

from rich.logging import RichHandler

from lochness.helpers import logs, utils, db
from lochness.models.data_sinks import DataSink
from lochness.models.files import File
from lochness.models.data_push import DataPush
from lochness.models.data_pulls import DataPull
from lochness.models.logs import Logs
from lochness.sinks.data_sink_i import DataSinkI
from lochness.sinks.minio_object_store.minio_sink import MinioSink
from lochness.sinks.azure_blob_storage.blob_sink import AzureBlobSink
from lochness.sinks.filesystem.filesystem_sink import FilesystemSink

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
NOISY_MODULES = ["azure.core.pipeline.policies.http_logging_policy", "urllib3.connectionpool"]
logs.silence_logs(
    NOISY_MODULES,
    target_level=logging.WARNING,
)


def push_file_to_sink(
    file_obj: File,
    data_sink: DataSink,
    data_source_name: str,
    project_id: str,
    site_id: str,
    modality: str,
    subject_id: str,
    config_file: Path,
) -> bool:
    """
    Dispatches the file push to the appropriate sink-specific handler.
    Only pushes if the file (by path and md5) has not already been
    pushed to this sink.
    """
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
        data_sink_i: Optional[DataSinkI] = None
        if sink_type == "minio":
            data_sink_i = MinioSink(data_sink=data_sink)
            data_sink_i.data_sink = data_sink
        elif sink_type == "azure_blob":
            data_sink_i = AzureBlobSink(data_sink=data_sink)
            data_sink_i.data_sink = data_sink
        elif sink_type == "filesystem":
            data_sink_i = FilesystemSink(data_sink=data_sink)
            data_sink_i.data_sink = data_sink

        if data_sink_i is None:
            raise ModuleNotFoundError

        start_time = datetime.now()
        data_push: DataPush = data_sink_i.push(
            file_to_push=file_obj.file_path,
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
            # Record successful push in data_pushes table
            db.execute_queries(
                config_file,
                [data_push.to_sql_query()],
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
            return

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
                associated_data_source_name = (
                    associated_data_source.data_source_name
                )
                associated_modality = (
                    associated_data_source.data_source_metadata.get(
                        "modality", "unknown"
                    )
                )

            push_file_to_sink(
                file_obj=file_obj,
                data_sink=active_data_sink,
                data_source_name=associated_data_source_name,
                modality=associated_modality,
                project_id=active_data_sink.project_id,
                site_id=active_data_sink.site_id,
                subject_id=subject_id,
                config_file=config_file,
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
        config_file=config_file, module_name=MODULE_NAME, logger=logger, noisy_modules=NOISY_MODULES
    )

    logger.info("Starting data push...")
    logger.debug(f"Using configuration file: {config_file}")

    project_id: Optional[str] = args.project_id
    site_id: Optional[str] = args.site_id

    logger.debug(f"Project ID: {project_id or 'ALL'}, Site ID: {site_id or 'ALL'}")

    push_all_data(config_file=config_file, project_id=project_id, site_id=site_id)

    logger.info("Finished data push.")
