#!/usr/bin/env python
"""
Refreshes REDCap dicionary in the database.

This script is intended to be run as a cron job.
It will refresh the REDCap dictionary for all active REDCap data sources
in the database.
"""

import sys
from pathlib import Path
import argparse

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

import logging
from typing import Any, Dict, List, Optional, cast, Set

import pandas as pd
import requests
from rich.logging import RichHandler

from lochness.helpers import logs, utils, db
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.sources.redcap.models.data_source import RedcapDataSource

MODULE_NAME = "lochness.sources.redcap.tasks.pull_dictionary"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def log_event(
    config_file: Path,
    log_level: str,
    event: str,
    message: str,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    data_source_name: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Standardized logging for REDCap metadata refresh events.

    Args:
        config_file (Path): Path to the config file.
        log_level (str): Log level (e.g., "INFO", "ERROR").
        event (str): Event name.
        message (str): Log message.
        project_id (Optional[str]): Project ID.
        site_id (Optional[str]): Site ID.
        data_source_name (Optional[str]): Data source name.
        extra (Optional[Dict[str, Any]]): Additional key-value pairs
            to include in the log.

    Returns:
        None
    """
    data_source_identifier = (
        f"{project_id}::{site_id}::{data_source_name}"
        if project_id and site_id and data_source_name
        else None
    )

    log_message = {
        "event": event,
        "message": message,
        "project_id": project_id,
        "site_id": site_id,
        "data_source_type": "redcap",
        "module": MODULE_NAME,
    }
    if data_source_identifier:
        log_message["data_source_identifier"] = data_source_identifier
    if extra:
        log_message.update(extra)
    Logs(
        log_level=log_level,
        log_message=log_message,
    ).insert(config_file)


def fetch_dictionary(
    redcap_data_source: RedcapDataSource,
    config_file: Path,
    timeout_s: int = 30,
) -> Optional[pd.DataFrame]:
    """
    Refreshes the data dictionary for a given REDCap data source.

    Args:
        redcap_data_source (RedcapDataSource): The REDCap data source to refresh.
        encryption_passphrase (str): The encryption passphrase for keystore access.
        timeout_s (int): Timeout for the API request.

    Returns:
        Optional[pd.DataFrame]: A DataFrame containing the metadata for the REDCap data source.
    """
    project_id = redcap_data_source.project_id
    site_id = redcap_data_source.site_id
    data_source_name = redcap_data_source.data_source_name

    redcap_endpoint_url: str = redcap_data_source.data_source_metadata.endpoint_url

    identifier = f"{project_id}::{site_id}::{data_source_name}"
    logger.info(f"Refreshing REDCap dictionary for {identifier}...")

    # Get API token from keystore
    keystore = KeyStore.retrieve_keystore(
        redcap_data_source.data_source_metadata.keystore_name,
        project_id,
        config_file,
    )
    if keystore is None:
        logger.error(f"Keystore entry not found for {identifier}")

        log_event(
            config_file=config_file,
            log_level="ERROR",
            event="redcap_metadata_fetch_keystore_missing",
            message=f"Keystore entry not found for {identifier}.",
            project_id=project_id,
            site_id=site_id,
            data_source_name=data_source_name,
        )
        return None

    api_token = keystore.key_value

    data = {
        "token": api_token,
        "content": "metadata",
        "format": "json",
        "returnFormat": "json",
    }

    r = requests.post(redcap_endpoint_url, data=data, timeout=timeout_s)
    if r.status_code != 200:
        logger.error(
            f"Failed to fetch metadata for {identifier}: {r.status_code} - {r.text}"
        )
        return None

    raw_data = r.json()

    if redcap_data_source.data_source_metadata.dictionary is None:
        Logs(
            log_level="INFO",
            log_message={
                "event": "redcap_metadata_initial_dictionary_fetch",
                "message": f"Initial fetch of metadata dictionary for {identifier}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_source_type": "redcap",
                "data_source_identifier": identifier,
                "module": MODULE_NAME,
            },
        ).insert(config_file)
        redcap_data_source.update_data_source_metadata_dictionary(
            config_file=config_file,
            project_id=project_id,
            site_id=site_id,
            dictionary=raw_data
        )
    else:
        if redcap_data_source.data_source_metadata.dictionary == raw_data:
            logger.info(f"No changes in data dictionary for {identifier}.")
            return None

        Logs(
            log_level="INFO",
            log_message={
                "event": "redcap_metadata_dictionary_updated",
                "message": f"Updated metadata dictionary for {identifier}.",
                "project_id": project_id,
                "site_id": site_id,
                "data_source_type": "redcap",
                "data_source_identifier": identifier,
                "module": MODULE_NAME,
            },
        ).insert(config_file)
        redcap_data_source.update_data_source_metadata_dictionary(
            config_file=config_file,
            project_id=project_id,
            site_id=site_id,
            dictionary=raw_data
        )

    return 1


def refresh_redcap_dictionary(
    config_file: Path, project_id: Optional[str] = None, site_id: Optional[str] = None
) -> None:
    """
    Refreshes the metadata for all active REDCap data sources in the database.

    Args:
        config_file (Path): Path to the config file.
        project_id (Optional[str]): If provided, only refresh this project ID.
        site_id (Optional[str]): If provided, only refresh this site ID.

    Returns:
        None
    """
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="redcap_dictionary_refresh_start",
        message="Starting REDCap dictionary refresh process.",
        project_id=project_id,
        site_id=site_id,
    )

    active_redcap_data_sources = RedcapDataSource.get_all_redcap_data_sources(
        config_file=config_file,
        active_only=True,
    )

    # make sure they have subject_id_variable in metadata
    active_redcap_data_sources = [
        ds for ds in active_redcap_data_sources if ds.data_source_metadata.main_redcap
    ]

    # Filter by project_id and/or site_id if provided
    if project_id:
        active_redcap_data_sources = [
            ds for ds in active_redcap_data_sources if ds.project_id == project_id
        ]
    if site_id:
        active_redcap_data_sources = [
            ds for ds in active_redcap_data_sources if ds.site_id == site_id
        ]

    if not active_redcap_data_sources:
        logger.info("No active REDCap data sources found.")
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_dictionary_refresh_no_active_sources",
            message="No active REDCap data sources found for refresh dictionary.",
            project_id=project_id,
            site_id=site_id,
        )
        return

    logger.info(f"Found {len(active_redcap_data_sources)} active REDCap data sources.")
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="redcap_dictionary_refresh_active_sources_found",
        message=f"Found {len(active_redcap_data_sources)} active REDCap data sources.",
        project_id=project_id,
        site_id=site_id,
        extra={"count": len(active_redcap_data_sources)},
    )

    for redcap_data_source in active_redcap_data_sources:
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_dictionary_fetch_start",
            message=f"Starting dictionary fetch for {redcap_data_source.data_source_name}.",
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            data_source_name=redcap_data_source.data_source_name,
        )

        fetch_dictionary(
            redcap_data_source=redcap_data_source,
            config_file=config_file,
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Refresh REDCap metadata for all or specific project/site."
    )
    parser.add_argument(
        "--project_id", type=str, default=None, help="Project ID to refresh (optional)"
    )
    parser.add_argument(
        "--site_id", type=str, default=None, help="Site ID to refresh (optional)"
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    logger.info(f"Using config file: {config_file}")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Refreshing REDCap dictionary...")
    refresh_redcap_dictionary(
        config_file=config_file, project_id=args.project_id, site_id=args.site_id
    )

    logger.info("Finished refreshing REDCap dictionary.")
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="redcap_dictionary_refresh_complete",
        message="Finished REDCap dictionary refresh process.",
        project_id=args.project_id,
        site_id=args.site_id,
    )
