#!/usr/bin/env python
"""
Pulls data from REDCap for active data sources and subjects.

This script is intended to be run as a cron job.
It will pull data for all active REDCap data sources and their associated subjects.
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
from typing import Any, List, Dict, Optional
from datetime import datetime

import requests
from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config
from lochness.models.subjects import Subject
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.models.files import File
from lochness.models.data_pulls import DataPull
from lochness.sources.redcap.models.data_source import RedcapDataSource

MODULE_NAME = "lochness.sources.redcap.tasks.pull_data"

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
    subject_id: Optional[str] = None,
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
        "subject_id": subject_id,
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


def add_filter_logic_for_penncnb_redcap(
    filter_logic: str, subject_id: str, subject_id_var: str
):
    """
    Enhances the existing filter logic for fetching data from REDCap by adding
    conditions to handle subject IDs with various suffix patterns used in the
    PennCNB REDCap project.

    This function appends additional logic to the provided filter logic string
    to accommodate different possible suffixes for subject IDs. These suffixes
    are used in REDCap to denote different sessions or versions of a subject's
    data.

    Args:
        filter_logic (str): The initial filter logic string to be enhanced.
        subject_id (str): The subject ID for which data is being fetched.
        subject_id_var (str): The variable name in REDCap that stores the
                            subject ID.

    Returns:
        str: The enhanced filter logic string with additional conditions
            included for handling various subject ID suffix patterns.
    """
    filter_logic = (
        f"[{subject_id_var}] = '{subject_id}' or "
        f"[{subject_id_var}] = '{subject_id.lower()}'"
    )

    digits = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    digits_str = [str(x) for x in digits]
    contains_logic = []
    for subject_id in [subject_id, subject_id.lower()]:
        contains_logic += [
            f"contains([{subject_id_var}], '{subject_id}_{x}')" for x in digits_str
        ]
        contains_logic += [
            f"contains([{subject_id_var}], '{subject_id}={x}')" for x in digits_str
        ]

    filter_logic += f" or {' or '.join(contains_logic)}"
    return filter_logic


def fetch_subject_data(
    redcap_data_source: RedcapDataSource,
    subject_id: str,
    config_file: Path,
    timeout_s: int = 60,
) -> Optional[bytes]:
    """
    Fetches data for a single subject from REDCap.

    Args:
        redcap_data_source (RedcapDataSource): The REDCap data source.
        subject_id (str): The subject ID to fetch data for.
        config_file (Path): Path to the config file.
        timeout_s (int): Timeout for the API request.

    Returns:
        Optional[bytes]: The raw data from REDCap, or None if fetching fails.
    """
    project_id = redcap_data_source.project_id
    site_id = redcap_data_source.site_id
    data_source_name = redcap_data_source.data_source_name

    redcap_endpoint_url = redcap_data_source.data_source_metadata.endpoint_url

    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"
    logger.info(f"Fetching data for {identifier}...")

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
            event="redcap_data_pull_keystore_missing",
            message=f"Keystore entry not found for {identifier}.",
            project_id=project_id,
            site_id=site_id,
            data_source_name=data_source_name,
            subject_id=subject_id,
        )
        return None

    api_token = keystore.key_value

    filter_logic = ""
    if redcap_data_source.data_source_metadata.subject_id_variable_as_the_pk:
        data = {
            "token": api_token,
            "content": "record",
            "action": "export",
            "format": "json",  # Changed from 'csv' to 'json'
            "type": "flat",
            "returnFormat": "json",
            "records[0]": subject_id,  # Export data for this specific subject
        }
    else:
        subject_id_var = redcap_data_source.data_source_metadata.subject_id_variable

        if redcap_data_source.data_source_metadata.messy_subject_id:
            filter_logic = add_filter_logic_for_penncnb_redcap(
                filter_logic, subject_id, subject_id_var  # type: ignore
            )

        data = {
            "token": api_token,
            "content": "record",
            "action": "export",
            "format": "json",  # Changed from 'csv' to 'json'
            "type": "flat",
            "returnFormat": "json",
            "csvDelimiter": "",
            "rawOrLabel": "raw",
            "rawOrLabelHeaders": "raw",
            "exportCheckboxLabel": "false",
            "exportSurveyFields": "false",
            "exportDataAccessGroups": "false",
            "filterLogic": filter_logic,  # Export data for this specific subject
        }

    try:
        r = requests.post(redcap_endpoint_url, data=data, timeout=timeout_s)
        r.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

        # Check if empty response
        if r.content in [b"", b"[]"]:
            log_event(
                config_file=config_file,
                log_level="WARN",
                event="redcap_data_pull_no_data",
                message=f"No data found for {identifier}.",
                project_id=project_id,
                site_id=site_id,
                data_source_name=data_source_name,
                subject_id=subject_id,
                extra={"filter_logic": filter_logic},
            )

            logger.warning(f"No data found for {identifier}")
            return None

        return r.content

    except requests.exceptions.RequestException as e:
        logger.error(f"filter_logic: {filter_logic}")
        logger.error(f"Failed to fetch data for {identifier}: {e}")
        log_event(
            config_file=config_file,
            log_level="ERROR",
            event="redcap_data_pull_fetch_failed",
            message=f"Failed to fetch data for {identifier}.",
            project_id=project_id,
            site_id=site_id,
            data_source_name=data_source_name,
            subject_id=subject_id,
            extra={"error": str(e)},
        )
        return None


def save_subject_data(
    data: bytes,
    project_id: str,
    site_id: str,
    subject_id: str,
    data_source_name: str,
    config_file: Path,
) -> Optional[tuple[Path, str]]:
    """
    Saves the fetched subject data to the file system and records it in the database.
    Uses the new path pattern for REDCap JSON files.
    """
    try:
        lochness_root: Path = config.parse(config_file, "general")["lochness_root"]  # type: ignore
        # Determine all REDCap data source instance names for this project+site
        # sql_query = f"""
        #     SELECT data_source_name FROM data_sources
        #     WHERE project_id = '{project_id}' AND
        #         site_id = '{site_id}' AND data_source_type = 'redcap'
        # """
        # df = db.execute_sql(config_file, sql_query)
        # instance_names = (
        #     sorted(df["data_source_name"].tolist())
        #     if not df.empty
        #     else [data_source_name]
        # )
        # The 'first' instance is the first in alphabetical order
        # is_first_redcap = data_source_name == instance_names[0]

        # Capitalize project name (first letter uppercase, rest lowercase)
        project_name_cap = (
            project_id[:1].upper() + project_id[1:].lower()
            if project_id
            else project_id
        )
        # Build output path
        output_dir = (
            Path(lochness_root)
            / project_name_cap
            / "PHOENIX"
            / "PROTECTED"
            / f"{project_name_cap}{site_id}"
            / "raw"
            / subject_id
            / "surveys"
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        file_name = f"{subject_id}.{project_name_cap}.{data_source_name}.json"
        file_path = output_dir / file_name
        with open(file_path, "wb") as f:
            f.write(data)
        # Record the file in the database
        file_model = File(
            file_path=file_path,
        )
        file_md5 = file_model.md5
        db.execute_queries(
            config_file, [file_model.to_sql_query()], show_commands=False
        )
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_data_pull_save_success",
            message=f"Successfully saved data for {subject_id} to {file_path}.",
            project_id=project_id,
            site_id=site_id,
            data_source_name=data_source_name,
            subject_id=subject_id,
            extra={
                "file_path": str(file_path),
                "file_md5": file_md5 if file_md5 else None,
            },
        )
        return file_path, file_md5 if file_md5 else ""
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"Failed to save data for {subject_id}: {e}")
        log_event(
            config_file=config_file,
            log_level="ERROR",
            event="redcap_data_pull_save_failed",
            message=f"Failed to save data for {subject_id}.",
            project_id=project_id,
            site_id=site_id,
            data_source_name=data_source_name,
            subject_id=subject_id,
            extra={"error": str(e)},
        )
        return None


def pull_all_data(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    subject_id_list: Optional[List[str]] = None,
):
    """
    Main function to pull data for all active REDCap data sources and subjects.
    """
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="redcap_data_pull_start",
        message="Starting REDCap data pull process.",
        project_id=project_id,
        site_id=site_id,
    )

    active_redcap_data_sources = RedcapDataSource.get_all_redcap_data_sources(
        config_file=config_file,
        active_only=True,
    )

    if project_id:
        active_redcap_data_sources = [
            ds for ds in active_redcap_data_sources if ds.project_id == project_id
        ]
    if site_id:
        active_redcap_data_sources = [
            ds for ds in active_redcap_data_sources if ds.site_id == site_id
        ]

    if not active_redcap_data_sources:
        logger.info("No active REDCap data sources found for data pull.")
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_data_pull_no_active_sources",
            message="No active REDCap data sources found for data pull.",
            project_id=project_id,
            site_id=site_id,
        )
        return

    logger.info(
        f"Found {len(active_redcap_data_sources)} active REDCap data sources for data pull."
    )
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="redcap_data_pull_active_sources_found",
        message=(
            "Found "
            + str(len(active_redcap_data_sources))
            + " active REDCap data sources for data pull."
        ),
        project_id=project_id,
        site_id=site_id,
        extra={"count": len(active_redcap_data_sources)},
    )

    for redcap_data_source in active_redcap_data_sources:
        # Get subjects for this data source
        # For simplicity, let's assume we pull data for all subjects associated with
        # this project/site
        # In a real scenario, you might filter for new subjects or subjects with updated metadata
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            config_file=config_file,
        )

        if subject_id_list:
            subjects_in_db = [
                x for x in subjects_in_db if x.subject_id in subject_id_list
            ]

        if not subjects_in_db:
            logger.info(  # pylint: disable=logging-not-lazy
                (
                    "No subjects found for "
                    + f"{redcap_data_source.project_id}"
                    + "::"
                    + f"{redcap_data_source.site_id}."
                )
            )
            log_event(
                config_file=config_file,
                log_level="INFO",
                event="redcap_data_pull_no_subjects",
                message=(
                    f"No subjects found for "
                    f"{redcap_data_source.project_id}::"
                    f"{redcap_data_source.site_id}."
                ),
                project_id=redcap_data_source.project_id,
                site_id=redcap_data_source.site_id,
                data_source_name=redcap_data_source.data_source_name,
            )
            continue

        logger.info(
            f"Found {len(subjects_in_db)} subjects for {redcap_data_source.data_source_name}."
        )
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_data_pull_subjects_found",
            message=(
                "Found "
                + str(len(subjects_in_db))
                + " subjects for "
                + str(redcap_data_source.data_source_name)
                + "."
            ),
            project_id=redcap_data_source.project_id,
            site_id=redcap_data_source.site_id,
            data_source_name=redcap_data_source.data_source_name,
            extra={"count": len(subjects_in_db)},
        )

        for subject in subjects_in_db:
            start_time = datetime.now()
            raw_data = fetch_subject_data(
                redcap_data_source=redcap_data_source,
                subject_id=subject.subject_id,
                config_file=config_file,
            )

            if raw_data:
                result = save_subject_data(
                    data=raw_data,
                    project_id=subject.project_id,
                    site_id=subject.site_id,
                    subject_id=subject.subject_id,
                    data_source_name=redcap_data_source.data_source_name,
                    config_file=config_file,
                )
                if result:
                    file_path, file_md5 = result
                    end_time = datetime.now()
                    pull_time_s = int((end_time - start_time).total_seconds())

                    data_pull = DataPull(
                        subject_id=subject.subject_id,
                        data_source_name=redcap_data_source.data_source_name,
                        site_id=subject.site_id,
                        project_id=subject.project_id,
                        file_path=str(file_path),
                        file_md5=file_md5,
                        pull_time_s=pull_time_s,
                        pull_metadata={
                            "redcap_endpoint": redcap_data_source.data_source_metadata.endpoint_url,
                            "records_pulled_bytes": len(raw_data),
                        },
                    )
                    db.execute_queries(
                        config_file, [data_pull.to_sql_query()], show_commands=False
                    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull REDCap data for all or specific project/site."
    )
    parser.add_argument(
        "--project_id",
        "-p",
        type=str,
        default=None,
        help="Project ID to pull data for (optional)",
    )
    parser.add_argument(
        "--site_id",
        "-s",
        type=str,
        default=None,
        help="Site ID to pull data for (optional)",
    )
    parser.add_argument(
        "--subject_id_list",
        "-l",
        type=str,
        nargs="+",
        default=None,
        help=(
            "List of subject IDs to pull data for (optional), "
            "e.g. --subject_id_list sub001 sub002 sub003"
        ),
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

    project_id: Optional[str] = args.project_id
    site_id: Optional[str] = args.site_id
    subject_id_list: Optional[List[str]] = args.subject_id_list

    logger.info(  # pylint: disable=logging-not-lazy
        "Pulling REDCap data for project_id="
        + str(project_id)
        + ", site_id="
        + str(site_id)
        + ", subject_id_list="
        + str(subject_id_list)
    )

    logger.info("Starting REDCap data pull...")
    pull_all_data(
        config_file=config_file,
        project_id=project_id,
        site_id=site_id,
        subject_id_list=subject_id_list,
    )

    logger.info("Finished REDCap data pull.")
