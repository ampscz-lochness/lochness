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
import json
import logging
from typing import Any, List, Dict, Optional, Tuple
from datetime import datetime
import tempfile

import requests
from rich.logging import RichHandler

from lochness.helpers import logs, utils, db, config, timer, fs
from lochness.sources.redcap import api as redcap_api
from lochness.sources.redcap import utils as redcap_utils
from lochness.models.subjects import Subject
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.models.files import File
from lochness.models.data_pulls import DataPull
from lochness.sources.redcap.models.data_source import RedcapDataSource

MODULE_NAME = "lochness.sources.redcap.tasks.pull_data"
NOISY_MODULES = ["urllib3.connectionpool"]

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


def add_filter_logic_for_penncnb_redcap(subject_id: str, subject_id_var: str):
    """
    Enhances the existing filter logic for fetching data from REDCap by adding
    conditions to handle subject IDs with various suffix patterns used in the
    PennCNB REDCap project.

    This function appends additional logic to the provided filter logic string
    to accommodate different possible suffixes for subject IDs. These suffixes
    are used in REDCap to denote different sessions or versions of a subject's
    data.

    Args:
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
    redact_identifiers: bool = True,
    timeout_s: int = 60,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[List[Dict[str, Any]]]]:
    """
    Fetches data and audit log for a single subject from REDCap.

    Args:
        redcap_data_source (RedcapDataSource): The REDCap data source.
        subject_id (str): The subject ID to fetch data for.
        config_file (Path): Path to the config file.
        redact_identifiers (bool): Whether to redact identifiers from the fetched data.
        timeout_s (int): Timeout for the API request.

    Returns:
        Tuple[Optional[List[Dict[str, Any]]], Optional[List[Dict[str, Any]]]]:
            The raw data and audit log from REDCap
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
        return None, None

    api_token = keystore.key_value

    filter_logic: Optional[str] = None
    records_list: Optional[List[str]] = None

    if redcap_data_source.data_source_metadata.subject_id_variable_as_the_pk:
        records_list = [subject_id]
    else:
        subject_id_var = redcap_data_source.data_source_metadata.subject_id_variable

        if redcap_data_source.data_source_metadata.messy_subject_id:
            filter_logic = add_filter_logic_for_penncnb_redcap(
                subject_id,
                subject_id_var,  # type: ignore
            )

    try:
        data_result = redcap_api.export_records(
            api_token=api_token,
            endpoint_url=redcap_endpoint_url,
            filter_logic=filter_logic,
            records=records_list,
            timeout_s=timeout_s,
        )
        logs_result = redcap_api.export_log(
            api_token=api_token,
            endpoint_url=redcap_endpoint_url,
            record_id=subject_id,
            timeout_s=timeout_s,
        )
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
        return None, None

    if data_result is None:
        log_event(
            config_file=config_file,
            log_level="WARN",
            event="redcap_data_pull_no_data",
            message=f"No data found for {identifier}.",
            project_id=project_id,
            site_id=site_id,
            data_source_name=data_source_name,
            subject_id=subject_id,
            extra={"filter_logic": filter_logic or ""},
        )

        logger.warning(f"No data found for {identifier}")
        return None, logs_result

    # Redact identifers
    if redact_identifiers:
        identifier_fields = redcap_utils.get_identifier_fields_from_data_source(
            redcap_data_source
        )
        data_result = redcap_utils.redact_identifiers(data_result, identifier_fields)

    return data_result, logs_result


def check_file_content_unchanged(
    file_path: Path,
    file_md5: str,
    file_content: bytes,
    replace_existing: bool = False,
) -> Optional[File]:
    """
    Check if provided file content matches provided MD5 hash and
    optionally replace existing file if content has changed.

    Args:
        file_path (Path): The path to the existing file.
        file_content (bytes): The new file content to compare.
        file_md5 (str): The MD5 hash of the new file content.

    Returns:
        bool: True if the content is unchanged, False otherwise.
    """
    temp_file_path: Optional[Path] = None
    with tempfile.NamedTemporaryFile(delete=False) as tmp_f:
        tmp_f.write(file_content)
        temp_file_path = Path(tmp_f.name)

    temp_file_model = File(file_path=temp_file_path)
    temp_file_md5 = temp_file_model.md5

    if temp_file_md5 == file_md5:
        # Content is unchanged – discard temp and skip
        fs.remove(temp_file_path)
        return None
    elif replace_existing:
        fs.copy(source=temp_file_path, destination=file_path)
        return temp_file_model
    else:
        # Content has changed and not replacing the existing file
        return temp_file_model


def save_subject_data(
    data: Optional[List[Dict[str, Any]]],
    log: Optional[List[Dict[str, Any]]],
    project_id: str,
    site_id: str,
    subject_id: str,
    data_source_name: str,
    config_file: Path,
) -> List[Tuple[File, str]]:
    """
    Saves the fetched subject data to the file system and records it in the database.

    Args:
        data (List[Dict[str, Any]]): The data to be saved.
        log (List[Dict[str, Any]]): The log to be saved.
        project_id (str): The project ID.
        site_id (str): The site ID.
        subject_id (str): The subject ID.
        data_source_name (str): The name of the data source.
        config_file (Path): Path to the config file.

    Returns:
        List[Tuple[File, str]]: A list of tuples containing the File model and
            its type (e.g., "data" or "log") for each file that was created or updated.
    """
    created_files: List[Tuple[File, str]] = []
    temp_path: Optional[Path] = None
    try:
        lochness_root: Path = config.parse(config_file, "general")["lochness_root"]  # type: ignore

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

        # Data File Handling
        if data:
            data_file_name = f"{subject_id}.{project_name_cap}.{data_source_name}.json"
            data_file_path = output_dir / data_file_name

            existing_data_file = File.get_most_recent_file_obj(
                config_file=config_file, file_path=data_file_path
            )
            existing_data_md5 = (
                existing_data_file.md5 if existing_data_file is not None else None
            )
            data_file_model = check_file_content_unchanged(
                file_path=data_file_path,
                file_md5="" if existing_data_md5 is None else existing_data_md5,
                file_content=json.dumps(data).encode("utf-8"),
                replace_existing=True,
            )
            if data_file_model is None:
                logger.info(f"Data file unchanged for {subject_id}, skipping save.")
                log_event(
                    config_file=config_file,
                    log_level="INFO",
                    event="redcap_data_pull_data_unchanged",
                    message=f"Data file unchanged for {subject_id}, skipping save.",
                    project_id=project_id,
                    site_id=site_id,
                    data_source_name=data_source_name,
                    subject_id=subject_id,
                    extra={
                        "file_path": str(data_file_path),
                        "file_md5": existing_data_md5,
                    },
                )
            else:
                created_files.append((data_file_model, "data"))

        # Log File Handling
        if log:
            log_file_name = (
                f"{subject_id}.{project_name_cap}.{data_source_name}.log.json"
            )
            log_file_path = output_dir / log_file_name

            existing_log_file = File.get_most_recent_file_obj(
                config_file=config_file, file_path=log_file_path
            )
            existing_log_md5 = (
                existing_log_file.md5 if existing_log_file is not None else None
            )
            log_file_model = check_file_content_unchanged(
                file_path=log_file_path,
                file_md5="" if existing_log_md5 is None else existing_log_md5,
                file_content=json.dumps(log).encode("utf-8"),
                replace_existing=True,
            )
            if log_file_model is None:
                logger.info(f"Log file unchanged for {subject_id}, skipping save.")
                log_event(
                    config_file=config_file,
                    log_level="INFO",
                    event="redcap_data_pull_log_unchanged",
                    message=f"Log file unchanged for {subject_id}, skipping save.",
                    project_id=project_id,
                    site_id=site_id,
                    data_source_name=data_source_name,
                    subject_id=subject_id,
                    extra={
                        "file_path": str(log_file_path),
                        "file_md5": existing_log_md5,
                    },
                )
            else:
                created_files.append((log_file_model, "log"))

        # write to DB
        queries = []
        for file_model, _ in created_files:
            queries += file_model.to_sql_queries_with_availability_update()
        db.execute_queries(
            config_file,
            queries,
            show_commands=False,
        )
    except Exception as e:  # pylint: disable=broad-except
        logger.error(f"Failed to save data for {subject_id}: {e}")
        if temp_path is not None and temp_path.exists():
            temp_path.unlink(missing_ok=True)
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

    return created_files


def pull_file_attachments(
    redcap_data_source: RedcapDataSource,
    subject_id: str,
    subject_data_json: Path,
    file_fields: Dict[str, str],
    config_file: Path,
) -> int:
    """
    Parses the pulled JSON data, identifies file upload fields with values,
    downloads the actual files from REDCap, and saves them under
    surveys/assets/{data_source_name}/{event}/{form}/{filename}.

    If the repeat instance is not 1 (or missing), ``_{instance}`` is appended
    to the stem of the downloaded filename.

    Args:
        redcap_data_source (RedcapDataSource): The REDCap data source.
        subject_id (str): The subject ID.
        raw_data (bytes): The raw JSON data previously fetched for this subject.
        file_fields (Dict[str, str]): Mapping of field_name: form_name for
            every file upload field.
        config_file (Path): Path to the config file.

    Returns:
        int: The number of files successfully downloaded.
    """
    project_id = redcap_data_source.project_id
    site_id = redcap_data_source.site_id
    data_source_name = redcap_data_source.data_source_name
    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"

    # Parse JSON data
    try:
        with open(subject_data_json, "r", encoding="utf-8") as f:
            records = json.load(f)
    except (json.JSONDecodeError, ValueError, FileNotFoundError) as e:
        logger.error(
            f"Failed to parse JSON data for file attachments: {identifier}, error: {e}"
        )
        return 0

    if not records:
        return 0

    keystore = KeyStore.retrieve_keystore(
        redcap_data_source.data_source_metadata.keystore_name,
        project_id,
        config_file,
    )
    if keystore is None:
        logger.error(f"Keystore not found for file attachment download: {identifier}")
        return 0

    api_token = keystore.key_value
    endpoint_url = redcap_data_source.data_source_metadata.endpoint_url

    # Build output base path
    lochness_root: Path = config.parse(config_file, "general")["lochness_root"]  # type: ignore
    project_name_cap = (
        project_id[:1].upper() + project_id[1:].lower() if project_id else project_id
    )
    assets_base = (
        Path(lochness_root)
        / project_name_cap
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{site_id}"
        / "raw"
        / subject_id
        / "surveys"
        / "assets"
        / data_source_name
    )

    files_downloaded = 0

    for record in records:
        # Determine the record_id for the API call
        if redcap_data_source.data_source_metadata.subject_id_variable_as_the_pk:
            record_id = subject_id
        else:
            # The first field in the record is always the record_id in REDCap
            first_field = list(record.keys())[0]
            record_id = str(record.get(first_field, subject_id))

        event_name = record.get("redcap_event_name")
        repeat_instance = record.get("redcap_repeat_instance")

        for field_name, form_name in file_fields.items():
            file_value = record.get(field_name, "")
            if not file_value:
                continue

            # Download the file
            with timer.Timer() as file_pull_timer:
                result = redcap_api.export_file(
                    endpoint_url=endpoint_url,
                    api_token=api_token,
                    record_id=str(record_id),
                    field_name=field_name,
                    event_name=event_name,
                    repeat_instance=(str(repeat_instance) if repeat_instance else None),
                )

            if result is None:
                continue

            file_content, original_filename = result

            # Build path: .../assets/{data_source_name}/{event}/{form}/{file}
            output_dir = assets_base
            if event_name:
                output_dir = output_dir / event_name
            output_dir = output_dir / form_name

            # Append _{instance} to the filename when instance is not 1
            fname = Path(original_filename)
            if (
                repeat_instance is not None
                and str(repeat_instance) != ""
                and str(repeat_instance) != "1"
            ):
                original_filename = f"{fname.stem}_{repeat_instance}{fname.suffix}"

            output_dir.mkdir(parents=True, exist_ok=True)
            file_path = output_dir / original_filename

            # Write to a temp file first so we can hash before committing
            with tempfile.NamedTemporaryFile(
                suffix=Path(original_filename).suffix,
                delete=False,
                dir=output_dir,
            ) as tmp_att_f:
                temp_att_path = Path(tmp_att_f.name)
                tmp_att_f.write(file_content)

            # Compute hash of the temp file
            temp_att_model = File(file_path=temp_att_path)
            new_att_md5 = temp_att_model.md5 or ""

            # Check DB for the most recent hash recorded at this file_path
            existing_att_file = File.get_most_recent_file_obj(
                config_file=config_file, file_path=file_path
            )
            existing_att_md5 = (
                existing_att_file.md5 if existing_att_file is not None else None
            )

            if existing_att_md5 is not None and existing_att_md5 == new_att_md5:
                # Attachment unchanged – discard temp and skip
                temp_att_path.unlink(missing_ok=True)
                logger.debug(f"File attachment unchanged, skipping: {file_path}")
                continue
            else:
                logger.info(
                    f"File attachment is new or changed for {subject_id}, saving to {file_path}."
                )

            # New or changed – move temp to the actual path
            fs.copy(
                source=temp_att_path,
                destination=file_path,
            )
            fs.remove(temp_att_path)

            # Track file in DB
            file_model = File.new(
                file_path=file_path,
                file_size_mb=temp_att_model.file_size_mb,
                m_time=datetime.fromtimestamp(file_path.stat().st_mtime),
                md5=new_att_md5,
                file_metadata={"available_at": [f"hn:{utils.get_hostname()}"]},
            )
            file_md5 = file_model.md5 or ""

            # Record the data pull in DB
            data_pull = DataPull(
                subject_id=subject_id,
                data_source_name=data_source_name,
                site_id=site_id,
                project_id=project_id,
                file_path=str(file_path),
                file_md5=file_md5,
                pull_time_s=int(file_pull_timer.duration),  # type: ignore
                pull_metadata={
                    "redcap_endpoint": endpoint_url,
                    "field_name": field_name,
                    "form_name": form_name,
                    "event_name": event_name,
                    "repeat_instance": (
                        str(repeat_instance) if repeat_instance else None
                    ),
                    "record_id": str(record_id),
                    "file_size_bytes": len(file_content),
                    "type": "file_attachment",
                    "relative_path": str(file_path.relative_to(lochness_root)),
                },
            )

            queries = file_model.to_sql_queries_with_availability_update() + [
                data_pull.to_sql_query()
            ]
            db.execute_queries(
                config_file,
                queries,
                show_commands=False,
            )

            files_downloaded += 1
            logger.info(f"Downloaded file attachment: {file_path}")

    if files_downloaded > 0:
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="redcap_file_attachments_downloaded",
            message=(
                f"Downloaded {files_downloaded} file attachment(s) for {identifier}."
            ),
            project_id=project_id,
            site_id=site_id,
            data_source_name=data_source_name,
            subject_id=subject_id,
            extra={"files_downloaded": files_downloaded},
        )

    return files_downloaded


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
        file_fields = redcap_utils.get_file_fields_from_dictionary(redcap_data_source)
        if file_fields:
            logger.info(
                f"Found {len(file_fields)} file upload field(s) for "
                f"{redcap_data_source.data_source_name}: "
                f"{list(file_fields.keys())}"
            )

        # Get subjects for this data source
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
            with timer.Timer() as pull_timer:
                raw_data, log_data = fetch_subject_data(
                    redcap_data_source=redcap_data_source,
                    subject_id=subject.subject_id,
                    config_file=config_file,
                )

            new_files: List[Tuple[File, str]] = save_subject_data(
                data=raw_data,
                log=log_data,
                project_id=subject.project_id,
                site_id=subject.site_id,
                subject_id=subject.subject_id,
                data_source_name=redcap_data_source.data_source_name,
                config_file=config_file,
            )

            data_pulls: List[DataPull] = []
            for file_model, file_type in new_files:
                if file_type == "data":
                    associated_data = raw_data
                elif file_type == "log":
                    associated_data = log_data
                else:
                    raise ValueError(f"Unexpected file type: {file_type}")
                file_path = file_model.file_path
                file_md5 = file_model.md5 or ""

                lochness_root_path: Path = Path(
                    config.parse(config_file, "general")["lochness_root"]  # type: ignore
                )
                pull_metadata = {
                    "redcap_endpoint": redcap_data_source.data_source_metadata.endpoint_url,
                    "records_pulled_bytes": len(associated_data),  # type: ignore
                    "type": file_type,
                    "relative_path": str(file_path.relative_to(lochness_root_path)),
                }
                if file_type == "data":
                    # Pull file attachments if any file upload fields exist
                    if file_fields:
                        files_downloaded_count = pull_file_attachments(
                            redcap_data_source=redcap_data_source,
                            subject_id=subject.subject_id,
                            subject_data_json=file_path,
                            file_fields=file_fields,
                            config_file=config_file,
                        )
                        pull_metadata["file_attachments_count"] = files_downloaded_count

                data_pull = DataPull(
                    subject_id=subject.subject_id,
                    data_source_name=redcap_data_source.data_source_name,
                    site_id=subject.site_id,
                    project_id=subject.project_id,
                    file_path=str(file_path),
                    file_md5=file_md5,
                    pull_time_s=int(pull_timer.duration),  # type: ignore
                    pull_metadata=pull_metadata,
                )
                data_pulls.append(data_pull)

            # Insert data pulls into DB
            db.execute_queries(
                config_file=config_file,
                queries=[dp.to_sql_query() for dp in data_pulls],
                show_commands=False,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull REDCap data for all or specific project/site."
    )
    parser.add_argument(
        "--project_id",
        type=str,
        default=None,
        help="Project ID to pull data for (optional)",
    )
    parser.add_argument(
        "--site_id", type=str, default=None, help="Site ID to pull data for (optional)"
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    logger.info(f"Using config file: {config_file}")
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file,
        module_name=MODULE_NAME,
        logger=logger,
        noisy_modules=NOISY_MODULES,
    )

    logger.info("Starting REDCap data pull...")
    pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
    )

    logger.info("Finished REDCap data pull.")
