#!/usr/bin/env python
"""
Pulls data from SharePoint for active data sources and subjects.

This script is intended to be run as a cron job.
It will pull data for all active SharePoint data sources and their associated subjects.
"""

import sys
from pathlib import Path, PurePosixPath

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))

import argparse
import json
import logging
from typing import Any, Dict, List, Optional

from rich.logging import RichHandler

from lochness.helpers import config, logs, utils
from lochness.models.keystore import KeyStore
from lochness.models.subjects import Subject
from lochness.sources.sharepoint import api as sharepoint_api
from lochness.sources.sharepoint import utils as sharepoint_utils
from lochness.sources.sharepoint.models.data_source import SharepointDataSource

MODULE_NAME = "lochness.sources.sharepoint.tasks.pull_data"
NOISY_MODULES = [
    "msal.application",
    "msal.authority",
    "msal.telemetry",
    "msal.token_cache",
    "urllib3.connectionpool",
    "urllib3.util.retry",
]

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

# Set up logger
logging.basicConfig(**logargs)
logger = logging.getLogger(MODULE_NAME)


def fetch_subject_data(
    sharepoint_data_source: SharepointDataSource,
    subject_id: str,
    config_file: Path,
    hash_only: bool = False
) -> None:
    """
    Fetches data for a single subject from SharePoint.

    Args:
        sharepoint_data_source: SharepointDataSource object.
        subject_id (str): The subject ID to fetch data for.
        hash_only (bool): If True, only update hashes of existing local files without downloading new files.

    Returns:
        None
    """
    project_id = sharepoint_data_source.project_id
    site_id = sharepoint_data_source.site_id
    data_source_name = sharepoint_data_source.data_source_name

    metadata = sharepoint_data_source.data_source_metadata
    form_name = metadata.form_name
    form_title = metadata.form_title
    modality = getattr(metadata, "modality", "unknown")
    date_str = getattr(metadata, "date_str", "unknown")
    subdirs_under_each_subject_dir = getattr(
            metadata, "subdirs_under_each_subject_dir", False)
    without_form = getattr(metadata, "without_form", False)
    potential_file_uploads_without_form_update = getattr(
            metadata, "potential_file_uploads_without_form_update", False)

    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"
    logger.debug("Fetching data for %s", identifier)

    # keystore
    keystore = KeyStore.retrieve_keystore(
        metadata.keystore_name, project_id, config_file=config_file
    )
    if not keystore:
        logger.error(f"Keystore '{metadata.keystore_name}' not found for {identifier}.")
        raise RuntimeError(
            f"Keystore '{metadata.keystore_name}' not found for {identifier}."
        )

    # Authenticate the sharepoint application
    # For device flow use client_secret=None
    sharepoint_cred_dict = json.loads(keystore.key_value)
    headers = sharepoint_api.get_auth_headers(
        client_id=sharepoint_cred_dict["client_id"],
        tenant_id=sharepoint_cred_dict["tenant_id"],
        client_secret=sharepoint_cred_dict["client_secret"],
    )

    # Get site ID
    sharepoint_site_id = sharepoint_api.get_site_id(headers, metadata.site_url)

    drives = sharepoint_api.get_drives(sharepoint_site_id, headers)
    data_drive = sharepoint_utils.find_drive_by_name(drives, metadata.drive_name)
    if not data_drive:
        raise RuntimeError(f"`{metadata.drive_name}` drive not found.")
    drive_id = data_drive["id"]

    # Build output path
    project_name_cap = (
        project_id[:1].upper() + project_id[1:].lower() if project_id else project_id
    )

    has_project_folder: Optional[bool] = True

    if metadata.has_project_folder:
        logger.debug(f"Looking for project folder '{project_name_cap}' in drive '{metadata.drive_name}'...")
        project_folder = sharepoint_utils.find_folder_in_drive(
            drive_id, project_name_cap, headers
        )
        if not project_folder:
            raise RuntimeError(
                f"Project folder `{project_name_cap}` not found in `{metadata.drive_name}` drive."
            )

        site_folder = sharepoint_utils.find_subfolder(
            drive_id, project_folder["id"], f"{project_name_cap}{site_id}", headers
        )
    else:
        project_folder = sharepoint_utils.find_folder_in_drive(
            drive_id, project_name_cap, headers
        )
        logger.debug(f"Not expecting project folder under sharepoint")
        site_folder = sharepoint_utils.find_folder_in_drive(
            drive_id, f"{project_name_cap}{site_id}", headers
        )

    if not site_folder:
        raise RuntimeError(
            f"Site folder {project_name_cap}{site_id} not found in `{project_name_cap}` folder."
        )

    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore
    include_project_dir_in_output = project_id.lower() != "pronet"

    output_dir = Path(lochness_root)
    if include_project_dir_in_output:
        output_dir = output_dir / project_name_cap

    output_dir = (
        output_dir
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{site_id}"
        / "raw"
        / subject_id
        / modality
    )


    if len([part for part in PurePosixPath(form_name).parts if part not in ("", "/")]) > 1:
        logger.warning(
            f"Form name '{form_name}' contains subdirectories. "
            "Subdirectory handling is not currently implemented, so form_name should be a single folder name. "
            "Proceeding with form_name as provided, but this may lead to issues if subdirectories are expected."
        )

        logger.info("Resolving nested SharePoint form path '%s'.", form_name)
        matched_form_folders = sharepoint_utils.find_matching_dirs_with_path(
            drive_id, site_folder, form_name, headers
        )

        subject_folders = []
        for form_folder in matched_form_folders:
            subject_folders.extend(
                sharepoint_utils.get_child_folders(
                    drive_id, form_folder, headers, prefix=subject_id
                )
            )
    else:
        subject_folders = sharepoint_utils.get_matching_subfolders(
            drive_id, site_folder, form_name, headers
        )

    for subject_folder in subject_folders:
        logger.debug(f"Checking subject folder: {subject_folder['name']} for subject_id: {subject_id}")
        if subject_folder["name"] == subject_id:
            logger.info(f"Found corresponding subfolder for {subject_id}")

            if subdirs_under_each_subject_dir:
                session_folders = sharepoint_utils.get_matching_subfolders(
                    drive_id,
                    subject_folder,
                    subject_id,
                    headers,
                    relaxed_search=True,
                )
            else:
                session_folders = [subject_folder]

            if session_folders is None:
                continue
                
            for session_folder in session_folders:
                logger.debug(f"Processing session folder: {session_folder['name']} for subject_id: {subject_id}")
                sharepoint_utils.download_new_or_updated_files(
                    session_folder,
                    drive_id,
                    headers,
                    form_title,
                    date_str,
                    subject_id,
                    site_id,
                    project_id,
                    data_source_name,
                    output_dir,
                    potential_file_uploads_without_form_update,
                    without_form=without_form,
                    config_file=config_file,
                    hash_only=hash_only
                )


def pull_all_data(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    subject_id_list: Optional[List[str]] = None,
    source_id: Optional[str] = None,
    hash_only: bool = False,
):
    """
    Main function to pull data for all SharePoint data sources and subjects.
    """

    sharepoint_utils.log_event(
        config_file=config_file,
        log_level="INFO",
        event="sharepoint_data_pull_start",
        message="Starting SharePoint data pull process.",
        project_id=project_id,
        site_id=site_id,
    )

    active_sharepoint_data_sources = (
        SharepointDataSource.get_all_sharepoint_data_sources(
            config_file=config_file, active_only=True
        )
    )

    if project_id:
        active_sharepoint_data_sources = [
            ds for ds in active_sharepoint_data_sources if ds.project_id == project_id
        ]
    if site_id:
        active_sharepoint_data_sources = [
            ds for ds in active_sharepoint_data_sources if ds.site_id == site_id
        ]
    if source_id:
        active_sharepoint_data_sources = [
            ds
            for ds in active_sharepoint_data_sources
            if ds.data_source_name == source_id
        ]

    if not active_sharepoint_data_sources:
        msg = "No active SharePoint data sources found for data pull."
        logger.info(msg)
        sharepoint_utils.log_event(
            config_file=config_file,
            log_level="INFO",
            event="sharepoint_data_pull_no_active_sources",
            message=msg,
            project_id=project_id,
            site_id=site_id,
        )
        return

    msg = (
        "Found "
        + str(len(active_sharepoint_data_sources))
        + " active REDCap data sources for data pull."
    )

    logger.info(msg)
    sharepoint_utils.log_event(
        config_file=config_file,
        log_level="INFO",
        event="sharepoint_data_pull_active_sources_found",
        message=msg,
        project_id=project_id,
        site_id=site_id,
        extra={"count": len(active_sharepoint_data_sources)},
    )

    if hash_only:
        msg = "Running in hash-only mode. Will update hashes of existing " \
              "local files without downloading new files."
        logger.info(msg)
        sharepoint_utils.log_event(
            config_file=config_file,
            log_level="INFO",
            event="sharepoint_data_pull_hash_only_mode",
            message=msg,
            project_id=project_id,
            site_id=site_id,
        )

    for sharepoint_data_source in active_sharepoint_data_sources:
        # Get subjects for this data source
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=sharepoint_data_source.project_id,
            site_id=sharepoint_data_source.site_id,
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
                    + f"{sharepoint_data_source.project_id}"
                    + "::"
                    + f"{sharepoint_data_source.site_id}."
                )
            )
            sharepoint_utils.log_event(
                config_file=config_file,
                log_level="INFO",
                event="sharepoint_data_pull_no_subjects",
                message=(
                    f"No subjects found for "
                    f"{sharepoint_data_source.project_id}::"
                    f"{sharepoint_data_source.site_id}."
                ),
                project_id=sharepoint_data_source.project_id,
                site_id=sharepoint_data_source.site_id,
                data_source_name=sharepoint_data_source.data_source_name,
            )
            continue

        msg = (
            f"Found {len(subjects_in_db)} subjects for "
            f"{sharepoint_data_source.data_source_name}."
        )
        logger.info(msg)
        sharepoint_utils.log_event(
            config_file=config_file,
            log_level="INFO",
            event="sharepoint_data_pull_subjects_found",
            message=msg,
            project_id=sharepoint_data_source.project_id,
            site_id=sharepoint_data_source.site_id,
            data_source_name=sharepoint_data_source.data_source_name,
            extra={"count": len(subjects_in_db)},
        )

        try:
            for subject in subjects_in_db:
                fetch_subject_data(
                    sharepoint_data_source=sharepoint_data_source,
                    subject_id=subject.subject_id,
                    config_file=config_file,
                    hash_only=hash_only
                )
        except RuntimeError as e:
            logger.error(
                f"Error fetching data for {sharepoint_data_source.data_source_name}: {str(e)}"
            )
            error_message = (
                f"Error fetching data for {sharepoint_data_source.data_source_name}: "
                "Potentially an issue with missing folders or authentication."
            )
            sharepoint_utils.log_event(
                config_file=config_file,
                log_level="ERROR",
                event="sharepoint_data_pull_subject_fetch_error",
                message=error_message,
                project_id=sharepoint_data_source.project_id,
                site_id=sharepoint_data_source.site_id,
                data_source_name=sharepoint_data_source.data_source_name,
                extra={"error_message": str(e)},
            )

    sharepoint_utils.log_event(
        config_file=config_file,
        log_level="INFO",
        event="sharepoint_data_pull_complete",
        message="Finished SharePoint data pull process.",
        project_id=project_id,
        site_id=site_id,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull SharePoint data for all or specific project/site."
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
    parser.add_argument(
        "--source_id",
        type=str,
        default=None,
        help="Sharepoint source ID to pull data for (optional)",
    )
    parser.add_argument(
        "--hash_only",
        action="store_true",
        help="Update hash of local files without downloading new files (optional)",
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

    logger.info("Starting SharePoint data pull...")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sharepoint_utils.log_event(
            config_file=config_file,
            log_level="FATAL",
            event="sharepoint_data_pull_config_missing",
            message=f"Config file does not exist: {config_file}",
            extra={"config_file_path": str(config_file)},
        )
        sys.exit(1)

    pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
        source_id=args.source_id,
        hash_only=args.hash_only
    )

    logger.info("Finished SharePoint data pull.")
