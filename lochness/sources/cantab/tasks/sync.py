#!/usr/bin/env python
"""
CANTAB Subject ID Fetcher

This script links study subjects IDs to their CANTAB IDs by querying the CANTAB API.
It stores the CANTAB IDs in the subject_metadata column for later use.
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

from rich.logging import RichHandler

from lochness.helpers import db
from lochness.models.subjects import Subject
from lochness.models.logs import Logs
from lochness.sources.cantab import api as cantab_api
from lochness.sources.cantab.models.data_source import CANTABDataSource
from lochness.helpers import utils

MODULE_NAME = "lochness.sources.cantab.tasks.sync"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_subjects_pending_cantab_link(
    config_file: Path,
    data_source: CANTABDataSource,
) -> List[Subject]:
    """
    Get subjects that do not have a CANTAB ID linked in their metadata.

    Args:
        config_file (Path): Path to the configuration file.
        project_id (str): The project ID.
        site_id (str): The site ID.

    Returns:
        List[Subject]: List of subjects pending CANTAB ID linking.
    """
    subjects_in_db = Subject.get_subjects_for_project_site(
        project_id=data_source.project_id,
        site_id=data_source.site_id,
        config_file=config_file,
    )

    subjects_pending_link: List[Subject] = []

    for subject in subjects_in_db:
        subject_metadata = subject.subject_metadata or {}
        if "cantab" not in subject_metadata:
            subjects_pending_link.append(subject)
        else:
            cantab_metadata: Dict[str, Any] = subject_metadata.get("cantab", {})
            if data_source.data_source_name not in cantab_metadata:
                subjects_pending_link.append(subject)
            else:
                data_source_metadata: Dict[str, Any] = cantab_metadata.get(
                    data_source.data_source_name, {}
                )
                cantab_id = data_source_metadata.get("cantab_id")
                logger.debug(
                    f"Subject {subject.subject_id} already has CANTAB ID: {cantab_id}"
                )

    logger.debug(
        (
            f"{data_source.data_source_name}: Found "
            f"{len(subjects_pending_link)} subjects "
            f"pending CANTAB ID linking"
        )
    )
    return subjects_pending_link


def add_cantab_id_to_subject_metadata(
    subject: Subject,
    cantab_data_source: CANTABDataSource,
    cantab_id: str,
    config_file: Path,
) -> None:
    """
    Add the CANTAB ID to the subject's metadata and update the database.

    Args:
        subject (Subject): The subject to update.
        cantab_data_source (CANTABDataSource): The CANTAB data source.
        cantab_id (str): The CANTAB ID to add.
        config_file (Path): Path to the configuration file.

    Returns:
        None
    """
    if not cantab_id:
        logger.warning(
            f"No CANTAB ID found for subject {subject.subject_id}, skipping update."
        )
        return

    subject_metadata = subject.subject_metadata or {}
    cantab_metadata: Dict[str, Any] = subject_metadata.get("cantab", {})
    data_source_metadata: Dict[str, Any] = cantab_metadata.get(
        cantab_data_source.data_source_name, {}
    )

    data_source_metadata["cantab_id"] = cantab_id
    cantab_metadata[cantab_data_source.data_source_name] = data_source_metadata
    subject_metadata["cantab"] = cantab_metadata
    subject.subject_metadata = subject_metadata

    queries: List[str] = [subject.to_sql_query()]

    db.execute_queries(
        config_file=config_file,
        queries=queries,
    )
    logger.info(
        f"Linked CANTAB ID {cantab_id} to subject {subject.subject_id} "
        f"in project {subject.project_id}, site {subject.site_id}"
    )


def link_cantab_subject_id(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
) -> None:
    """
    Link CANTAB IDs to subjects in the database.

    Args:
        config_file (Path): Path to the configuration file.
        project_id (Optional[str]): Project ID to filter by.
        site_id (Optional[str]): Site ID to filter by.

    Returns:
        None
    """
    Logs(
        log_level="INFO",
        log_message={
            "event": "link_cantab_subject_id_start",
            "message": "Starting CANTAB subject ID linking process",
            "project_id": project_id,
            "site_id": site_id,
        },
    ).insert(config_file)

    active_cantab_data_sources = CANTABDataSource.get_all_cantab_data_sources(
        config_file=config_file,
        active_only=True,
    )

    if project_id:
        active_cantab_data_sources = [
            ds for ds in active_cantab_data_sources if ds.project_id == project_id
        ]
    if site_id:
        active_cantab_data_sources = [
            ds for ds in active_cantab_data_sources if ds.site_id == site_id
        ]

    if not active_cantab_data_sources:
        logger.warning("No active CANTAB data sources found.")
        Logs(
            log_level="WARN",
            log_message={
                "event": "link_cantab_subject_id_no_data_sources",
                "message": "No active CANTAB data sources found",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return

    logger.info(
        f"Found {len(active_cantab_data_sources)} active CANTAB data source(s)."
    )

    for cantab_data_source in active_cantab_data_sources:
        logger.info(
            f"Processing CANTAB data source: {cantab_data_source.data_source_name} "
            f"(Project: {cantab_data_source.project_id}, Site: {cantab_data_source.site_id})"
        )
        subjects_pending_cantab_link = get_subjects_pending_cantab_link(
            config_file=config_file,
            data_source=cantab_data_source,
        )

        if not subjects_pending_cantab_link:
            logger.info(
                "No subjects pending CANTAB ID linking for data source "
                f"{cantab_data_source.data_source_name}"
            )
            continue

        linked_subjects: List[Subject] = []
        for subject in subjects_pending_cantab_link:
            cantab_id = cantab_api.fetch_cantab_id(
                cantab_data_source=cantab_data_source,
                subject_id=subject.subject_id,
                config_file=config_file,
            )
            if cantab_id:
                add_cantab_id_to_subject_metadata(
                    subject=subject,
                    cantab_data_source=cantab_data_source,
                    cantab_id=cantab_id,
                    config_file=config_file,
                )
                linked_subjects.append(subject)
            else:
                logger.warning(
                    f"No CANTAB ID found for subject {subject.subject_id}, skipping update."
                )

        logger.info(
            f"Linked CANTAB IDs for {len(linked_subjects)}/{len(subjects_pending_cantab_link)} "
            f"subjects for data source {cantab_data_source.data_source_name}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Link CANTAB IDs to subjects")
    parser.add_argument(
        "-c", "--config", type=str, default="config.ini", help="Path to config file"
    )
    parser.add_argument("-p", "--project-id", type=str, help="Project ID to filter by")
    parser.add_argument("-s", "--site-id", type=str, help="Site ID to filter by")

    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"Config file not found: {config_file}")
        sys.exit(1)

    project_id = args.project_id
    site_id = args.site_id

    link_cantab_subject_id(
        config_file=config_file,
        project_id=project_id,
        site_id=site_id,
    )
