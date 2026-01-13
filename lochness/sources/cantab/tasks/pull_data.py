#!/usr/bin/env python
"""
Pulls data from CANTAB for active data sources and subjects.

This script is intended to be run as a cron job.
It will pull data for all active CANTAB data sources and their associated subjects.
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

from lochness.helpers import logs, db, utils
from lochness.models.logs import Logs
from lochness.models.subjects import Subject
from lochness.sources.cantab import utils as cantab_utils
from lochness.sources.cantab.models.data_source import CANTABDataSource

MODULE_NAME = "lochness.sources.cantab.tasks.pull_data"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)
NOISY_MODULES = ["urllib3.connectionpool", "paramiko.transport"]
logs.silence_logs(NOISY_MODULES)


def pull_data_for_data_source(
    config_file: Path,
    data_source: CANTABDataSource,
) -> None:
    """
    Pull data for a specific CANTAB data source.

    Args:
        config_file (Path): Path to the configuration file.
        data_source (CANTABDataSource): The CANTAB data source to pull data for.

    Returns:
        None
    """
    data_source_filter: Dict[str, Any] = {
        f"cantab.{data_source.data_source_name}": None
    }
    subjects_to_pull = Subject.get_by_filter(
        project_id=data_source.project_id,
        site_id=data_source.site_id,
        filters=data_source_filter,
        config_file=config_file,
    )
    if not subjects_to_pull:
        logger.warning(
            f"No subjects found for data source: {data_source.data_source_name}"
        )
        Logs(
            log_level="INFO",
            log_message={
                "event": "cantab_data_pull_no_subjects",
                "message": f"No subjects found for data source: {data_source.data_source_name}",
                "data_source": data_source.data_source_name,
                "project_id": data_source.project_id,
                "site_id": data_source.site_id,
            },
        ).insert(config_file)
        return

    logger.info(
        f"Found {len(subjects_to_pull)} subjects for data source: {data_source.data_source_name}"
    )

    for subject in subjects_to_pull:
        logger.info(
            "Pulling data for subject "
            f"{subject.subject_id} "
            "from data source "
            f"{data_source.data_source_name}"
        )
        try:
            data_pulls = cantab_utils.pull_data_for_subject(
                config_file=config_file,
                data_source=data_source,
                subject=subject,
            )

            if data_pulls:
                queries: List[str] = [
                    data_pull.to_sql_query() for data_pull in data_pulls
                ]
                db.execute_queries(
                    config_file=config_file,
                    queries=queries,
                    show_commands=False,
                )
                Logs(
                    log_level="INFO",
                    log_message={
                        "event": "cantab_data_pull_subject_complete",
                        "message": (
                            f"Fetched {len(data_pulls)} data pulls for subject "
                            f"{subject.subject_id} in project {data_source.project_id} "
                            f"and site {data_source.site_id}."
                        ),
                        "subject_id": subject.subject_id,
                        "data_source_name": data_source.data_source_name,
                        "project_id": data_source.project_id,
                        "site_id": data_source.site_id,
                    },
                ).insert(config_file)
        except Exception as e:  # pylint: disable=broad-except
            logger.error(
                (
                    f"Error pulling data for subject {subject.subject_id} "
                    f"from data source {data_source.data_source_name}: "
                    f"{e}"
                )
            )

    logger.info(f"Completed data pull for data source: {data_source.data_source_name}")


def pull_all_data(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
) -> None:
    """
    Pull data from all active CANTAB data sources.

    Args:
        config_file (Path): Path to the configuration file.
        project_id (Optional[str]): Project ID to filter by.
        site_id (Optional[str]): Site ID to filter by.

    Returns:
        None
    """

    active_cantab_data_sources: List[CANTABDataSource] = (
        CANTABDataSource.get_all_cantab_data_sources(
            config_file=config_file,
            active_only=True,
        )
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
            log_level="INFO",
            log_message={
                "event": "cantab_data_pull_no_active_sources",
                "message": "No active CANTAB data sources found for data pull.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return

    for data_source in active_cantab_data_sources:
        logger.info(f"Pulling data for data source: {data_source.data_source_name}")
        pull_data_for_data_source(
            config_file=config_file,
            data_source=data_source,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull data from CANTAB data sources")
    parser.add_argument("-p", "--project-id", type=str, help="Project ID to filter by")
    parser.add_argument("-s", "--site-id", type=str, help="Site ID to filter by")

    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"Config file not found: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger, noisy_modules=NOISY_MODULES
    )

    project_id = args.project_id
    site_id = args.site_id

    pull_all_data(
        config_file=config_file,
        project_id=project_id,
        site_id=site_id,
    )
