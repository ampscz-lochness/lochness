"""
MindLAMP Data Pull Script

This script pulls data from MindLAMP data sources and saves it to the file system.
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
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pytz
from rich.logging import RichHandler

from lochness.helpers import logs, utils
from lochness.models.logs import Logs
from lochness.models.subjects import Subject
from lochness.sources.mindlamp import utils as mindlamp_utils
from lochness.sources.mindlamp.models.data_source import MindLAMPDataSource

MODULE_NAME = "lochness.sources.mindlamp.tasks.pull_data"

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
        "data_source_type": "mindlamp",
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


def pull_all_data(
    config_file: Path,
    start_date: datetime,
    end_date: datetime,
    force_start_date: Optional[datetime],
    force_end_date: Optional[datetime],
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    subject_id_list: Optional[List[str]] = None,
):
    """
    Main function to pull data for all active MindLAMP data sources and subjects.
    """
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="mindlamp_data_pull_start",
        message="Starting MindLAMP data pull process.",
        project_id=project_id,
        site_id=site_id,
        data_source_name=None,
    )

    active_mindlamp_data_sources = MindLAMPDataSource.get_all_mindlamp_data_sources(
        config_file=config_file, active_only=True
    )

    if project_id:
        active_mindlamp_data_sources = [
            ds for ds in active_mindlamp_data_sources if ds.project_id == project_id
        ]
    if site_id:
        active_mindlamp_data_sources = [
            ds for ds in active_mindlamp_data_sources if ds.site_id == site_id
        ]

    if not active_mindlamp_data_sources:
        logger.info("No active MindLAMP data sources found for data pull.")
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="mindlamp_data_pull_no_active_sources",
            message="No active MindLAMP data sources found for data pull.",
            project_id=project_id,
            site_id=site_id,
            data_source_name=None,
        )
        return

    logger.info(
        "Identified "
        f"{len(active_mindlamp_data_sources)} "
        "active MindLAMP data sources "
        "for data pull."
    )
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="mindlamp_data_pull_active_sources_found",
        message=(
            f"Identified {len(active_mindlamp_data_sources)} "
            "active MindLAMP data sources for data pull."
        ),
        project_id=project_id,
        site_id=site_id,
        data_source_name=None,
        extra={"count": len(active_mindlamp_data_sources)},
    )

    for mindlamp_data_source in active_mindlamp_data_sources:
        # Get subjects for this data source
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=mindlamp_data_source.project_id,
            site_id=mindlamp_data_source.site_id,
            config_file=config_file,
        )

        if subject_id_list:
            subjects_in_db = [
                x for x in subjects_in_db if x.subject_id in subject_id_list
            ]

        if not subjects_in_db:
            logger.info(
                (
                    "No subjects found for "
                    f"{mindlamp_data_source.project_id}::"
                    f"{mindlamp_data_source.site_id}."
                )
            )
            log_event(
                config_file=config_file,
                log_level="INFO",
                event="mindlamp_data_pull_no_subjects",
                message=(
                    f"No subjects found for {mindlamp_data_source.project_id}::"
                    f"{mindlamp_data_source.site_id}."
                ),
                project_id=mindlamp_data_source.project_id,
                site_id=mindlamp_data_source.site_id,
                data_source_name=mindlamp_data_source.data_source_name,
            )
            continue

        logger.info(
            f"Found {len(subjects_in_db)} subjects for {mindlamp_data_source.data_source_name}."
        )
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="mindlamp_data_pull_subjects_found",
            message=(
                f"Found {len(subjects_in_db)} subjects for "
                f"{mindlamp_data_source.data_source_name}."
            ),
            project_id=mindlamp_data_source.project_id,
            site_id=mindlamp_data_source.site_id,
            data_source_name=mindlamp_data_source.data_source_name,
            extra={"count": len(subjects_in_db)},
        )

        for subject in subjects_in_db:
            data_pulls = mindlamp_utils.pull_subject_data(
                mindlamp_data_source=mindlamp_data_source,
                subject_id=subject.subject_id,
                start_date=start_date,
                end_date=end_date,
                force_start_date=force_start_date,
                force_end_date=force_end_date,
                config_file=config_file,
            )

            if data_pulls:
                logger.info(
                    f"Fetched {len(data_pulls)} data pulls for subject {subject.subject_id} "
                    f"in project {mindlamp_data_source.project_id} and site "
                    f"{mindlamp_data_source.site_id}."
                )
                log_event(
                    config_file=config_file,
                    log_level="INFO",
                    event="mindlamp_data_pull_subject_complete",
                    message=(
                        f"Fetched {len(data_pulls)} data pulls for subject "
                        f"{subject.subject_id} in project {mindlamp_data_source.project_id} "
                        f"and site {mindlamp_data_source.site_id}."
                    ),
                    project_id=mindlamp_data_source.project_id,
                    site_id=mindlamp_data_source.site_id,
                    data_source_name=mindlamp_data_source.data_source_name,
                    subject_id=subject.subject_id,
                )

    logger.info("MindLAMP data pull process completed.")
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="mindlamp_data_pull_complete",
        message="MindLAMP data pull process completed.",
        project_id=project_id,
        site_id=site_id,
        data_source_name=None,
    )


def parse_date(date_str: str) -> datetime:
    """
    Parse a date string in the format YYYY-MM-DD and return a timezone-aware datetime object.

    Args:
        date_str (str): Date string in the format YYYY-MM-DD.

    Returns:
        datetime: A timezone-aware datetime object set to UTC.
    """
    datetime_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    return datetime_dt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull data from MindLAMP data sources")
    parser.add_argument("-c", "--config", type=str, help="Path to config file")
    parser.add_argument("-p", "--project-id", type=str, help="Project ID to filter by")
    parser.add_argument("-s", "--site-id", type=str, help="Site ID to filter by")
    parser.add_argument(
        "-d",
        "--days-to-pull",
        type=int,
        default=14,
        help="Number of days of data to pull",
    )
    parser.add_argument(
        "-r",
        "--days-to-redownload",
        type=int,
        default=7,
        help="Number of days of data to redownload",
    )
    parser.add_argument(
        "--start-date",
        "-a",
        type=str,
        help="Start date for data pull (YYYY-MM-DD)",
        default=None,
    )
    parser.add_argument(
        "--end-date",
        "-b",
        type=str,
        help="End date for data pull (YYYY-MM-DD)",
        default=None,
    )
    parser.add_argument(
        "--force-start-date",
        "-f",
        type=str,
        help="Start date for force redownload (YYYY-MM-DD)",
        default=None,
    )
    parser.add_argument(
        "--force-end-date",
        "-g",
        type=str,
        help="End date for force redownload (YYYY-MM-DD)",
        default=None,
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

    logger.info("Starting MindLAMP data pull script...")

    # Validation logic for date/days arguments
    date_range_provided = args.start_date is not None and args.end_date is not None
    days_provided = (
        args.days_to_pull is not None and args.days_to_redownload is not None
    )

    if date_range_provided and days_provided:
        logger.error(
            "Both date range (start_date/end_date) and days-based arguments "
            "(days_to_pull/days_to_redownload) provided. Please provide only one method."
        )
        sys.exit(1)
    if not date_range_provided and not days_provided:
        logger.error(
            "Neither date range nor days-based arguments provided. "
            "Please specify either a date range or days to pull/redownload."
        )
        sys.exit(1)

    # Parse date arguments if provided, else use days-based logic
    if date_range_provided:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        logger.info(f"Using date range: {start_date.date()} to {end_date.date()}")
    else:
        end_date = datetime.now(tz=pytz.UTC) - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days_to_pull - 1)
        logger.info(f"Days to pull: {args.days_to_pull}")
        logger.info(f"Pulling data from {start_date.date()} to {end_date.date()}")

    if args.force_start_date and args.force_end_date:
        force_start_date = parse_date(args.force_start_date)
        force_end_date = parse_date(args.force_end_date)
        logger.info(
            f"Redownloading data from {force_start_date.date()} to {force_end_date.date()}"
        )
    else:
        force_end_date = end_date
        force_start_date = force_end_date - timedelta(days=args.days_to_redownload - 1)
        logger.info(f"Days to redownload: {args.days_to_redownload}")
        logger.info(
            f"Redownloading data from {force_start_date.date()} to {force_end_date.date()}"
        )

    pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
        start_date=start_date,
        end_date=end_date,
        force_start_date=force_start_date,
        force_end_date=force_end_date,
    )
