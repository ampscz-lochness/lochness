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
from collections import Counter, defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

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
    subject_ids: Optional[List[str]] = None,
    jobs: int = 1,
):
    """
    Main function to pull data for all active MindLAMP data sources and subjects.

    If subject_ids is provided, only those subjects are pulled.
    Up to `jobs` subject-dates are downloaded in parallel.
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

    total_data_pulls = 0
    for mindlamp_data_source in active_mindlamp_data_sources:
        # Get subjects for this data source
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=mindlamp_data_source.project_id,
            site_id=mindlamp_data_source.site_id,
            config_file=config_file,
        )

        if subject_ids:
            subjects_in_db = [
                subject
                for subject in subjects_in_db
                if subject.subject_id in subject_ids
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

        # Plan the (subject, date) downloads, then run them across `jobs` workers.
        # Data sources are processed one at a time, since LAMP.connect() sets
        # module-level clients and data sources may use different credentials.
        download_tasks: List[Tuple[str, str, Path, datetime]] = []
        for subject in subjects_in_db:
            download_plan = mindlamp_utils.get_subject_dates_to_download(
                mindlamp_data_source=mindlamp_data_source,
                subject_id=subject.subject_id,
                start_date=start_date,
                end_date=end_date,
                force_start_date=force_start_date,
                force_end_date=force_end_date,
                config_file=config_file,
            )
            if download_plan is None:
                continue

            mindlamp_id, subject_mindlamp_data_root, dates_to_download = download_plan
            for date_dt in dates_to_download:
                download_tasks.append(
                    (subject.subject_id, mindlamp_id, subject_mindlamp_data_root, date_dt)
                )

        logger.info(
            f"Downloading {len(download_tasks)} subject-dates for "
            f"{mindlamp_data_source.data_source_name} using {jobs} parallel job(s)."
        )

        remaining_dates_per_subject: Dict[str, int] = Counter(
            task[0] for task in download_tasks
        )
        pulls_per_subject: Dict[str, int] = defaultdict(int)

        with ThreadPoolExecutor(max_workers=jobs) as executor:
            futures: Dict[Future[List[Any]], Tuple[str, datetime]] = {
                executor.submit(
                    mindlamp_utils.fetch_subject_data_for_date,
                    mindlamp_data_source=mindlamp_data_source,
                    subject_id=subject_id,
                    mindlamp_id=mindlamp_id,
                    datetime_dt=date_dt,
                    config_file=config_file,
                    subject_mindlamp_data_root=subject_mindlamp_data_root,
                ): (subject_id, date_dt)
                for (
                    subject_id,
                    mindlamp_id,
                    subject_mindlamp_data_root,
                    date_dt,
                ) in download_tasks
            }

            for future in as_completed(futures):
                subject_id, date_dt = futures[future]
                try:
                    num_new_pulls = len(future.result())
                    pulls_per_subject[subject_id] += num_new_pulls
                    total_data_pulls += num_new_pulls
                except Exception as e:  # pylint: disable=broad-except
                    logger.error(
                        f"Failed to fetch data for subject {subject_id} on "
                        f"{date_dt.date()}: {e}"
                    )
                    log_event(
                        config_file=config_file,
                        log_level="ERROR",
                        event="mindlamp_data_pull_date_failed",
                        message=(
                            f"Failed to fetch data for subject {subject_id} on "
                            f"{date_dt.date()}: {e}"
                        ),
                        project_id=mindlamp_data_source.project_id,
                        site_id=mindlamp_data_source.site_id,
                        data_source_name=mindlamp_data_source.data_source_name,
                        subject_id=subject_id,
                        extra={"data_date_utc": str(date_dt.date())},
                    )

                remaining_dates_per_subject[subject_id] -= 1
                if remaining_dates_per_subject[subject_id] > 0:
                    continue

                num_pulls = pulls_per_subject[subject_id]
                if num_pulls:
                    logger.info(
                        f"Fetched {num_pulls} data pulls for subject {subject_id} "
                        f"in project {mindlamp_data_source.project_id} and site "
                        f"{mindlamp_data_source.site_id}."
                    )
                    log_event(
                        config_file=config_file,
                        log_level="INFO",
                        event="mindlamp_data_pull_subject_complete",
                        message=(
                            f"Fetched {num_pulls} data pulls for subject "
                            f"{subject_id} in project {mindlamp_data_source.project_id} "
                            f"and site {mindlamp_data_source.site_id}."
                        ),
                        project_id=mindlamp_data_source.project_id,
                        site_id=mindlamp_data_source.site_id,
                        data_source_name=mindlamp_data_source.data_source_name,
                        subject_id=subject_id,
                    )

    logger.info(f"Recorded {total_data_pulls} DataPulls in this run.")
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="mindlamp_data_pull_total",
        message=f"Recorded {total_data_pulls} DataPulls in this run.",
        project_id=project_id,
        site_id=site_id,
        data_source_name=None,
        extra={"count": total_data_pulls},
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
        "-sid",
        "--subject-id",
        type=str,
        nargs="+",
        default=None,
        help="One or more subject IDs to filter by (space or comma separated)",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=1,
        help="Number of subject-dates to download in parallel (default: 1)",
    )
    parser.add_argument(
        "-d",
        "--days-to-pull",
        type=int,
        default=None,
        help="Number of days of data to pull (default: 14 when days mode is used)",
    )
    parser.add_argument(
        "-r",
        "--days-to-redownload",
        type=int,
        default=None,
        help=(
            "Number of days, ending at the end date, to redownload even if already "
            "pulled. Ignored if --force-start-date/--force-end-date are given. "
            "Use 0 to disable (default: 7)"
        ),
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
    args = parser.parse_args()

    config_file = (
        Path(args.config).expanduser().resolve()
        if args.config
        else utils.get_config_file_path()
    )
    logger.info(f"Using config file: {config_file}")
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Starting MindLAMP data pull script...")

    if args.jobs < 1:
        logger.error(f"--jobs must be at least 1, got {args.jobs}.")
        sys.exit(1)

    subject_ids: Optional[List[str]] = None
    if args.subject_id:
        subject_ids = [
            sid.strip()
            for value in args.subject_id
            for sid in value.split(",")
            if sid.strip()
        ]
        logger.info(f"Filtering to subjects: {', '.join(subject_ids)}")

    # Validation logic for date/days arguments
    date_range_partially_provided = (
        args.start_date is not None or args.end_date is not None
    )
    date_range_provided = args.start_date is not None and args.end_date is not None
    if date_range_partially_provided and not date_range_provided:
        logger.error(
            "Both start_date and end_date must be provided when using date-range mode."
        )
        sys.exit(1)

    if date_range_provided and args.days_to_pull is not None:
        logger.error(
            "Both date range (start_date/end_date) and days_to_pull provided. "
            "Please provide only one method."
        )
        sys.exit(1)

    if args.days_to_redownload is not None and args.days_to_redownload < 0:
        logger.error(
            f"--days-to-redownload must be at least 0, got {args.days_to_redownload}."
        )
        sys.exit(1)

    days_to_redownload = (
        args.days_to_redownload if args.days_to_redownload is not None else 7
    )
    # Parse date arguments if provided, else use days-based logic
    if date_range_provided:
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        logger.info(f"Using date range: {start_date.date()} to {end_date.date()}")
    else:
        days_to_pull = args.days_to_pull if args.days_to_pull is not None else 14
        end_date = datetime.now(tz=pytz.UTC) - timedelta(days=1)
        start_date = end_date - timedelta(days=days_to_pull - 1)
        logger.info(f"Days to pull: {days_to_pull}")
        logger.info(f"Pulling data from {start_date.date()} to {end_date.date()}")

    force_start_date: Optional[datetime] = None
    force_end_date: Optional[datetime] = None
    if args.force_start_date and args.force_end_date:
        force_start_date = parse_date(args.force_start_date)
        force_end_date = parse_date(args.force_end_date)
        logger.info(
            f"Redownloading data from {force_start_date.date()} to {force_end_date.date()}"
        )
    elif days_to_redownload > 0:
        force_end_date = end_date
        force_start_date = force_end_date - timedelta(days=days_to_redownload - 1)
        logger.info(f"Days to redownload: {days_to_redownload}")
        logger.info(
            f"Redownloading data from {force_start_date.date()} to {force_end_date.date()}"
        )
    else:
        logger.info("Skipping forced redownload (days to redownload is 0).")

    pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
        subject_ids=subject_ids,
        jobs=args.jobs,
        start_date=start_date,
        end_date=end_date,
        force_start_date=force_start_date,
        force_end_date=force_end_date,
    )
