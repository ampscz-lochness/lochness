#!/usr/bin/env python
"""
Scheduler task to pull REDCap data for all active REDCap data sources.
"""

import argparse
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

import logging
from datetime import datetime
from typing import Any, Dict, List

from rich.logging import RichHandler

from lochness.helpers import cli, config, db, logs, utils
from lochness.models.job import Job
from lochness.models.sites import Site
from lochness.models.subjects import Subject

MODULE_NAME = "lochness.sources.redcap.schedule.pull_data"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to schedule REDCap metadata refresh jobs."
    )
    parser.add_argument(
        "--project_id",
        "-p",
        type=str,
        help="Refresh metadata for all sites from this project ID.",
        required=True,
        default=None,
    )
    parser.add_argument(
        "--site_id_list",
        "-s",
        type=str,
        nargs="+",
        help=(
            "List of site IDs to refresh metadata for (optional), "
            "e.g. --site_id_list siteA siteB siteC"
        ),
        default=None,
    )
    parser.add_argument(
        "--subject_id_list",
        "-l",
        type=str,
        nargs="+",
        help=(
            "List of subject IDs to refresh metadata for (optional), "
            "e.g. --subject_id_list sub001 sub002 sub003"
        ),
        default=None,
    )

    args = parser.parse_args()
    project_id: str = args.project_id
    site_id_list: List[str] = args.site_id_list if args.site_id_list else []
    subject_id_list: List[str] = args.subject_id_list if args.subject_id_list else []

    config_file = utils.get_config_file_path()
    logger.info(f"Using config file: {config_file}")

    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    project_sites = Site.fetch_all(
        config_file=config_file,
        project_id=project_id,
        active_only=True,
    )

    if site_id_list:
        project_sites = [site for site in project_sites if site.site_id in site_id_list]

    if len(project_sites) == 0:
        logger.warning(f"No active sites found for project ID: {project_id}")
        sys.exit(0)

    repo_root = Path(cli.get_repo_root())
    script_path = (
        repo_root / "lochness" / "sources" / "redcap" / "tasks" / "pull_data.py"
    )

    scheduler_config = config.parse(config_file, "scheduler")
    if "exec" not in scheduler_config:
        logger.error(
            f"'exec' not found in [scheduler] section of config file: {config_file}"
        )
        sys.exit(1)
    scheduler_exec: str = scheduler_config["exec"]  # type: ignore[assignment]

    jobs: List[Job] = []
    for site in project_sites:
        site_subjects = Subject.get_by_filter(
            config_file=config_file,
            project_id=site.project_id,
            site_id=site.site_id,
        )
        if subject_id_list:
            site_subjects = [
                subject
                for subject in site_subjects
                if subject.subject_id in subject_id_list
            ]

        if len(site_subjects) == 0:
            logger.warning(
                f"No subjects found for project ID: {site.project_id}, site ID: {site.site_id}"
            )
            continue

        for subject in site_subjects:
            job = Job(
                job_payload=(
                    f"{scheduler_exec} "
                    f"{script_path} "
                    f"-p {site.project_id} "
                    f"-s {site.site_id} "
                    f"-l {subject.subject_id}"
                ),
                job_metadata={
                    "CWD": str(repo_root),
                },
                job_status="PENDING",
                job_tags=[
                    "redcap",
                    "pull_data",
                    site.project_id,
                    site.site_id,
                    subject.subject_id,
                ],
                job_last_updated=datetime.now(),
                job_submission_time=datetime.now(),
            )
            jobs.append(job)

    queries = [job.insert_query() for job in jobs]
    db.execute_queries(config_file=config_file, queries=queries, db="job_queue")

    logger.info(
        f"Scheduled {len(jobs)} metadata refresh jobs for project ID: {project_id}"
    )
