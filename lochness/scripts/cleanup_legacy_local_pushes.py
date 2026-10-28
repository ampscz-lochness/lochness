#!/usr/bin/env python
"""
Clean up local files and availability metadata for files pushed before
automatic post-push local cleanup was added.
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

try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import argparse
import logging
from typing import Any, Dict, Optional, Tuple

from rich.logging import RichHandler

from lochness.helpers import logs, utils
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.tasks.push_data import cleanup_local_file_after_push

MODULE_NAME = "lochness.scripts.cleanup_legacy_local_pushes"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


def resolve_file_scope(
    file_obj: File,
    config_file: Path,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Determine the project/site scope for a file version.

    Prefer the matching DataPull record for the file version, and fall back to
    file metadata when needed.
    """
    if file_obj.md5 is not None:
        data_pull = DataPull.get_most_recent_data_pull(
            config_file=config_file,
            file_path=str(file_obj.file_path),
            file_md5=file_obj.md5,
        )
        if data_pull is not None:
            return data_pull.project_id, data_pull.site_id

    project_id = file_obj.file_metadata.get("project_id")
    site_id = file_obj.file_metadata.get("site_id")
    return project_id, site_id


def cleanup_legacy_local_pushes(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Remove stale local copies for files that have already been pushed.
    """
    hostname_location = f"hn:{utils.get_hostname()}"
    candidates = File.get_files_with_available_location(
        config_file=config_file,
        location=hostname_location,
    )

    logger.info(
        f"Found {len(candidates)} files with local availability at {hostname_location}."
    )

    cleaned_count = 0
    dry_run_count = 0
    skipped_missing_scope = 0
    skipped_no_pushes = 0
    skipped_pending = 0

    for file_obj in candidates:
        file_project_id, file_site_id = resolve_file_scope(file_obj, config_file)

        if not file_project_id or not file_site_id:
            skipped_missing_scope += 1
            logger.warning(
                f"Skipping {file_obj.file_path}: unable to resolve project/site scope."
            )
            continue

        if project_id and file_project_id != project_id:
            continue
        if site_id and file_site_id != site_id:
            continue

        if not file_obj.has_any_pushes(config_file):
            skipped_no_pushes += 1
            logger.debug(
                f"Skipping {file_obj.file_path}: no data_push record exists for this file version."
            )
            continue

        if file_obj.has_pending_pushes(
            config_file=config_file,
            project_id=file_project_id,
            site_id=file_site_id,
        ):
            skipped_pending += 1
            logger.debug(
                f"Skipping {file_obj.file_path}: one or more active sink pushes are still pending."
            )
            continue

        if dry_run:
            dry_run_count += 1
            logger.info(
                f"DRY RUN: would clean up local file and availability for {file_obj.file_path}"
            )
            continue

        cleanup_local_file_after_push(file_obj=file_obj, config_file=config_file)
        cleaned_count += 1

    logger.info(
        "Cleanup summary: "
        f"cleaned={cleaned_count}, "
        f"dry_run_candidates={dry_run_count}, "
        f"skipped_missing_scope={skipped_missing_scope}, "
        f"skipped_no_pushes={skipped_no_pushes}, "
        f"skipped_pending={skipped_pending}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Clean up local files and hostname availability metadata for file "
            "versions that were already pushed before automatic cleanup existed."
        )
    )
    parser.add_argument(
        "--project-id",
        "-p",
        type=str,
        default=None,
        help="Optional project ID filter.",
    )
    parser.add_argument(
        "--site-id",
        "-s",
        type=str,
        default=None,
        help="Optional site ID filter.",
    )
    parser.add_argument(
        "--config",
        "-c",
        type=Path,
        default=utils.get_config_file_path(),
        help="Path to the configuration file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report eligible files without removing local files or updating metadata.",
    )
    args = parser.parse_args()

    config_file = Path(args.config)
    logs.configure_logging(
        config_file=config_file,
        module_name=MODULE_NAME,
        logger=logger,
        use_db=False,
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    cleanup_legacy_local_pushes(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
        dry_run=args.dry_run,
    )