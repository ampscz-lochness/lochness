#!/usr/bin/env python
"""
Pulls data from WebDAV for active data sources and subjects.
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
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from rich.logging import RichHandler
from webdav4.client import Client

from lochness.helpers import config, db, logs, timer, utils
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.models.keystore import KeyStore
from lochness.models.logs import Logs
from lochness.models.subjects import Subject
from lochness.sources.webdav.models.data_source import WebDavDataSource

MODULE_NAME = "lochness.sources.webdav.tasks.pull_data"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)
logs.silence_logs([])


def get_subject_webdav_data_root(
    subject_id: str, webdav_data_source: WebDavDataSource, config_file: Path
) -> Path:
    """
    Get the root directory for storing WebDAV data for a subject.

    Args:
        subject_id (str): The subject ID.
        webdav_data_source (WebDavDataSource): The WebDAV data source object.
        config_file (Path): Path to the configuration file.

    Returns:
        Path: The root directory for storing WebDAV data for the subject.
    """
    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore

    project_id = webdav_data_source.project_id
    site_id = webdav_data_source.site_id

    project_name_cap = (
        project_id[:1].upper() + project_id[1:].lower() if project_id else project_id
    )

    output_dir = (
        Path(lochness_root)
        / project_name_cap
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{site_id}"
        / "raw"
        / subject_id
        / webdav_data_source.data_source_metadata.modality
    )

    return output_dir


def handle_multi_subject_file(
    file_path: Path,
    data_source: WebDavDataSource,
    config_file: Path,
) -> List[DataPull]:
    """
    Process a multi-subject file and extract data for each subject.

    Args:
        file_path (Path): Path to the multi-subject file.
        data_source (WebDavDataSource): The WebDAV data source object.
        config_file (Path): Path to the configuration file.

    Returns:
        List[DataPull]: A list of DataPull objects for each subject.
    """
    data_pulls: List[DataPull] = []
    subjects = Subject.get_by_filter(
        project_id=data_source.project_id,
        site_id=data_source.site_id,
        filters={},
        config_file=config_file,
    )

    file_extension = file_path.suffix.lower()
    if file_extension != ".csv":
        logger.warning(f"Unsupported file type for multi-subject: {file_extension}")
        raise ValueError(f"Unsupported file type for multi-subject: {file_extension}")

    multi_subject_df = pd.read_csv(file_path)

    subject_id_column = (
        data_source.data_source_metadata.file_datastructure_metadata.get(
            "subject_id_column"
        )
    )
    if subject_id_column is None:
        logger.error("Subject ID column not specified in file_datastructure_metadata")
        raise ValueError(
            "Subject ID column not specified in file_datastructure_metadata"
        )

    for subject in subjects:
        subject_id = subject.subject_id
        subject_df: pd.DataFrame = multi_subject_df[
            multi_subject_df[subject_id_column] == subject_id
        ]
        if subject_df.empty:
            logger.info(f"No data for subject {subject_id} in file {file_path}")
            continue

        subject_data_root = get_subject_webdav_data_root(
            subject_id=subject_id,
            webdav_data_source=data_source,
            config_file=config_file,
        )
        subject_data_root.mkdir(parents=True, exist_ok=True)

        file_tag = data_source.data_source_metadata.file_datastructure_metadata.get(
            "file_tag", "data"
        )
        file_name = f"{subject_id}.{file_path.name}.{file_tag}.csv"

        subject_file_path = subject_data_root / file_name

        subject_df.to_csv(subject_file_path, index=False)
        logger.info(f"Saved data for subject {subject_id} to {subject_file_path}")
        subject_file = File(file_path=subject_file_path, with_hash=True)

        db.execute_queries(
            config_file=config_file,
            queries=[subject_file.to_sql_query()],
        )

        data_pull = DataPull(
            subject_id=subject_id,
            data_source_name=data_source.data_source_name,
            site_id=data_source.site_id,
            project_id=data_source.project_id,
            file_path=str(subject_file.file_path),
            file_md5=subject_file.md5,  # type: ignore
            pull_time_s=0,
            pull_metadata={
                "source_file": str(file_path),
                "extracted_at": datetime.now().isoformat(),
            },
        )

        data_pulls.append(data_pull)

    return data_pulls


def get_files_to_download(
    client: Client,
    prefix: str,
    postfix: str,
) -> List[str]:
    """
    Get a list of files to download from the WebDAV server based on prefix and postfix.

    Args:
        client (Client): The WebDAV client.
        prefix (str): The prefix to match.
        postfix (str): The postfix to match.

    Returns:
        List[str]: A list of file paths to download.
    """
    files_do_download: List[Dict[str, Any]] = []

    child_dirs = [prefix]
    idx = 0

    while idx < len(child_dirs):
        current_dir = child_dirs[idx]
        idx += 1
        logger.info(f"Processing directory: {current_dir}")
        matched_items = client.ls(current_dir)

        for item in matched_items:
            item_name: str = item.get("name")  # type: ignore
            if item_name.endswith(postfix):
                files_do_download.append(item)  # type: ignore
            item_type = item.get("type")  # type: ignore
            if item_type == "directory":
                child_dirs.append(item_name)

    file_names = [f.get("name") for f in files_do_download]
    file_names = [f for f in file_names if f is not None]
    return file_names


def pull_data_for_data_source(
    config_file: Path,
    data_source: WebDavDataSource,
) -> None:
    """
    Pull data for a specific WebDAV data source.

    Args:
        config_file (Path): Path to the configuration file.
        data_source (WebDavDataSource): The WebDAV data source to pull data for.

    Returns:
        None
    """

    endpoint_url = data_source.data_source_metadata.endpoint_url
    keystore_name = data_source.data_source_metadata.keystore_name
    project_id = data_source.project_id

    key_store = KeyStore.retrieve_keystore(
        config_file=config_file, key_name=keystore_name, project_id=project_id
    )
    if not key_store:
        raise ValueError("WebDAV credentials not found in keystore")

    keystore_value: Dict[str, Any] = json.loads(key_store.key_value)
    username: Optional[str] = keystore_value.get("username")
    password: Optional[str] = keystore_value.get("password")

    if not username or not password:
        raise ValueError("WebDAV credentials incomplete in keystore")

    client = Client(endpoint_url, auth=(username, password))

    prefix_to_pull: str = data_source.data_source_metadata.match_prefix
    postfix_to_pull: str = data_source.data_source_metadata.match_postfix
    file_datastructure: str = data_source.data_source_metadata.file_datastructure

    files_to_download = get_files_to_download(
        client=client,
        prefix=prefix_to_pull,
        postfix=postfix_to_pull,
    )

    if not files_to_download:
        logger.warning(
            f"No files found for data source: {data_source.data_source_name}"
        )
        Logs(
            log_level="INFO",
            log_message={
                "event": "webdav_data_pull_no_files",
                "message": f"No files found for data source: {data_source.data_source_name}",
                "data_source": data_source.data_source_name,
                "project_id": data_source.project_id,
                "site_id": data_source.site_id,
            },
        ).insert(config_file)
        return

    logger.info(
        f"Found {len(files_to_download)} files for data source: {data_source.data_source_name}"
    )

    for file in files_to_download:
        with tempfile.NamedTemporaryFile() as tmp_file:
            local_path = Path(tmp_file.name)
            logger.debug(f"Downloading file: {file} to {local_path}")

            with timer.Timer() as download_timer:
                client.download_file(from_path=file, to_path=local_path)

            data_pulls: List[DataPull] = []
            if file_datastructure == "multi-subject":
                data_pulls.extend(
                    handle_multi_subject_file(
                        file_path=local_path,
                        data_source=data_source,
                        config_file=config_file,
                    )
                )
            else:
                logger.warning(
                    "Unsupported file_datastructure: "
                    f"{file_datastructure} "
                    "for data source: "
                    f"{data_source.data_source_name}"
                )
                raise ValueError(
                    "Unsupported file_datastructure: "
                    f"{file_datastructure} "
                    "for data source: "
                    f"{data_source.data_source_name}"
                )

            for data_pull in data_pulls:
                data_pull.pull_time_s = (
                    int(download_timer.duration)
                    if download_timer.duration is not None
                    else 0
                )

            queries: List[str] = [data_pull.to_sql_query() for data_pull in data_pulls]
            db.execute_queries(
                config_file=config_file,
                queries=queries,
                show_commands=False,
            )
            logger.info(
                (
                    f"Processed file: {file} "
                    f"with {len(data_pulls)} data pulls, "
                    f"generating {len(data_pulls)} pull records."
                )
            )

    logger.info(f"Completed data pull for data source: {data_source.data_source_name}")


def pull_all_data(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
) -> None:
    """
    Pull data from all active WebDAV data sources.

    Args:
        config_file (Path): Path to the configuration file.
        project_id (Optional[str]): Project ID to filter by.
        site_id (Optional[str]): Site ID to filter by.

    Returns:
        None
    """

    active_webdav_data_sources: List[WebDavDataSource] = (
        WebDavDataSource.get_all_webdav_data_sources(
            config_file=config_file,
            active_only=True,
        )
    )

    if project_id:
        active_webdav_data_sources = [
            ds for ds in active_webdav_data_sources if ds.project_id == project_id
        ]

    if site_id:
        active_webdav_data_sources = [
            ds for ds in active_webdav_data_sources if ds.site_id == site_id
        ]

    if not active_webdav_data_sources:
        logger.warning("No active WebDAV data sources found.")
        Logs(
            log_level="INFO",
            log_message={
                "event": "webdav_data_pull_no_active_sources",
                "message": "No active WebDAV data sources found for data pull.",
                "project_id": project_id,
                "site_id": site_id,
            },
        ).insert(config_file)
        return

    for data_source in active_webdav_data_sources:
        logger.info(f"Pulling data for data source: {data_source.data_source_name}")
        pull_data_for_data_source(
            config_file=config_file,
            data_source=data_source,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull data from WebDAV data sources")
    parser.add_argument("-p", "--project-id", type=str, help="Project ID to filter by")
    parser.add_argument("-s", "--site-id", type=str, help="Site ID to filter by")

    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"Config file not found: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    project_id = args.project_id
    site_id = args.site_id

    pull_all_data(
        config_file=config_file,
        project_id=project_id,
        site_id=site_id,
    )
