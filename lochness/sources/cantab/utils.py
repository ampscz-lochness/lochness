"""
CANTAB data pull logic and utilities.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional

from lochness.helpers import config, db
from lochness.helpers.timer import Timer
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.models.subjects import Subject
from lochness.sources.cantab import api as cantab_api
from lochness.sources.cantab.models.data_source import CANTABDataSource

logger = logging.getLogger()


def get_subject_cantab_data_root(
    subject_id: str, cantab_data_source: CANTABDataSource, config_file: Path
) -> Path:
    """
    Get the root directory for storing CANTAB data for a subject.

    Args:
        subject_id (str): The subject ID.
        cantab_data_source (CANTABDataSource): The CANTAB data source object.
        config_file (Path): Path to the configuration file.

    Returns:
        Path: The root directory for storing CANTAB data for the subject.
    """
    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore

    project_id = cantab_data_source.project_id
    site_id = cantab_data_source.site_id

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
        / "cantab"
    )

    return output_dir


def get_subject_cantab_id(
    subject: Subject, data_source: CANTABDataSource
) -> Optional[str]:
    """
    Retrieve the CANTAB ID for a given subject from their metadata.

    Args:
        subject (Subject): The subject to retrieve the CANTAB ID for.
        data_source (CANTABDataSource): The CANTAB data source to use.

    Returns:
        Optional[str]: The CANTAB ID if found, otherwise None.
    """
    cantab_metadata = subject.subject_metadata.get("cantab", {})
    data_source_metadata = cantab_metadata.get(data_source.data_source_name, {})
    cantab_id = data_source_metadata.get("cantab_id", None)
    return cantab_id


def pull_data_for_subject(
    config_file: Path,
    data_source: CANTABDataSource,
    subject: Subject,
) -> List[DataPull]:
    """
    Pull data for a specific subject from a CANTAB data source.

    Args:
        config_file (Path): Path to the configuration file.
        data_source (CANTABDataSource): The CANTAB data source to pull data from.
        subject (Subject): The subject to pull data for.

    Returns:
        List[DataPull]: A list of DataPull records created for the subject.
    """
    associated_files: List[File] = []
    data_pulls: List[DataPull] = []
    subject_cantab_id = get_subject_cantab_id(subject=subject, data_source=data_source)

    if not subject_cantab_id:
        logger.warning(
            f"No CANTAB ID found for subject: "
            f"{subject.subject_id} "
            f"from data source: "
            f"{data_source.data_source_name}"
        )
        return data_pulls

    with Timer() as timer:
        cantab_data = cantab_api.get_cantab_data(
            cantab_data_source=data_source,
            cantab_id=subject_cantab_id,
            config_file=config_file,
        )

    cantab_data_root = get_subject_cantab_data_root(
        subject_id=subject.subject_id,
        cantab_data_source=data_source,
        config_file=config_file,
    )
    data_file_name = f"{subject.subject_id}.{data_source.data_source_name}.json"
    cantab_data_root.mkdir(parents=True, exist_ok=True)
    data_file_path = cantab_data_root / data_file_name

    with open(data_file_path, "w", encoding="utf-8") as f:
        json.dump(cantab_data, f, indent=4)

    data_file = File(file_path=data_file_path)
    associated_files.append(data_file)

    data_pull = DataPull(
        subject_id=subject.subject_id,
        project_id=data_source.project_id,
        site_id=data_source.site_id,
        data_source_name=data_source.data_source_name,
        file_path=str(data_file_path),
        file_md5=data_file.md5,  # type: ignore
        pull_time_s=int(timer.duration),  # type: ignore
        pull_metadata={"cantab_id": subject_cantab_id},
    )
    data_pulls.append(data_pull)

    logger.info(
        (
            f"Pulled data for subject: {subject.subject_id} "
            f"from data source: {data_source.data_source_name} "
            f"in {timer.duration:.2f} seconds. "
            f"Data saved to: {data_file_path}"
        )
    )

    queries: List[str] = []
    for file in associated_files:
        queries.extend(file.to_sql_queries_with_availability_update())

    db.execute_queries(
        config_file=config_file,
        queries=queries,
        show_commands=False,
    )

    return data_pulls
