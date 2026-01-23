"""
MindLAMP data pull logic and utilities.
"""

import base64
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import pytz

from lochness.helpers import config, db
from lochness.helpers.timer import Timer
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.models.subjects import Subject
from lochness.sources.mindlamp import api as mindlamp_api
from lochness.sources.mindlamp.models.data_source import MindLAMPDataSource

logger = logging.getLogger(__name__)


# --- Audio extraction ---
def extract_audio_from_activities(
    activity_dicts: List[Dict[str, Any]],
    audio_data_root: Path,
    audio_file_name_template: str,
) -> Tuple[List[Dict[str, Any]], List[Path]]:
    """
    Separate out audio data from the content pulled from MindLAMP API.

    Returns a tuple containing:
    - List of activity dictionaries with audio URLs replaced by placeholders
    - List of Paths to the saved audio files
    """
    activity_dicts_wo_sound = []
    num = 0
    audio_file_paths = []

    for activity_events_dicts in activity_dicts:
        if "url" in activity_events_dicts.get("static_data", {}):
            audio: str = activity_events_dicts["static_data"]["url"]
            activity_events_dicts["static_data"]["url"] = f"SOUND_{num}"
            audio_timestamp: Optional[int] = activity_events_dicts.get(
                "timestamp", None
            )  # 1684976609734
            audio_dt = (
                datetime.fromtimestamp(audio_timestamp / 1000, tz=pytz.UTC)
                if audio_timestamp
                else None
            )
            suffix = (
                f"_{audio_dt.strftime('%H_%M_%S')}_{num}_audio.mp3"
                if audio_dt
                else f"TIME_{num}_audio.mp3"
            )
            try:
                decode_bytes = base64.b64decode(audio.split(",")[1])
                # Generate a unique audio file name for each audio event
                audio_file_name = f"{audio_file_name_template}{suffix}"
                audio_file_path = audio_data_root / audio_file_name
                with open(audio_file_path, "wb", encoding="utf-8") as f:
                    f.write(decode_bytes)
                audio_file_paths.append(Path(audio_file_path))
                logger.debug(f"Saved audio file: {audio_file_path}")
                num += 1
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(f"Failed to decode audio data: {e}")

        activity_dicts_wo_sound.append(activity_events_dicts)

    return activity_dicts_wo_sound, audio_file_paths


# --- Subject utilities ---
def get_mindlamp_id_for_subject(
    mindlamp_data_source: MindLAMPDataSource,
    subject_id: str,
    config_file: Path,
) -> Optional[str]:
    """
    Get the MindLAMP ID for a subject.

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        subject_id (str): The subject ID.
        config_file (Path): Path to the configuration file.

    Returns:
        Optional[str]: The MindLAMP ID for the subject, or None if not found.
    """
    subject = Subject.get(
        project_id=mindlamp_data_source.project_id,
        site_id=mindlamp_data_source.site_id,
        subject_id=subject_id,
        config_file=config_file,
    )

    if subject:
        return subject.subject_metadata.get("mindlamp_id", None)

    return None


def get_subject_mindlamp_data_root(
    subject_id: str, mindlamp_data_source: MindLAMPDataSource, config_file: Path
) -> Path:
    """
    Get the root directory for storing MindLAMP data for a subject.

    Args:
        subject_id (str): The subject ID.
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        config_file (Path): Path to the configuration file.

    Returns:
        Path: The root directory for storing MindLAMP data for the subject.
    """
    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore

    project_id = mindlamp_data_source.project_id
    site_id = mindlamp_data_source.site_id

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
        / "phone"
    )

    return output_dir


# --- Date utilities ---
def find_missing_data_dates(
    subject_id: str,
    mindlamp_ds: MindLAMPDataSource,
    start_date: datetime,
    config_file: Path,
    end_date: Optional[datetime] = None,
) -> Set[datetime]:
    """
    Returns a set of dates for which there is no data pull for the given subject.

    Args:
        subject_id (str): The subject ID.
        mindlamp_ds (MindLAMPDataSource): The MindLAMP data source object.
        start_date (datetime): The start date.
        config_file (Path): Path to the configuration file.
        end_date (Optional[datetime]): The end date. Defaults to None, which sets it to yesterday.

    Returns:
        Set[datetime]: A set of dates without data pulls.
    """

    if end_date is None:
        end_date = datetime.now(tz=pytz.UTC) - timedelta(days=1)
        end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    data_pulls_df = DataPull.get_data_pulls_for_subject(
        subject_id=subject_id,
        site_id=mindlamp_ds.site_id,
        project_id=mindlamp_ds.project_id,
        config_file=config_file,
        data_source_name=mindlamp_ds.data_source_name,
    )

    all_dates = pd.date_range(start=start_date, end=end_date, freq="D", tz=pytz.UTC)
    if len(data_pulls_df) == 0:
        missing_dates = sorted(set(all_dates))
        missing_dates_dt = [date.to_pydatetime() for date in missing_dates]
        return set(missing_dates_dt)

    data_pulls_df["data_date_utc"] = pd.to_datetime(
        data_pulls_df["data_date_utc"], utc=True
    )

    pulled_dates = data_pulls_df["data_date_utc"].dt.normalize().unique()

    missing_dates = sorted(set(all_dates) - set(pulled_dates))

    missing_dates_dt = [date.to_pydatetime() for date in missing_dates]
    return set(missing_dates_dt)


# --- Data pull logic ---
def fetch_subject_data_for_date(
    mindlamp_data_source: MindLAMPDataSource,
    subject_id: str,
    mindlamp_id: str,
    datetime_dt: datetime,
    config_file: Path,
    subject_mindlamp_data_root: Path,
) -> List[DataPull]:
    """
    Fetch data for a subject from MindLAMP for a specific date.

    This includes fetching activity and sensor events, daily audio journals.

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        subject_id (str): The subject ID.
        mindlamp_id (str): The MindLAMP ID of the subject.
        datetime_dt (datetime): The date for which to fetch data.
        config_file (Path): Path to the configuration file.
        subject_mindlamp_data_root (Path): Path to the directory where subject data will be saved.

    Returns:
        List[DataPull]: A list of data pulls for the subject.
    """
    data_pulls: List[DataPull] = []
    associated_files: List[File] = []

    project_id = mindlamp_data_source.project_id
    site_id = mindlamp_data_source.site_id
    data_source_name = mindlamp_data_source.data_source_name

    identifier = f"{project_id}::{site_id}::{data_source_name}::{subject_id}"

    mindlamp_api.connect_to_mindlamp(mindlamp_data_source, config_file)

    dt_in_utc = datetime_dt.astimezone(pytz.timezone("UTC"))
    date_utc = dt_in_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    date_str = date_utc.strftime("%Y_%m_%d")

    # unix timestamps in milliseconds
    start_timestamp = int(date_utc.timestamp() * 1000)
    end_timestamp = start_timestamp + 24 * 60 * 60 * 1000

    logger.debug(f"Fetching data for {identifier} for date {date_str}...")
    logger.debug(f"Start timestamp: {start_timestamp}, End timestamp: {end_timestamp}")

    logger.debug(f"Fetching activity and sensor events for {identifier}...")
    with Timer() as a_timer:
        activity_events = mindlamp_api.get_activity_events_lamp(
            mindlamp_id, from_ts=start_timestamp, to_ts=end_timestamp
        )
    if len(activity_events) == 0:
        logger.debug(f"No activity events found for {identifier} on {date_str}")

    logger.debug(f"Fetching sensor events for {identifier}...")
    with Timer() as timer:
        sensor_events = mindlamp_api.get_sensor_events_lamp(
            mindlamp_id, from_ts=start_timestamp, to_ts=end_timestamp
        )
    if sensor_events:
        sensor_file_name = f"{mindlamp_id}_{subject_id}_sensor_{date_str}.json"
        sensor_file_path = subject_mindlamp_data_root / sensor_file_name
        with open(sensor_file_path, "w", encoding="utf-8") as f:
            json.dump(sensor_events, f, indent=4)
            logger.debug(f"Saved sensor events to {sensor_file_path}")

        sensors_file = File(
            file_path=sensor_file_path,
        )
        associated_files.append(sensors_file)

        sensor_data_pull = DataPull(
            subject_id=subject_id,
            data_source_name=mindlamp_data_source.data_source_name,
            site_id=mindlamp_data_source.site_id,
            project_id=mindlamp_data_source.project_id,
            file_path=str(sensor_file_path),
            file_md5=sensors_file.md5,  # type: ignore
            pull_time_s=int(timer.duration),  # type: ignore
            pull_metadata={
                "mindlamp_id": mindlamp_id,
                "mindlamp_data_type": "sensor",
                "data_date_utc": date_utc,
            },
        )
        data_pulls.append(sensor_data_pull)
    else:
        logger.debug(f"No sensor events found for {identifier} on {date_str}.")

    if activity_events:
        # get audio data from activity events
        logger.debug(f"Processing audio data for {identifier}...")
        audio_file_name_template = f"{mindlamp_id}_{subject_id}_activity_{date_str}"
        activity_dicts_wo_sound, audio_file_paths = extract_audio_from_activities(
            activity_dicts=activity_events,
            audio_data_root=subject_mindlamp_data_root,
            audio_file_name_template=audio_file_name_template,
        )
        activity_events = activity_dicts_wo_sound

        activity_file_name = f"{mindlamp_id}_{subject_id}_activity_{date_str}.json"
        activity_file_path = subject_mindlamp_data_root / activity_file_name
        with open(activity_file_path, "w", encoding="utf-8") as f:
            json.dump(activity_events, f, indent=4)
            logger.debug(f"Saved activity events to {activity_file_path}")

        activities_file = File(
            file_path=activity_file_path,
        )
        associated_files.append(activities_file)

        activity_data_pull = DataPull(
            subject_id=subject_id,
            data_source_name=mindlamp_data_source.data_source_name,
            site_id=mindlamp_data_source.site_id,
            project_id=mindlamp_data_source.project_id,
            file_path=str(activity_file_path),
            file_md5=activities_file.md5,  # type: ignore
            pull_time_s=int(a_timer.duration),  # type: ignore
            pull_metadata={
                "mindlamp_id": mindlamp_id,
                "mindlamp_data_type": "activity",
                "data_date_utc": date_utc,
            },
        )
        data_pulls.append(activity_data_pull)

        for audio_file in audio_file_paths:
            audio_file_o = File(
                file_path=audio_file,
            )
            associated_files.append(audio_file_o)

            audio_data_pull = DataPull(
                subject_id=subject_id,
                data_source_name=mindlamp_data_source.data_source_name,
                site_id=mindlamp_data_source.site_id,
                project_id=mindlamp_data_source.project_id,
                file_path=str(audio_file),
                file_md5=audio_file_o.md5,  # type: ignore
                pull_time_s=int(a_timer.duration),  # type: ignore
                pull_metadata={
                    "mindlamp_id": mindlamp_id,
                    "mindlamp_data_type": "audio_journals",
                    "data_date_utc": date_utc,
                },
            )
            data_pulls.append(audio_data_pull)

        logger.debug(f"Fetched {len(audio_file_paths)} audio files for {identifier}.")

    if data_pulls:
        queries: List[str] = []
        for file in associated_files:
            queries.extend(file.to_sql_queries_with_availability_update())
        queries += [data_pull.to_sql_query() for data_pull in data_pulls]
        db.execute_queries(
            config_file=config_file,
            queries=queries,
            show_commands=False,
        )

    return data_pulls


def pull_subject_data(
    mindlamp_data_source: MindLAMPDataSource,
    subject_id: str,
    start_date: datetime,
    end_date: datetime,
    force_start_date: Optional[datetime],
    force_end_date: Optional[datetime],
    config_file: Path,
) -> List[DataPull]:
    """
    Fetch data for a subject from MindLAMP.

    Args:
        mindlamp_data_source (MindLAMPDataSource): The MindLAMP data source object.
        subject_id (str): The subject ID.
        start_date (datetime): Start date for data fetch.
        end_date (datetime): End date for data fetch.
        force_start_date (Optional[datetime]): Start date for forced redownload.
        force_end_date (Optional[datetime]): End date for forced redownload.
        config_file (Path): Path to the configuration file.

    Returns:
        List[DataPull]: A list of data pulls for the subject.
    """
    data_pulls: List[DataPull] = []

    subject_mindlamp_id = get_mindlamp_id_for_subject(
        mindlamp_data_source=mindlamp_data_source,
        subject_id=subject_id,
        config_file=config_file,
    )

    if not subject_mindlamp_id:
        logger.warning(
            (
                f"MindLAMP ID not found for subject {subject_id} "
                f"in project {mindlamp_data_source.project_id} "
                f"and site {mindlamp_data_source.site_id}."
            )
        )
        return data_pulls

    # Drop time info from start/end dates
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    dates_to_fetch = find_missing_data_dates(
        subject_id=subject_id,
        mindlamp_ds=mindlamp_data_source,
        start_date=start_date,
        end_date=end_date,
        config_file=config_file,
    )

    dates_to_redownload_set: Set[datetime] = set()
    if force_start_date and force_end_date:
        force_start_date = force_start_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        force_end_date = force_end_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        dates_to_redownload = pd.date_range(
            start=force_start_date,
            end=force_end_date,
            freq="D",
        )
        dates_to_redownload_set = set(
            date.to_pydatetime() for date in dates_to_redownload
        )

    dates_to_download: Set[datetime] = dates_to_fetch.union(dates_to_redownload_set)

    subject_mindlamp_data_root = get_subject_mindlamp_data_root(
        subject_id=subject_id,
        mindlamp_data_source=mindlamp_data_source,
        config_file=config_file,
    )
    subject_mindlamp_data_root.mkdir(parents=True, exist_ok=True)

    logger.debug(
        f"Found {len(dates_to_download)} dates to fetch for subject {subject_id}."
    )
    for date_dt in sorted(dates_to_download):
        pulls = fetch_subject_data_for_date(
            mindlamp_data_source=mindlamp_data_source,
            subject_id=subject_id,
            mindlamp_id=subject_mindlamp_id,
            datetime_dt=date_dt,
            config_file=config_file,
            subject_mindlamp_data_root=subject_mindlamp_data_root,
        )
        data_pulls.extend(pulls)

    return data_pulls
