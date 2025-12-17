"""
Utility functions for SharePoint data source interactions.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Literal, Optional, Tuple

import requests

from lochness.helpers import db
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.models.logs import Logs
from lochness.sources.sharepoint import api as sharepoint_api

logger = logging.getLogger(__name__)


def log_event(
    config_file: Path,
    log_level: Literal["DEBUG", "INFO", "WARN", "ERROR", "FATAL"],
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
        "data_source_type": "sharepoint",
        "module": __name__,
    }
    if data_source_identifier:
        log_message["data_source_identifier"] = data_source_identifier
    if extra:
        log_message.update(extra)
    Logs(
        log_level=log_level,
        log_message=log_message,
    ).insert(config_file)


def find_folder_in_drive(
    drive_id: str, folder_name: str, headers: Dict, timeout: int = 120
) -> Optional[Dict]:
    """
    Find a folder by name in the root of a drive.

    Args:
        drive_id (str): The SharePoint drive ID.
        folder_name (str): The name of the folder to find.
        headers (Dict): Headers containing the access token.
        timeout (int): Request timeout in seconds.
    Returns:
        Optional[Dict]: The folder item if found, else None.
    """
    for item in sharepoint_api.list_drive_root(drive_id, headers, timeout=timeout):
        if item.get("name", "").lower() == folder_name.lower() and "folder" in item:
            logger.info(f"Found folder '{folder_name}' in drive {drive_id}")
            return item
    logger.warning(f"Folder '{folder_name}' not found in drive {drive_id}")
    return None


def find_subfolder(
    drive_id: str,
    parent_id: str,
    subfolder_name: str,
    headers: Dict[str, str],
    timeout: int = 120,
) -> Optional[Dict]:
    """
    Find a subfolder by name within a specified parent folder.

    Args:
        drive_id (str): The SharePoint drive ID.
        parent_id (str): The parent folder ID.
        subfolder_name (str): The name of the subfolder to find.
        headers (Dict[str, str]): Headers containing the access token.
        timeout (int): Request timeout in seconds.
    Returns:
        Optional[Dict]: The subfolder item if found, else None.
    """
    for item in sharepoint_api.list_folder_items(
        drive_id, parent_id, headers, timeout=timeout
    ):
        if item.get("name", "").lower() == subfolder_name.lower() and "folder" in item:
            logger.info(f"Found subfolder '{subfolder_name}' in parent {parent_id}")
            return item

    logger.warning(f"Subfolder '{subfolder_name}' not found in parent {parent_id}")
    return None


def find_subfolders(
    drive_id: str,
    parent_id: str,
    subfolder_name_patt: str,
    headers: Dict[str, str],
    timeout: int = 120,
) -> Optional[List[Dict]]:
    """
    Find subfolders by subfolder name pattern within a specified parent folder.

    Args:
        drive_id (str): The SharePoint drive ID.
        parent_id (str): The parent folder ID.
        subfolder_name_patt (str): The name pattern of the subfolders to find.
        headers (Dict[str, str]): Headers containing the access token.
        timeout (int): Request timeout in seconds.
    Returns:
        Optional[Dict]: The subfolder item if found, else None.
    """
    subfolders = []
    for item in sharepoint_api.list_folder_items(
        drive_id, parent_id, headers, timeout=timeout
    ):
        if subfolder_name_patt.lower() in item.get("name", "").lower() and "folder" in item:
            logger.info(f"Found subfolder '{subfolder_name_patt}' in parent {parent_id}")
            subfolders.append(item)

    if subfolders == "":
        logger.warning(f"Subfolder matching '{subfolder_name_patt}' pattern not found in parent {parent_id}")
        return None

    return subfolders

def get_matching_subfolders(
    drive_id: str,
    ms_folder_dict: Dict,
    subdir_name: str,
    headers: Dict[str, str],
    timeout: int = 120,
    relaxed_search: bool = False
) -> List[Dict]:
    """
    Get subfolders matching a specific form name within the 'Responses' folder.

    Args:
        drive_id (str): The SharePoint drive ID.
        ms_folder_dict (Dict): The Sharepoint folder item.
        subdir_name (str): The name of the form to match subfolders.
        headers (Dict[str, str]): Headers containing the access token.
        timeout (int): Request timeout in seconds.
        relaxed_search (bool): Find patterns that include the subdir_name instead of exact match.

    Returns:
        List[Dict]: A list of matching subfolders.

    Raises:
        RuntimeError: If the 'Responses' folder or matching form folder is not found.
    """
    if relaxed_search:
        subfolders = find_subfolders(
            drive_id, ms_folder_dict["id"], subdir_name,
            headers,
            timeout=timeout
        )
        if not subfolders:
            logger.info(f"No subfolders found in '{subdir_name}'.")
        else:
            logger.info(f"Found {len(subfolders)} subfolders for form:'{subdir_name}'.")
        return subfolders

    matching_folder = find_subfolder(
        drive_id, ms_folder_dict["id"], subdir_name, headers, timeout=timeout
    )

    if not matching_folder:
        raise RuntimeError(f"'{subdir_name}' folder not found inside {ms_folder_dict['name']}.")

    subfolders = sharepoint_api.list_folder_items(
        drive_id, matching_folder["id"], headers, timeout=timeout
    )
    if not subfolders:
        logger.info(f"No subfolders found in '{subdir_name}'.")
    else:
        logger.info(f"Found {len(subfolders)} subfolders for form:'{subdir_name}'.")

    return subfolders


def find_drive_by_name(drives: List[Dict], name: str) -> Optional[Dict]:
    """
    Find a drive by its name from a list of drives.

    Args:
        drives (List[Dict]): A list of drive metadata dictionaries.
        name (str): The name of the drive to find.
    Returns:
        Optional[Dict]: The drive metadata if found, else None.
    """
    drive = next((d for d in drives if d["name"].lower() == name.lower()), None)
    if drive:
        logger.info(f"Found drive: {name}")
    else:
        logger.warning(f"Drive not found: {name}")
    return drive


def should_download_file(local_file_path: Path, quick_xor_hash: Optional[str]) -> bool:
    """
    Determines whether a file should be downloaded based on its local presence and hash.

    TODO: Currently checks for a local .quickxorhash file. In the future, consider storing
    the hash in a database or metadata file for better integrity checks.

    Returns True if the file should be downloaded, False otherwise.

    Args:
        local_file_path (Path): The path to the local file.
        quick_xor_hash (Optional[str]): The QuickXorHash from the remote file metadata.

    Returns:
        bool: True if the file should be downloaded, False otherwise.
    """
    hash_file_path = local_file_path.parent / (
        "." + local_file_path.name + ".quickxorhash"
    )

    if local_file_path.is_file() and hash_file_path.is_file():
        with open(hash_file_path, "r", encoding="utf-8") as hf:
            local_hash = hf.read().strip()
        if quick_xor_hash and local_hash == quick_xor_hash:
            logger.info(
                f"QuickXorHashes match for {local_file_path}. Skipping download."
            )
            return False
        else:
            logger.info(
                f"QuickXorHashes mismatch or remote hash not available for {local_file_path}. "
                "Re-downloading."
            )
            return True

    if local_file_path.is_file():
        logger.info(
            f"Local file exists but no hash file for {local_file_path}. Re-downloading."
        )
    else:
        logger.info(f"Local file does not exist. Downloading {local_file_path}.")

    return True


def extract_info(
    json_path: Path,
    date_str: str,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Extract ampsczSubjectId and formTitle from a response.submitted.json file.

    Args:
        json_path (Path): Path to the response.submitted.json file.

    Returns:
        Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        - subject_id: The extracted ampsczSubjectId or None if not found.
        - form_title: The extracted formTitle or None if not found.
        - dt_str: The extracted dateOfEgg in "YYYY_MM_DD" format or None if not found.
        - timestamp: The extracted timestamp or None if not found.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    subject_id = None
    form_title = None
    dt_str = None
    timestamp = None

    try:
        form_title = data.get("formTitle")
        subject_id = data.get("data", {}).get("data", {}).get("ampsczSubjectId")
        timestamp = data.get("timestamp")
    except Exception:  # pylint: disable=broad-except
        pass

    try:
        # date_str = data.get("data", {}).get("data", {}).get("dateOfEeg")
        # date_str = data.get("data", {}).get("data", {}).get("dateOfAudio")
        date_str = data.get("data", {}).get("data", {}).get(date_str)
        dt = datetime.fromisoformat(date_str)
        # date_only = dt.date()
        dt_str = dt.strftime("%Y_%m_%d")
    except Exception:  # pylint: disable=broad-except
        # date_only = None
        pass

    return subject_id, form_title, dt_str, timestamp


def download_subdirectory(
    files: List[Dict],
    subject_id: str,
    site_id: str,
    project_id: str,
    data_source_name: str,
    output_dir: Path,
    config_file: Path,
) -> None:
    """
    Download updated files to the output_dir and clean up previous files

    Args:
        files (list): List of file metadata dictionaries from SharePoint.
        subject_id (str): The subject ID associated with the files.
        site_id (str): The site ID for logging purposes.
        project_id (str): The project ID for logging purposes.
        data_source_name (str): The data source name for logging purposes.
        output_dir (Path): The directory to save downloaded files.
        config_file (Path): Path to the configuration file for database operations.
    Returns:
        None
    Raises:
        RuntimeError: If a file download fails.
    """

    # Label previously downloaded files that are now removed from the form
    filename_list = [x["name"] for x in files]
    removed_file_paths = [
        x for x in output_dir.glob("*") if x.name not in filename_list
    ]

    for removed_file_path in removed_file_paths:
        file_model = File(file_path=removed_file_path)
        file_model.md5 = "DELETED_FROM_TEAMS_FORM"
        db.execute_queries(
            config_file, [file_model.to_sql_query()], show_commands=False
        )

    # Download all other files
    for f in files:
        logger.debug(f"file: {f}")
        if "file" not in f:
            continue

        file_name = f["name"]
        file_info = f.get("file", {})
        quick_xor_hash = file_info.get("hashes", {}).get("quickXorHash")
        local_file_path = output_dir / file_name
        hash_file_path = output_dir / ("." + file_name + ".quickxorhash")

        file_target_path = output_dir / file_name

        if should_download_file(local_file_path, quick_xor_hash):
            download_url = f.get("@microsoft.graph.downloadUrl")
            if download_url:
                start_time = datetime.now()
                download_file(download_url, file_target_path)
                file_model = File(file_path=file_target_path, with_hash=True)
                file_md5: str = file_model.md5  # type: ignore

                # Save the QuickXorHash to a hidden file
                with open(hash_file_path, "w", encoding="utf-8") as hf:
                    hf.write(quick_xor_hash)

                hash_file_model = File(file_path=hash_file_path)

                msg = (
                    f"Successfully saved data for {subject_id} "
                    f"to {file_target_path}."
                )
                logger.info(msg)
                log_event(
                    config_file=config_file,
                    log_level="INFO",
                    event="sharepoint_data_pull_save_success",
                    message=msg,
                    project_id=project_id,
                    site_id=site_id,
                    data_source_name=data_source_name,
                    subject_id=subject_id,
                    extra={
                        "file_path": str(file_target_path),
                        "file_md5": file_md5,
                        "quickxorhash": quick_xor_hash,
                    },
                )
                end_time = datetime.now()
                pull_time_s = int((end_time - start_time).total_seconds())

                data_pull = DataPull(
                    subject_id=subject_id,
                    data_source_name=data_source_name,
                    site_id=site_id,
                    project_id=project_id,
                    file_path=str(file_target_path),
                    file_md5=file_md5,
                    pull_time_s=pull_time_s,
                    pull_metadata={"quickxorhash": quick_xor_hash},
                )

                queries = [
                    file_model.to_sql_query(),
                    hash_file_model.to_sql_query(),
                    data_pull.to_sql_query(),
                ]
                db.execute_queries(config_file, queries, show_commands=False)


def download_file(download_url: str, local_path: Path) -> None:
    """
    Downloads a file from a SharePoint URL to a local path.

    Args:
        download_url (str): The URL to download the file from.
        local_path (Path): The local path to save the downloaded file.

    Returns:
        None
    Raises:
        RuntimeError: If the download fails.
    """
    logger.info(f"Downloading {local_path}...")
    resp = requests.get(download_url, timeout=30)
    if resp.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(resp.content)
        logger.info(f"Downloaded to {local_path}")
    else:
        logger.error(f"Failed to download file: {local_path} (HTTP {resp.status_code})")
        raise RuntimeError(
            f"Failed to download file: {local_path} (HTTP {resp.status_code})"
        )


def is_response_json_updated(
    response_json_file: Dict,
    subfolder_name: str,
    subject_id: str,
    form_name: str,
    date_str: str,
    potential_file_uploads_without_form_update: bool,
    output_dir: Path,
) -> Optional[Path]:
    """
    Investigates the response json to determine and execute the download
    """
    # Download response.submitted.json to temp location
    with TemporaryDirectory() as tmpdirname:
        # Create a Path object for easier path manipulation
        temp_dir_path = Path(tmpdirname)

        # Define the path for a temporary file within this directory
        temp_file_path = (
            temp_dir_path / "response.submitted.json"
        )  # Write content to the temporary file

        download_url = response_json_file.get("@microsoft.graph.downloadUrl")
        if not download_url:
            logger.warning(
                f"No download URL for response.submitted.json in {subfolder_name}, skipping."
            )
            return None

        download_file(download_url, temp_file_path)

        form_subject_id, form_title, dt_str, timestamp = extract_info(
                temp_file_path, date_str)

        if form_subject_id != subject_id:
            logger.info(
                "Skipping subfolder "
                f"{subfolder_name} "
                "(subject_id "
                f"{form_subject_id} "
                "!= "
                f"{subject_id})"
            )
            return None

        if form_title != form_name:
            logger.info(
                f"Skipping subfolder {subfolder_name} (form_title {form_title} != {form_name})"
            )
            return None

        # Now set the final local path
        # Move response.submitted.json to the new location
        logger.info(f"Found matching form - {form_name} - {subject_id}")

        if not dt_str:
            dt_str = "undated"
            logger.warning(
                f"No valid date found in response.submitted.json in "
                f"{subfolder_name}, using 'undated'."
            )

        output_dir = output_dir / dt_str
        if potential_file_uploads_without_form_update:
            return output_dir

        response_json_local = output_dir / "response.submitted.json"
        timestamp_pre = None
        if response_json_local.is_file():
            _, _, _, timestamp_pre = extract_info(response_json_local,
                                                  date_str)
            logger.debug("Response json exists: %s", timestamp_pre)
        else:
            logger.debug(
                "A new response is found: %s %s %s",
                form_subject_id,
                form_title,
                dt_str,
            )

        if timestamp == timestamp_pre:
            logger.debug(
                "No update in the response forms: %s %s %s",
                form_subject_id,
                form_title,
                dt_str,
            )
            return None
        else:
            # new or updated response
            return output_dir


def download_new_or_updated_files(
    subfolder: Dict,
    drive_id: str,
    headers: Dict[str, str],
    form_name: str,
    date_str: str,
    subject_id: str,
    site_id: str,
    project_id: str,
    data_source_name: str,
    output_dir_root: Path,
    potential_file_uploads_without_form_update: bool,
    config_file: Path,
) -> None:
    """
    Download all files under the subfolder from a submitted form

    Args:
        subfolder (Dict): The subfolder metadata dictionary from SharePoint.
        drive_id (str): The ID of the SharePoint drive.
        headers (Dict[str, str]): Headers for authentication in API requests.
        form_name (str): The name of the form to filter submissions.
        subject_id (str): The subject ID to filter submissions.
        site_id (str): The site ID for logging purposes.
        project_id (str): The project ID for logging purposes.
        data_source_name (str): The data source name for logging purposes.
        output_dir_root (Path): The root directory to save downloaded files.
        config_file (Path): Path to the configuration file for database operations.

    Returns:
        None
        None
    Raises:
        RuntimeError: If a file download fails.
    """
    subfolder_name = subfolder["name"]
    subfolder_id = subfolder["id"]
    logger.info("Found subfolder: %s", subfolder_name)

    files = sharepoint_api.list_folder_items(drive_id, subfolder_id, headers)
    response_json_file = next(
        (f for f in files if f.get("name") == "response.submitted.json"), None
    )
    if not response_json_file:
        logger.debug(
            "No response.submitted.json found in %s, skipping.", subfolder_name
        )
        return

    # reponse needs to be downloaded each time as it does not have checksum
    output_dir = is_response_json_updated(
        response_json_file, subfolder_name, subject_id,
        form_name, date_str, potential_file_uploads_without_form_update,
        output_dir_root
    )

    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        download_subdirectory(
            files,
            subject_id,
            site_id,
            project_id,
            data_source_name,
            output_dir,
            config_file=config_file,
        )
