"""
Module for REDCap API interactions.

All functions accept API credentials (api_token, endpoint_url) directly
as parameters, keeping this layer free of config-file dependencies.
"""

import logging
import re
from typing import Any, Dict, List, Optional
import json

import requests

logger = logging.getLogger(__name__)


def export_records(
    api_token: str,
    endpoint_url: str,
    filter_logic: Optional[str] = None,
    records: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
    raw_or_label: str = "raw",
    export_survey_fields: bool = False,
    export_data_access_groups: bool = False,
    timeout_s: int = 60,
) -> Optional[List[Dict[str, Any]]]:
    """
    Export records from a REDCap project.

    Supports filtering either by explicit record IDs or by REDCap filter
    logic.  When *records* is provided the ``records[i]`` API parameters
    are sent; when *filter_logic* is provided the ``filterLogic``
    parameter is sent.  If neither is given, **all** records are exported.

    Args:
        api_token: REDCap API token.
        endpoint_url: REDCap API endpoint URL.
        filter_logic: REDCap filter-logic expression (optional).
        records: List of record IDs to export (optional).
        fields: List of field names to export (optional).  When ``None``
            all fields are returned.
        raw_or_label: ``"raw"`` (default) or ``"label"``.
        export_survey_fields: Include survey timestamp / identifier fields.
        export_data_access_groups: Include the DAG field.
        timeout_s: HTTP request timeout in seconds.

    Returns:
        The raw response body (bytes), or ``None`` when the request fails
        or the response is empty.

    Raises:
        requests.exceptions.RequestException: Propagated when the HTTP
            request itself fails (timeout, connection error, etc.).
    """
    data: Dict[str, Any] = {
        "token": api_token,
        "content": "record",
        "action": "export",
        "format": "json",
        "type": "flat",
        "returnFormat": "json",
        "csvDelimiter": "",
        "rawOrLabel": raw_or_label,
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": str(export_survey_fields).lower(),
        "exportDataAccessGroups": str(export_data_access_groups).lower(),
    }

    if records is not None:
        for idx, record_id in enumerate(records):
            data[f"records[{idx}]"] = record_id

    if fields is not None:
        for idx, field_name in enumerate(fields):
            data[f"fields[{idx}]"] = field_name

    if filter_logic:
        data["filterLogic"] = filter_logic

    try:
        r = requests.post(endpoint_url, data=data, timeout=timeout_s)
        r.raise_for_status()

        if r.content in (b"", b"[]"):
            logger.debug("REDCap returned empty response for export_records.")
            return None

        record_data = json.loads(r.content)
        return record_data
    except requests.exceptions.RequestException as e:
        logger.error("Failed to export records from REDCap: %s", e)
        raise


def export_metadata(
    api_token: str,
    endpoint_url: str,
    timeout_s: int = 30,
) -> Optional[List[Dict[str, Any]]]:
    """
    Export the data dictionary (metadata) for a REDCap project.

    Args:
        api_token: REDCap API token.
        endpoint_url: REDCap API endpoint URL.
        timeout_s: HTTP request timeout in seconds.

    Returns:
        A list of dictionaries representing each field in the data
        dictionary, or ``None`` if the request fails.

    Raises:
        requests.exceptions.RequestException: Propagated when the HTTP
            request itself fails.
    """
    data: Dict[str, str] = {
        "token": api_token,
        "content": "metadata",
        "format": "json",
        "returnFormat": "json",
    }

    try:
        r = requests.post(endpoint_url, data=data, timeout=timeout_s)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        logger.error("Failed to export metadata from REDCap: %s", e)
        raise


def export_file(
    api_token: str,
    endpoint_url: str,
    record_id: str,
    field_name: str,
    event_name: Optional[str] = None,
    repeat_instance: Optional[str] = None,
    timeout_s: int = 60,
) -> Optional[tuple]:
    """
    Download a file from a REDCap file-upload field.

    Args:
        api_token: REDCap API token.
        endpoint_url: REDCap API endpoint URL.
        record_id: The record ID (primary key) in REDCap.
        field_name: The field name of the file-upload field.
        event_name: Event name for longitudinal projects (optional).
        repeat_instance: Repeat-instance number (optional).
        timeout_s: HTTP request timeout in seconds.

    Returns:
        A ``(file_content_bytes, original_filename)`` tuple, or ``None``
        if the download fails.
    """
    data: Dict[str, str] = {
        "token": api_token,
        "content": "file",
        "action": "export",
        "record": record_id,
        "field": field_name,
        "returnFormat": "json",
    }
    if event_name:
        data["event"] = event_name
    if repeat_instance:
        data["repeat_instance"] = repeat_instance

    try:
        r = requests.post(endpoint_url, data=data, timeout=timeout_s)
        r.raise_for_status()

        # REDCap returns the filename in the Content-Type header as:
        #   application/pdf; name="filename.pdf"
        filename: Optional[str] = None
        content_type = r.headers.get("Content-Type", "")
        name_match = re.search(r'name="([^"]+)"', content_type)
        if name_match:
            filename = name_match.group(1)

        if not filename:
            filename = f"{field_name}_file"

        return r.content, filename
    except requests.exceptions.RequestException as e:
        logger.error(
            "Failed to download file for record=%s, field=%s: %s",
            record_id,
            field_name,
            e,
        )
        return None


def export_log(
    api_token: str,
    endpoint_url: str,
    record_id: str,
    timeout_s: int = 30,
) -> Optional[List[Dict[str, Any]]]:
    """
    Export the data change log for a specific record.

    Args:
        api_token: REDCap API token.
        endpoint_url: REDCap API endpoint URL.
        record_id: The record ID to retrieve the log for.
        timeout_s: HTTP request timeout in seconds.
    Returns:
        A list of log entries (dictionaries), or ``None`` if the request fails.
    """
    data: Dict[str, str] = {
        "token": api_token,
        "content": "log",
        "record": record_id,
        "format": "json",
        "returnFormat": "json",
    }

    try:
        r = requests.post(endpoint_url, data=data, timeout=timeout_s)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        logger.error("Failed to export log for record=%s: %s", record_id, e)
        return None
