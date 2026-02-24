"""
REDCap API client.

Centralises all direct HTTP calls to the REDCap API
"""

import logging
import re
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class RedcapAPIError(Exception):
    """Raised when a REDCap API request fails.

    Attributes:
        status_code: The HTTP status code (if available).
        response_text: The body of the error response (if available).
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_text: Optional[str] = None,
    ):
        self.status_code = status_code
        self.response_text = response_text
        super().__init__(message)


def build_penncnb_filter_logic(subject_id: str, subject_id_var: str) -> str:
    """
    Enhances the existing filter logic for fetching data from REDCap by adding
    conditions to handle subject IDs with various suffix patterns used in the
    PennCNB REDCap project.

    This function appends additional logic to the provided filter logic string
    to accommodate different possible suffixes for subject IDs. These suffixes
    are used in REDCap to denote different sessions or versions of a subject's
    data.

    Args:
        filter_logic (str): The initial filter logic string to be enhanced.
        subject_id (str): The subject ID for which data is being fetched.
        subject_id_var (str): The variable name in REDCap that stores the
                            subject ID.

    Returns:
        str: The enhanced filter logic string with additional conditions
            included for handling various subject ID suffix patterns.
    """
    filter_logic = (
        f"[{subject_id_var}] = '{subject_id}' or "
        f"[{subject_id_var}] = '{subject_id.lower()}'"
    )

    digits_str = [str(x) for x in range(1, 10)]
    contains_logic: List[str] = []
    for sid in [subject_id, subject_id.lower()]:
        contains_logic += [
            f"contains([{subject_id_var}], '{sid}_{d}')" for d in digits_str
        ]
        contains_logic += [
            f"contains([{subject_id_var}], '{sid}={d}')" for d in digits_str
        ]

    filter_logic += f" or {' or '.join(contains_logic)}"
    return filter_logic


def export_records(
    endpoint_url: str,
    api_token: str,
    records: Optional[List[str]] = None,
    fields: Optional[List[str]] = None,
    filter_logic: Optional[str] = None,
    raw_or_label: str = "raw",
    raw_or_label_headers: str = "raw",
    export_checkbox_label: bool = False,
    export_survey_fields: bool = False,
    export_data_access_groups: bool = False,
    timeout_s: int = 60,
) -> bytes:
    """
    Export records from a REDCap project via the API.

    Args:
        endpoint_url: The REDCap API endpoint URL.
        api_token: The API token for authentication.
        records: Optional list of record IDs to export.
        fields: Optional list of field names to export.
        filter_logic: Optional REDCap filter-logic string.
        raw_or_label: ``"raw"`` or ``"label"``.
        raw_or_label_headers: ``"raw"`` or ``"label"``.
        export_checkbox_label: Whether to export checkbox labels.
        export_survey_fields: Whether to export survey fields.
        export_data_access_groups: Whether to export data-access groups.
        timeout_s: HTTP request timeout in seconds.

    Returns:
        The raw response body as ``bytes``.

    Raises:
        RedcapAPIError: On non-2xx HTTP responses or network failures.
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
        "rawOrLabelHeaders": raw_or_label_headers,
        "exportCheckboxLabel": str(export_checkbox_label).lower(),
        "exportSurveyFields": str(export_survey_fields).lower(),
        "exportDataAccessGroups": str(export_data_access_groups).lower(),
    }

    if records is not None:
        for i, rec in enumerate(records):
            data[f"records[{i}]"] = rec

    if fields is not None:
        for i, fld in enumerate(fields):
            data[f"fields[{i}]"] = fld

    if filter_logic is not None:
        data["filterLogic"] = filter_logic

    try:
        r = requests.post(endpoint_url, data=data, timeout=timeout_s)
    except requests.exceptions.RequestException as e:
        raise RedcapAPIError(
            f"Network error during record export: {e}",
        ) from e

    if not r.ok:
        raise RedcapAPIError(
            f"REDCap record export failed: {r.status_code} - {r.text}",
            status_code=r.status_code,
            response_text=r.text,
        )

    return r.content


def export_file(
    endpoint_url: str,
    api_token: str,
    record_id: str,
    field_name: str,
    event_name: Optional[str] = None,
    repeat_instance: Optional[str] = None,
    timeout_s: int = 60,
) -> tuple[bytes, str]:
    """
    Download a single file from a REDCap file-upload field.

    Args:
        endpoint_url: The REDCap API endpoint URL.
        api_token: The API token for authentication.
        record_id: The record ID (primary key) in REDCap.
        field_name: The field name of the file-upload field.
        event_name: The event name (for longitudinal projects).
        repeat_instance: The repeat instance number.
        timeout_s: HTTP request timeout in seconds.

    Returns:
        A ``(file_content, original_filename)`` tuple.

    Raises:
        RedcapAPIError: On non-2xx HTTP responses or network failures.
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
    except requests.exceptions.RequestException as e:
        raise RedcapAPIError(
            f"Network error during file export for record={record_id}, "
            f"field={field_name}: {e}",
        ) from e

    if not r.ok:
        raise RedcapAPIError(
            f"REDCap file export failed for record={record_id}, "
            f"field={field_name}: {r.status_code}",
            status_code=r.status_code,
            response_text=r.text,
        )

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


def export_data_dictionary(
    endpoint_url: str,
    api_token: str,
    timeout_s: int = 30,
) -> List[Dict[str, Any]]:
    """
    Export the data dictionary (field metadata) from a REDCap project.

    Args:
        endpoint_url: The REDCap API endpoint URL.
        api_token: The API token for authentication.
        timeout_s: HTTP request timeout in seconds.

    Returns:
        A list of dictionaries, one per field in the data dictionary.

    Raises:
        RedcapAPIError: On non-2xx HTTP responses or network failures.
    """
    data: Dict[str, str] = {
        "token": api_token,
        "content": "metadata",
        "format": "json",
        "returnFormat": "json",
    }

    try:
        r = requests.post(endpoint_url, data=data, timeout=timeout_s)
    except requests.exceptions.RequestException as e:
        raise RedcapAPIError(
            f"Network error during data-dictionary export: {e}",
        ) from e

    if not r.ok:
        raise RedcapAPIError(
            f"REDCap data-dictionary export failed: {r.status_code} - {r.text}",
            status_code=r.status_code,
            response_text=r.text,
        )

    return r.json()  # type: ignore[no-any-return]
