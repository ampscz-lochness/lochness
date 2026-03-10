"""
Utility functions for REDCap data processing.
"""

from typing import Any, Dict, List, Optional

from lochness.sources.redcap.models.data_source import RedcapDataSource


def get_redcap_identifier_fields(
    data_dictionary: List[Dict[str, Any]],
) -> List[str]:
    """
    Extracts the list of identifier fields from the REDCap data dictionary.

    Args:
        data_dictionary (List[Dict[str, Any]]): The REDCap data dictionary.
    Returns:
        List[str]: A list of field names that are marked as identifiers.
    """
    identifier_fields = []
    for field in data_dictionary:
        if field.get("identifier") == "y":
            identifier_fields.append(field["field_name"])
    return identifier_fields


def redact_identifiers(
    data: List[Dict[str, Any]],
    identifier_fields: List[str],
    redacted_string: str = "lochness_redacted",
) -> List[Dict[str, Any]]:
    """
    Redacts specified identifier fields in a list of dictionaries.

    Args:
        data (List[Dict[str, Any]]): The data to redact.
        identifier_fields (List[str]): The fields to redact.

    Returns:
        List[Dict[str, Any]]: The redacted data.
    """
    redacted_data = []

    for record in data:
        redacted_record = record.copy()
        for field in identifier_fields:
            if field in redacted_record:
                # Check if variable has value before redacting
                if redacted_record[field] is not None and redacted_record[field] != "":
                    redacted_record[field] = redacted_string
        redacted_data.append(redacted_record)

    return redacted_data


def get_file_fields_from_dictionary(
    redcap_data_source: RedcapDataSource,
) -> Dict[str, str]:
    """
    Identifies file upload fields from the REDCap data dictionary stored in
    the database.  Returns a mapping of field_name : form_name.

    Args:
        redcap_data_source (RedcapDataSource): The REDCap data source.

    Returns:
        Dict[str, str]: Mapping of field_name → form_name for every file
            upload field.  Empty dict when no dictionary is available or
            no file fields exist.
    """

    data_dictionary: Optional[List[Dict[str, str]]] = (
        redcap_data_source.data_source_metadata.dictionary
    )

    if data_dictionary is None:
        return {}

    # field_name: form_name for every file field
    file_fields: Dict[str, str] = {
        entry["field_name"]: entry.get("form_name", "unknown_form")
        for entry in data_dictionary
        if entry.get("field_type") == "file"
    }

    return file_fields

_identifier_fields_cache: Dict[str, List[str]] = {}


def get_identifier_fields_from_data_source(
    redcap_data_source: RedcapDataSource,
) -> List[str]:
    """
    Identifies identifier fields from the REDCap data dictionary stored in
    the database.  Returns a list of field names that are marked as
    identifiers.

    Args:
        redcap_data_source (RedcapDataSource): The REDCap data source.
    Returns:
        List[str]: A list of field names that are marked as identifiers.
    """
    cache_key = (
        f"{redcap_data_source.project_id}::{redcap_data_source.site_id}"
        f"::{redcap_data_source.data_source_name}"
    )
    if cache_key in _identifier_fields_cache:
        return _identifier_fields_cache[cache_key]

    data_dictionary: Optional[List[Dict[str, str]]] = (
        redcap_data_source.data_source_metadata.dictionary
    )

    if data_dictionary is None:
        return []

    identifier_fields: List[str] = get_redcap_identifier_fields(
        data_dictionary=data_dictionary
    )

    _identifier_fields_cache[cache_key] = identifier_fields
    return identifier_fields
