"""
Utility functions for Terra data source interactions.
"""

import csv
import fnmatch
import hashlib
import io
import json
import logging
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Literal, Optional, Tuple

from lochness.helpers import config, db, fs
from lochness.models.data_pulls import DataPull
from lochness.models.files import File
from lochness.models.logs import Logs
from lochness.models.subjects import Subject
from lochness.sources.terra import api as terra_api

logger = logging.getLogger(__name__)

VERSION_SIDECAR_SUFFIX = ".terraversion"


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
    Standardized database logging for Terra data source events.
    """
    data_source_identifier = (
        f"{project_id}::{site_id}::{data_source_name}"
        if project_id and site_id and data_source_name
        else None
    )

    log_message: Dict[str, Any] = {
        "event": event,
        "message": message,
        "project_id": project_id,
        "site_id": site_id,
        "subject_id": subject_id,
        "data_source_type": "terra",
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


def build_subject_output_dir(
    lochness_root: str,
    project_id: str,
    site_id: str,
    subject_id: str,
    modality: str = "terra",
) -> Path:
    """
    Build the standardized PHOENIX output directory for a subject and modality.
    """
    project_name_cap = (
        project_id[:1].upper() + project_id[1:].lower() if project_id else project_id
    )
    return (
        Path(lochness_root)
        / project_name_cap
        / "PHOENIX"
        / "PROTECTED"
        / f"{project_name_cap}{site_id}"
        / "raw"
        / subject_id
        / modality
    )


def resolve_terra_subject_id(
    subject: Subject,
    metadata_key: Optional[str] = None,
) -> Optional[str]:
    """
    Resolve the Terra identifier corresponding to a Lochness Subject.

    If ``metadata_key`` is not provided, returns ``subject.subject_id``.
    If ``metadata_key`` is provided (supports dot-notation such as
    ``"terra.participant_id"``), looks up the value in ``subject.subject_metadata``.
    Returns ``None`` if the configured metadata key is missing or empty.
    """
    if not metadata_key:
        return subject.subject_id

    current: Any = subject.subject_metadata
    for part in metadata_key.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None

    if current is None:
        return None

    value = str(current).strip()
    return value if value else None


def extract_entity_subject_id(
    entity: Dict[str, Any],
    subject_id_column: str = "name",
) -> Optional[str]:
    """
    Extract the subject identifier from a Terra entity record.

    Supports:
    - ``subject_id_column == "name"`` -> ``entity["name"]``
    - Primitive attribute values -> ``str(value)``
    - Terra entity references (``{"entityType": ..., "entityName": "..."}``) -> ``entityName``
    """
    if subject_id_column == "name":
        raw_name = entity.get("name")
        if raw_name is None:
            return None
        value = str(raw_name).strip()
        return value if value else None

    attributes = entity.get("attributes")
    if not isinstance(attributes, dict):
        return None

    raw_value = attributes.get(subject_id_column)
    if raw_value is None:
        return None

    if isinstance(raw_value, dict):
        entity_name = raw_value.get("entityName")
        if entity_name is None:
            return None
        value = str(entity_name).strip()
        return value if value else None

    value = str(raw_value).strip()
    return value if value else None


def group_entities_by_subject(
    entities: List[Dict[str, Any]],
    subject_id_column: str = "name",
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Group Terra entity rows by extracted subject ID, sorting each subject's
    rows by entity name for deterministic output.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for entity in entities:
        subject_key = extract_entity_subject_id(
            entity, subject_id_column=subject_id_column
        )
        if not subject_key:
            continue
        grouped.setdefault(subject_key, []).append(entity)

    for rows in grouped.values():
        rows.sort(key=lambda item: str(item.get("name", "")))

    return grouped


def _collect_gcs_uris(value: Any, found: List[str]) -> None:
    """Recursively collect valid ``gs://`` object URIs from a Terra attribute value."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("gs://"):
            try:
                terra_api.parse_gcs_uri(stripped)
                found.append(stripped)
            except ValueError:
                pass
    elif isinstance(value, (list, tuple)):
        for item in value:
            _collect_gcs_uris(item, found)
    elif isinstance(value, dict) and isinstance(value.get("items"), list):
        # Terra typed list representation: {"itemsType": "AttributeValue", "items": [...]}
        _collect_gcs_uris(value["items"], found)


def extract_gcs_uris_from_entity(
    entity: Dict[str, Any],
    gcs_uri_columns: Optional[List[str]] = None,
) -> List[Tuple[str, str]]:
    """
    Extract ``(attribute_name, gcs_uri)`` pairs from a Terra entity's attributes.

    Args:
        entity (Dict[str, Any]): Terra entity dictionary.
        gcs_uri_columns (Optional[List[str]]): Specific attribute names to scan.
            If None, scans all attributes in sorted key order.

    Returns:
        List[Tuple[str, str]]: Deduplicated list of ``(attribute_name, gcs_uri)``.
    """
    attributes = entity.get("attributes")
    if not isinstance(attributes, dict):
        return []

    if gcs_uri_columns is not None:
        keys = [k for k in gcs_uri_columns if k in attributes]
    else:
        keys = sorted(attributes.keys())

    seen_uris = set()
    results: List[Tuple[str, str]] = []
    for key in keys:
        found: List[str] = []
        _collect_gcs_uris(attributes[key], found)
        for uri in found:
            if uri not in seen_uris:
                seen_uris.add(uri)
                results.append((key, uri))

    return results


def matches_file_patterns(
    blob_name: str,
    include_patterns: Optional[List[str]] = None,
    exclude_patterns: Optional[List[str]] = None,
) -> bool:
    """
    Return True if ``blob_name`` satisfies optional include/exclude glob patterns.

    Patterns are matched against both the full blob key and its basename.
    """
    basename = PurePosixPath(blob_name).name

    if include_patterns:
        included = any(
            fnmatch.fnmatch(blob_name, pattern) or fnmatch.fnmatch(basename, pattern)
            for pattern in include_patterns
        )
        if not included:
            return False

    if exclude_patterns:
        excluded = any(
            fnmatch.fnmatch(blob_name, pattern) or fnmatch.fnmatch(basename, pattern)
            for pattern in exclude_patterns
        )
        if excluded:
            return False

    return True


def get_remote_blob_version(blob: Any) -> Optional[str]:
    """
    Derive a deterministic version identifier for a GCS blob.

    Handles both standard objects (``md5_hash``) and composite GCS objects
    (where ``md5_hash`` is None and ``crc32c`` is populated).
    """
    md5_hash = getattr(blob, "md5_hash", None)
    if md5_hash:
        return f"md5:{md5_hash}"

    crc32c = getattr(blob, "crc32c", None)
    if crc32c:
        return f"crc32c:{crc32c}"

    generation = getattr(blob, "generation", None)
    if generation is not None:
        return f"gen:{generation}"

    etag = getattr(blob, "etag", None)
    if etag:
        return f"etag:{etag}"

    return None


def compute_content_version(content: bytes) -> str:
    """Compute a deterministic SHA-256 version identifier for serialized bytes."""
    digest = hashlib.sha256(content).hexdigest()
    return f"sha256:{digest}"


def serialize_entities(
    entities: List[Dict[str, Any]],
    table_format: Literal["json", "tsv", "csv"] = "json",
) -> bytes:
    """
    Deterministically serialize a list of Terra entity records into JSON, TSV, or CSV.
    """
    sorted_entities = sorted(entities, key=lambda item: str(item.get("name", "")))

    if table_format == "json":
        return json.dumps(sorted_entities, sort_keys=True, indent=2).encode("utf-8")

    delimiter = "\t" if table_format == "tsv" else ","
    attribute_keys = sorted(
        {
            key
            for entity in sorted_entities
            if isinstance(entity.get("attributes"), dict)
            for key in entity["attributes"]
        }
    )
    fieldnames = ["name", "entityType", *attribute_keys]

    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=fieldnames,
        delimiter=delimiter,
        lineterminator="\n",
    )
    writer.writeheader()

    for entity in sorted_entities:
        row: Dict[str, Any] = {
            "name": entity.get("name", ""),
            "entityType": entity.get("entityType", ""),
        }
        attrs = entity.get("attributes", {})
        if isinstance(attrs, dict):
            for key in attribute_keys:
                val = attrs.get(key, "")
                if isinstance(val, (dict, list)):
                    row[key] = json.dumps(val, sort_keys=True)
                elif val is None:
                    row[key] = ""
                else:
                    row[key] = val
        writer.writerow(row)

    return buffer.getvalue().encode("utf-8")


def safe_relative_blob_path(blob_name: str, strip_prefix: str = "") -> Path:
    """
    Convert a GCS blob name into a safe relative ``Path`` with no traversal components.

    Raises:
        ValueError: If the resulting path is empty or contains ``..`` segments.
    """
    rel = blob_name
    if strip_prefix and rel.startswith(strip_prefix):
        rel = rel[len(strip_prefix) :]

    rel = rel.lstrip("/")
    posix_path = PurePosixPath(rel)
    parts = [part for part in posix_path.parts if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        raise ValueError(f"Unsafe or empty relative GCS blob path: {blob_name!r}")

    return Path(*parts)


def get_version_sidecar_path(local_file_path: Path) -> Path:
    """Return the hidden sidecar path storing the remote Terra version token."""
    return local_file_path.parent / f".{local_file_path.name}{VERSION_SIDECAR_SUFFIX}"


def is_pulled_and_pushed(
    local_file_path: Path,
    remote_version: Optional[str],
    config_file: Path,
    project_id: str,
    site_id: str,
) -> bool:
    """
    Check the database to determine whether this exact remote version was already
    pulled and pushed to every active sink (and potentially cleaned up locally).
    """
    if not remote_version:
        return False

    data_pull = DataPull.get_most_recent_data_pull_for_file_path(
        config_file=config_file,
        file_path=str(local_file_path),
    )
    if data_pull is None:
        return False

    if data_pull.pull_metadata.get("remote_version") != remote_version:
        return False

    if not File.version_has_any_pushes(
        config_file=config_file,
        file_path=local_file_path,
        file_md5=data_pull.file_md5,
    ):
        return False

    return not File.version_has_pending_pushes(
        config_file=config_file,
        file_path=local_file_path,
        file_md5=data_pull.file_md5,
        project_id=project_id,
        site_id=site_id,
    )


def should_download_file(
    local_file_path: Path,
    remote_version: Optional[str],
    config_file: Optional[Path] = None,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    force_redownload: bool = False,
) -> bool:
    """
    Determine whether a Terra file or table extract should be downloaded/written.

    Checks:
    1. If ``force_redownload`` is True, always returns True.
    2. If the local file and sidecar version file exist and match ``remote_version``,
       returns False.
    3. If the local file is absent, checks the database via ``is_pulled_and_pushed``
       to avoid re-downloading files removed by post-push local cleanup.
    """
    if force_redownload:
        logger.info(f"Force redownload enabled for {local_file_path}.")
        return True

    sidecar_path = get_version_sidecar_path(local_file_path)

    if local_file_path.is_file() and sidecar_path.is_file():
        local_version = sidecar_path.read_text(encoding="utf-8").strip()
        if remote_version and local_version == remote_version:
            logger.info(
                f"Remote version matches local copy for {local_file_path}. Skipping."
            )
            return False
        logger.info(
            f"Remote version changed or unavailable for {local_file_path}. Updating."
        )
        return True

    if local_file_path.is_file():
        logger.info(
            f"Local file exists without version sidecar for {local_file_path}. Updating."
        )
        return True

    if (
        config_file is not None
        and project_id is not None
        and site_id is not None
        and is_pulled_and_pushed(
            local_file_path=local_file_path,
            remote_version=remote_version,
            config_file=config_file,
            project_id=project_id,
            site_id=site_id,
        )
    ):
        logger.info(
            f"{local_file_path} was already pulled, pushed, and cleaned up locally. "
            "Skipping download."
        )
        return False

    logger.info(f"Local file does not exist. Downloading {local_file_path}.")
    return True


def record_pulled_file(
    local_file_path: Path,
    remote_version: Optional[str],
    subject_id: str,
    site_id: str,
    project_id: str,
    data_source_name: str,
    config_file: Path,
    start_time: datetime,
    pull_metadata_extra: Optional[Dict[str, Any]] = None,
) -> DataPull:
    """
    Record a newly pulled Terra file in the local version sidecar and Lochness DB
    (``files`` and ``data_pull`` tables), and emit a structured audit log event.
    """
    if remote_version:
        sidecar_path = get_version_sidecar_path(local_file_path)
        with fs.atomic_write(sidecar_path, mode="w", encoding="utf-8") as handle:
            handle.write(remote_version)

    file_model = File(file_path=local_file_path, with_hash=True)
    file_md5: str = file_model.md5  # type: ignore[assignment]

    end_time = datetime.now()
    pull_time_s = int((end_time - start_time).total_seconds())

    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore[index]
    try:
        relative_path = str(local_file_path.relative_to(lochness_root))
    except ValueError:
        relative_path = str(local_file_path)

    pull_metadata: Dict[str, Any] = {
        "remote_version": remote_version,
        "relative_path": relative_path,
    }
    if pull_metadata_extra:
        pull_metadata.update(pull_metadata_extra)

    data_pull = DataPull(
        subject_id=subject_id,
        data_source_name=data_source_name,
        site_id=site_id,
        project_id=project_id,
        file_path=str(local_file_path),
        file_md5=file_md5,
        pull_time_s=pull_time_s,
        pull_metadata=pull_metadata,
    )

    queries = file_model.to_sql_queries_with_availability_update() + [
        data_pull.to_sql_query()
    ]
    db.execute_queries(config_file, queries, show_commands=False)

    msg = f"Successfully saved Terra data for {subject_id} to {local_file_path}."
    logger.info(msg)
    log_event(
        config_file=config_file,
        log_level="INFO",
        event="terra_data_pull_save_success",
        message=msg,
        project_id=project_id,
        site_id=site_id,
        data_source_name=data_source_name,
        subject_id=subject_id,
        extra={
            "file_path": str(local_file_path),
            "file_md5": file_md5,
            "remote_version": remote_version,
            **(pull_metadata_extra or {}),
        },
    )

    return data_pull
