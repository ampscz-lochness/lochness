#!/usr/bin/env python
"""
Pulls data from Terra (app.terra.bio) for active data sources and subjects.

Supports pulling:
1. Workspace entity data tables (JSON / TSV / CSV) per subject
2. GCS files referenced by ``gs://`` URIs in matched entity table rows
3. Files stored directly under subject folders in the workspace GCS bucket
4. Full workspace GCS bucket mirrors (``pull_workspace_bucket=True``)
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name in ("lochness_v2", "lochness") and (parent / "lochness").is_dir():
        root_dir = parent
        break
if root_dir is None:
    root_dir = file.parents[4]

if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

import argparse
import json
import logging
import os
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from rich.logging import RichHandler

from lochness.helpers import config, db, fs, logs, utils
from lochness.models.keystore import KeyStore
from lochness.models.subjects import Subject
from lochness.sources.terra import api as terra_api
from lochness.sources.terra import utils as terra_utils
from lochness.sources.terra.models.data_source import TerraDataSource

MODULE_NAME = "lochness.sources.terra.tasks.pull_data"
NOISY_MODULES = [
    "google.auth.transport.requests",
    "urllib3.connectionpool",
    "urllib3.util.retry",
]

TABLE_GCS_DB_BATCH_SIZE = 10
BUCKET_DB_BATCH_SIZE = 100
BUCKET_MAX_IN_FLIGHT = 1000

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


@dataclass
class _TableGcsTask:
    """Single table-referenced GCS object download candidate."""

    subject: Subject
    terra_subject_id: str
    entity_type: str
    attr_name: str
    gcs_uri: str
    bucket_name: str
    blob_name: str
    local_file_path: Path


def _sync_b2c_token_file_if_configured(
    cred_dict: Dict[str, Any],
    config_file: Optional[Path] = None,
) -> None:
    """Optionally mirror rotated B2C credentials to disk token JSON file."""
    token_file_str = os.environ.get("TERRA_B2C_TOKEN_FILE")
    if not token_file_str and config_file is not None:
        try:
            terra_cfg = config.parse(config_file, "terra")
            token_file_str = terra_cfg.get("b2c_token_file")  # type: ignore[assignment]
        except Exception:  # noqa: BLE001
            token_file_str = None
    if not token_file_str:
        return
    token_path = Path(token_file_str).expanduser()
    if not token_path.parent.is_dir():
        return
    try:
        with fs.atomic_write(token_path, mode="w", encoding="utf-8") as handle:
            json.dump(cred_dict, handle, indent=2)
        os.chmod(token_path, 0o600)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"Failed to update b2c_token_file ({token_path}): {e}")


def _load_terra_credentials(
    terra_data_source: TerraDataSource,
    config_file: Path,
) -> Any:
    """Retrieve KeyStore entry (if configured) and build Terra/Google credentials."""
    metadata = terra_data_source.data_source_metadata
    project_id = terra_data_source.project_id
    identifier = (
        f"{project_id}::{terra_data_source.site_id}::{terra_data_source.data_source_name}"
    )

    cred_dict: Optional[Dict[str, Any]] = None
    keystore: Optional[KeyStore] = None
    if metadata.keystore_name:
        keystore = KeyStore.retrieve_keystore(
            metadata.keystore_name,
            project_id,
            config_file=config_file,
        )
        if not keystore:
            raise RuntimeError(
                f"Keystore '{metadata.keystore_name}' not found for {identifier}."
            )
        try:
            cred_dict = json.loads(keystore.key_value)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Keystore '{metadata.keystore_name}' for {identifier} "
                f"does not contain valid JSON: {e}"
            ) from e

    credentials = terra_api.get_credentials(cred_dict=cred_dict)

    if (
        isinstance(credentials, terra_api.TerraB2CCredentials)
        and keystore is not None
    ):
        def _persist_rotated_refresh_token(new_refresh_token: str) -> None:
            updated_dict = credentials.to_cred_dict()
            updated_dict["refresh_token"] = new_refresh_token
            passphrase = db.sanitize_string(
                config.get_encryption_passphrase(config_file)
            )
            sanitized_key_name = db.sanitize_string(keystore.key_name)
            sanitized_value = db.sanitize_string(json.dumps(updated_dict))
            key_type = db.sanitize_string(keystore.key_type or "terra")
            update_all_sql = f"""
                UPDATE key_store
                SET key_value = pgp_sym_encrypt('{sanitized_value}', '{passphrase}')
                WHERE key_name = '{sanitized_key_name}'
                  AND key_type = '{key_type}';
            """
            db.execute_queries(
                config_file=config_file,
                queries=[update_all_sql],
                show_commands=False,
            )
            _sync_b2c_token_file_if_configured(updated_dict, config_file=config_file)
            logger.debug(
                f"Persisted rotated Terra B2C refresh token to KeyStore "
                f"'{keystore.key_name}' across all '{key_type}' projects."
            )

        credentials.on_refresh_token_rotated = _persist_rotated_refresh_token

    return credentials


def _save_subject_entity_table(
    terra_data_source: TerraDataSource,
    subject: Subject,
    terra_subject_id: str,
    entity_type: str,
    matched_entities: List[Dict[str, Any]],
    output_dir: Path,
    lochness_root: str,
    config_file: Path,
    dry_run: bool,
    force_redownload: bool,
    stats: Dict[str, int],
) -> None:
    """Serialize and save matched entity rows for a single subject & table."""
    metadata = terra_data_source.data_source_metadata
    table_bytes = terra_utils.serialize_entities(
        matched_entities, table_format=metadata.table_format
    )
    table_version = terra_utils.compute_content_version(table_bytes)
    table_file_path = (
        output_dir
        / "tables"
        / f"{entity_type}_{subject.subject_id}.{metadata.table_format}"
    )

    if terra_utils.should_download_file(
        local_file_path=table_file_path,
        remote_version=table_version,
        config_file=config_file,
        project_id=terra_data_source.project_id,
        site_id=terra_data_source.site_id,
        force_redownload=force_redownload,
    ):
        if dry_run:
            logger.info(
                f"[DRY RUN] Would save Terra table '{entity_type}' "
                f"({len(matched_entities)} rows) to {table_file_path}."
            )
        else:
            start_time = datetime.now()
            with fs.atomic_write(table_file_path, mode="wb") as handle:
                handle.write(table_bytes)

            terra_utils.record_pulled_file(
                local_file_path=table_file_path,
                remote_version=table_version,
                subject_id=subject.subject_id,
                site_id=terra_data_source.site_id,
                project_id=terra_data_source.project_id,
                data_source_name=terra_data_source.data_source_name,
                config_file=config_file,
                start_time=start_time,
                lochness_root=lochness_root,
                pull_metadata_extra={
                    "workspace_namespace": metadata.workspace_namespace,
                    "workspace_name": metadata.workspace_name,
                    "entity_type": entity_type,
                    "terra_subject_id": terra_subject_id,
                    "row_count": len(matched_entities),
                },
            )
        stats["tables_saved"] += 1
    else:
        stats["files_skipped"] += 1


def _collect_subject_table_gcs_tasks(
    terra_data_source: TerraDataSource,
    subject: Subject,
    terra_subject_id: str,
    entity_type: str,
    matched_entities: List[Dict[str, Any]],
    output_dir: Path,
) -> List[_TableGcsTask]:
    """Collect deduplicated GCS download tasks referenced by a subject's rows."""
    metadata = terra_data_source.data_source_metadata
    seen_uris = set()
    tasks: List[_TableGcsTask] = []

    for entity in matched_entities:
        uri_pairs = terra_utils.extract_gcs_uris_from_entity(
            entity, gcs_uri_columns=metadata.gcs_uri_columns
        )
        for attr_name, gcs_uri in uri_pairs:
            if gcs_uri in seen_uris:
                continue
            seen_uris.add(gcs_uri)

            bucket_name, blob_name = terra_api.parse_gcs_uri(gcs_uri)
            if not terra_utils.matches_file_patterns(
                blob_name,
                include_patterns=metadata.include_patterns,
                exclude_patterns=metadata.exclude_patterns,
            ):
                logger.debug(
                    f"Skipping {gcs_uri} (does not match include/exclude patterns)."
                )
                continue

            rel_path = terra_utils.safe_relative_blob_path(blob_name)
            local_file_path = (
                output_dir / "table_files" / entity_type / bucket_name / rel_path
            )
            tasks.append(
                _TableGcsTask(
                    subject=subject,
                    terra_subject_id=terra_subject_id,
                    entity_type=entity_type,
                    attr_name=attr_name,
                    gcs_uri=gcs_uri,
                    bucket_name=bucket_name,
                    blob_name=blob_name,
                    local_file_path=local_file_path,
                )
            )
    return tasks


def _execute_single_table_gcs_task(
    task: _TableGcsTask,
    terra_data_source: TerraDataSource,
    storage_client: Any,
    user_project: Optional[str],
    lochness_root: str,
    config_file: Path,
    dry_run: bool,
    force_redownload: bool,
) -> Tuple[str, Optional[terra_utils.PreparedPulledFile]]:
    """
    Execute metadata check and download for one table-referenced GCS URI.

    Returns ``("skipped" | "dry_run" | "downloaded", prepared_record_or_None)``.
    """
    metadata = terra_data_source.data_source_metadata
    blob = terra_api.get_gcs_blob_metadata(
        storage_client=storage_client,
        bucket_name=task.bucket_name,
        blob_name=task.blob_name,
        user_project=user_project,
    )
    remote_version = terra_utils.get_remote_blob_version(blob)

    if not terra_utils.should_download_file(
        local_file_path=task.local_file_path,
        remote_version=remote_version,
        config_file=config_file,
        project_id=terra_data_source.project_id,
        site_id=terra_data_source.site_id,
        force_redownload=force_redownload,
    ):
        return ("skipped", None)

    if dry_run:
        logger.info(
            f"[DRY RUN] Would download {task.gcs_uri} to {task.local_file_path}."
        )
        return ("dry_run", None)

    start_time = datetime.now()
    terra_api.download_gcs_blob(blob=blob, local_path=task.local_file_path)
    prepared = terra_utils.prepare_pulled_file_record(
        local_file_path=task.local_file_path,
        remote_version=remote_version,
        subject_id=task.subject.subject_id,
        site_id=terra_data_source.site_id,
        project_id=terra_data_source.project_id,
        data_source_name=terra_data_source.data_source_name,
        lochness_root=lochness_root,
        start_time=start_time,
        pull_metadata_extra={
            "workspace_namespace": metadata.workspace_namespace,
            "workspace_name": metadata.workspace_name,
            "entity_type": task.entity_type,
            "attribute_name": task.attr_name,
            "terra_subject_id": task.terra_subject_id,
            "gcs_uri": task.gcs_uri,
        },
    )
    return ("downloaded", prepared)


def _run_table_gcs_tasks(
    tasks: List[_TableGcsTask],
    terra_data_source: TerraDataSource,
    storage_client: Any,
    user_project: Optional[str],
    lochness_root: str,
    config_file: Path,
    dry_run: bool,
    force_redownload: bool,
    stats: Dict[str, int],
) -> None:
    """Run table-referenced GCS tasks across a parallel worker pool with batched DB commits."""
    if not tasks:
        return

    max_workers = max(1, min(terra_data_source.data_source_metadata.max_workers, len(tasks)))
    pending_records: List[terra_utils.PreparedPulledFile] = []
    total_tasks = len(tasks)
    completed_tasks = 0

    def _handle_outcome(
        status: str,
        prepared: Optional[terra_utils.PreparedPulledFile],
    ) -> None:
        nonlocal completed_tasks
        completed_tasks += 1
        if status == "skipped":
            stats["files_skipped"] += 1
        elif status == "dry_run":
            stats["files_downloaded"] += 1
        elif status == "downloaded" and prepared is not None:
            pending_records.append(prepared)
            stats["files_downloaded"] += 1
            if len(pending_records) >= TABLE_GCS_DB_BATCH_SIZE:
                terra_utils.commit_pulled_file_records(pending_records, config_file)
                pending_records.clear()
        if completed_tasks % 25 == 0 or completed_tasks == total_tasks:
            logger.info(
                f"Progress for {terra_data_source.data_source_name}: "
                f"{completed_tasks}/{total_tasks} table GCS files processed "
                f"(downloaded={stats['files_downloaded']}, skipped={stats['files_skipped']}, "
                f"errors={stats['errors']})."
            )

    def _log_task_error(task: _TableGcsTask, exc: Exception) -> None:
        stats["errors"] += 1
        logger.error(
            f"Error pulling Terra file {task.gcs_uri} for subject "
            f"{task.subject.subject_id} from {terra_data_source.data_source_name}: {exc}"
        )
        terra_utils.log_event(
            config_file=config_file,
            log_level="ERROR",
            event="terra_data_pull_subject_error",
            message=(
                f"Error pulling Terra file {task.gcs_uri} for subject "
                f"{task.subject.subject_id} from {terra_data_source.data_source_name}."
            ),
            project_id=terra_data_source.project_id,
            site_id=terra_data_source.site_id,
            data_source_name=terra_data_source.data_source_name,
            subject_id=task.subject.subject_id,
            extra={"gcs_uri": task.gcs_uri, "error_message": str(exc)},
        )

    if max_workers == 1:
        for task in tasks:
            try:
                status, prepared = _execute_single_table_gcs_task(
                    task=task,
                    terra_data_source=terra_data_source,
                    storage_client=storage_client,
                    user_project=user_project,
                    lochness_root=lochness_root,
                    config_file=config_file,
                    dry_run=dry_run,
                    force_redownload=force_redownload,
                )
                _handle_outcome(status, prepared)
            except Exception as exc:  # noqa: BLE001
                _log_task_error(task, exc)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(
                    _execute_single_table_gcs_task,
                    task,
                    terra_data_source,
                    storage_client,
                    user_project,
                    lochness_root,
                    config_file,
                    dry_run,
                    force_redownload,
                ): task
                for task in tasks
            }
            pending_futures = set(future_to_task.keys())
            while pending_futures:
                done, pending_futures = wait(
                    pending_futures, return_when=FIRST_COMPLETED
                )
                for fut in done:
                    task = future_to_task[fut]
                    try:
                        status, prepared = fut.result()
                        _handle_outcome(status, prepared)
                    except Exception as exc:  # noqa: BLE001
                        _log_task_error(task, exc)

    if pending_records:
        terra_utils.commit_pulled_file_records(pending_records, config_file)
        pending_records.clear()


def _pull_subject_bucket_files(
    terra_data_source: TerraDataSource,
    subject: Subject,
    terra_subject_id: str,
    bucket_name: str,
    output_dir: Path,
    storage_client: Any,
    user_project: Optional[str],
    lochness_root: str,
    config_file: Path,
    dry_run: bool,
    force_redownload: bool,
    stats: Dict[str, int],
) -> None:
    """Sync files directly from a subject subfolder in the workspace GCS bucket."""
    metadata = terra_data_source.data_source_metadata
    subject_folder = metadata.bucket_subject_folder_template.format(
        subject_id=terra_subject_id
    ).strip("/")

    base_prefix = metadata.bucket_prefix.strip("/")
    subject_prefix = (
        f"{base_prefix}/{subject_folder}/" if base_prefix else f"{subject_folder}/"
    )

    blobs = terra_api.list_gcs_blobs(
        storage_client=storage_client,
        bucket_name=bucket_name,
        prefix=subject_prefix,
        user_project=user_project,
    )

    pending_records: List[terra_utils.PreparedPulledFile] = []
    for blob in blobs:
        if not terra_utils.matches_file_patterns(
            blob.name,
            include_patterns=metadata.include_patterns,
            exclude_patterns=metadata.exclude_patterns,
        ):
            continue

        rel_path = terra_utils.safe_relative_blob_path(
            blob.name, strip_prefix=subject_prefix
        )
        local_file_path = output_dir / "bucket_files" / rel_path
        remote_version = terra_utils.get_remote_blob_version(blob)

        if not terra_utils.should_download_file(
            local_file_path=local_file_path,
            remote_version=remote_version,
            config_file=config_file,
            project_id=terra_data_source.project_id,
            site_id=terra_data_source.site_id,
            force_redownload=force_redownload,
        ):
            stats["files_skipped"] += 1
            continue

        gcs_uri = f"gs://{bucket_name}/{blob.name}"
        if dry_run:
            logger.info(f"[DRY RUN] Would download {gcs_uri} to {local_file_path}.")
        else:
            start_time = datetime.now()
            terra_api.download_gcs_blob(blob=blob, local_path=local_file_path)
            pending_records.append(
                terra_utils.prepare_pulled_file_record(
                    local_file_path=local_file_path,
                    remote_version=remote_version,
                    subject_id=subject.subject_id,
                    site_id=terra_data_source.site_id,
                    project_id=terra_data_source.project_id,
                    data_source_name=terra_data_source.data_source_name,
                    lochness_root=lochness_root,
                    start_time=start_time,
                    pull_metadata_extra={
                        "workspace_namespace": metadata.workspace_namespace,
                        "workspace_name": metadata.workspace_name,
                        "terra_subject_id": terra_subject_id,
                        "gcs_uri": gcs_uri,
                    },
                )
            )
            if len(pending_records) >= BUCKET_DB_BATCH_SIZE:
                terra_utils.commit_pulled_file_records(pending_records, config_file)
                pending_records.clear()
        stats["files_downloaded"] += 1

    if pending_records:
        terra_utils.commit_pulled_file_records(pending_records, config_file)
        pending_records.clear()


def _download_single_workspace_bucket_blob(
    blob: Any,
    bucket_name: str,
    local_file_path: Path,
    remote_version: Optional[str],
    terra_data_source: TerraDataSource,
    subject: Subject,
    terra_subject_id: str,
    lochness_root: str,
) -> terra_utils.PreparedPulledFile:
    """Download one workspace bucket blob and prepare its DB record."""
    metadata = terra_data_source.data_source_metadata
    start_time = datetime.now()
    terra_api.download_gcs_blob(blob=blob, local_path=local_file_path)
    return terra_utils.prepare_pulled_file_record(
        local_file_path=local_file_path,
        remote_version=remote_version,
        subject_id=subject.subject_id,
        site_id=terra_data_source.site_id,
        project_id=terra_data_source.project_id,
        data_source_name=terra_data_source.data_source_name,
        lochness_root=lochness_root,
        start_time=start_time,
        pull_metadata_extra={
            "workspace_namespace": metadata.workspace_namespace,
            "workspace_name": metadata.workspace_name,
            "terra_subject_id": terra_subject_id,
            "gcs_uri": f"gs://{bucket_name}/{blob.name}",
        },
    )


def _pull_workspace_bucket_files(
    terra_data_source: TerraDataSource,
    subject: Subject,
    terra_subject_id: str,
    bucket_name: str,
    output_dir: Path,
    storage_client: Any,
    user_project: Optional[str],
    lochness_root: str,
    config_file: Path,
    dry_run: bool,
    force_redownload: bool,
    stats: Dict[str, int],
) -> None:
    """
    Stream and mirror all blobs in the workspace GCS bucket (under optional
    ``bucket_prefix``) into ``{output_dir}/bucket_files/`` using a bounded
    parallel worker pool and batched PostgreSQL writes.
    """
    metadata = terra_data_source.data_source_metadata
    base_prefix = metadata.bucket_prefix.strip("/")
    prefix = f"{base_prefix}/" if base_prefix else ""

    logger.info(
        f"Streaming workspace bucket gs://{bucket_name}/{prefix} for "
        f"{terra_data_source.data_source_name} (subject={subject.subject_id}, "
        f"max_workers={metadata.max_workers})..."
    )

    pending_records: List[terra_utils.PreparedPulledFile] = []
    scanned_count = 0

    def _drain_completed(
        done_futures: set,
        future_to_uri: Dict[Any, str],
    ) -> None:
        for fut in done_futures:
            gcs_uri = future_to_uri.pop(fut)
            try:
                prepared = fut.result()
                pending_records.append(prepared)
                stats["files_downloaded"] += 1
                if len(pending_records) >= BUCKET_DB_BATCH_SIZE:
                    terra_utils.commit_pulled_file_records(pending_records, config_file)
                    pending_records.clear()
            except Exception as exc:  # noqa: BLE001
                stats["errors"] += 1
                logger.error(f"Failed to download {gcs_uri}: {exc}")
                terra_utils.log_event(
                    config_file=config_file,
                    log_level="ERROR",
                    event="terra_data_pull_bucket_blob_error",
                    message=f"Failed to download workspace bucket blob {gcs_uri}.",
                    project_id=terra_data_source.project_id,
                    site_id=terra_data_source.site_id,
                    data_source_name=terra_data_source.data_source_name,
                    subject_id=subject.subject_id,
                    extra={"gcs_uri": gcs_uri, "error_message": str(exc)},
                )

    with ThreadPoolExecutor(max_workers=max(1, metadata.max_workers)) as executor:
        in_flight: Dict[Any, str] = {}

        for blob in terra_api.iter_gcs_blobs(
            storage_client=storage_client,
            bucket_name=bucket_name,
            prefix=prefix,
            user_project=user_project,
        ):
            scanned_count += 1
            if scanned_count % 5000 == 0:
                logger.info(
                    f"Bucket scan progress for gs://{bucket_name}/{prefix}: "
                    f"scanned={scanned_count}, downloaded={stats['files_downloaded']}, "
                    f"skipped={stats['files_skipped']}, in_flight={len(in_flight)}"
                )

            if not terra_utils.matches_file_patterns(
                blob.name,
                include_patterns=metadata.include_patterns,
                exclude_patterns=metadata.exclude_patterns,
            ):
                continue

            try:
                rel_path = terra_utils.safe_relative_blob_path(
                    blob.name, strip_prefix=prefix
                )
            except ValueError:
                continue

            local_file_path = output_dir / "bucket_files" / rel_path
            remote_version = terra_utils.get_remote_blob_version(blob)

            if not terra_utils.should_download_file(
                local_file_path=local_file_path,
                remote_version=remote_version,
                config_file=config_file,
                project_id=terra_data_source.project_id,
                site_id=terra_data_source.site_id,
                force_redownload=force_redownload,
            ):
                stats["files_skipped"] += 1
                continue

            gcs_uri = f"gs://{bucket_name}/{blob.name}"
            if dry_run:
                logger.info(f"[DRY RUN] Would download {gcs_uri} to {local_file_path}.")
                stats["files_downloaded"] += 1
                continue

            fut = executor.submit(
                _download_single_workspace_bucket_blob,
                blob,
                bucket_name,
                local_file_path,
                remote_version,
                terra_data_source,
                subject,
                terra_subject_id,
                lochness_root,
            )
            in_flight[fut] = gcs_uri

            if len(in_flight) >= BUCKET_MAX_IN_FLIGHT:
                done, _ = wait(set(in_flight.keys()), return_when=FIRST_COMPLETED)
                _drain_completed(done, in_flight)

        while in_flight:
            done, _ = wait(set(in_flight.keys()), return_when=FIRST_COMPLETED)
            _drain_completed(done, in_flight)

    if pending_records:
        terra_utils.commit_pulled_file_records(pending_records, config_file)
        pending_records.clear()

    logger.info(
        f"Finished workspace bucket sync for gs://{bucket_name}/{prefix}: "
        f"scanned={scanned_count}, downloaded={stats['files_downloaded']}, "
        f"skipped={stats['files_skipped']}, errors={stats['errors']}."
    )


def fetch_workspace_data_for_subjects(
    terra_data_source: TerraDataSource,
    subjects: List[Subject],
    config_file: Path,
    dry_run: bool = False,
    force_redownload: bool = False,
    shared_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """
    Pull Terra workspace tables and/or GCS files for a list of subjects.

    Authenticates and fetches workspace entity tables once per workspace to
    avoid redundant Terra API requests across subjects and sites.
    """
    stats: Dict[str, int] = {
        "tables_saved": 0,
        "files_downloaded": 0,
        "files_skipped": 0,
        "errors": 0,
    }

    metadata = terra_data_source.data_source_metadata
    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore[index]

    cred_cache = (
        shared_context.setdefault("credentials", {})
        if shared_context is not None
        else {}
    )
    cred_key = metadata.keystore_name or "__default__"
    if cred_key in cred_cache:
        credentials = cred_cache[cred_key]
    else:
        credentials = _load_terra_credentials(terra_data_source, config_file)
        if shared_context is not None:
            cred_cache[cred_key] = credentials

    headers = terra_api.get_auth_headers(credentials)

    needs_gcs = (
        metadata.pull_table_gcs_files
        or metadata.pull_bucket_files
        or metadata.pull_workspace_bucket
    )
    needs_workspace_metadata = (
        metadata.pull_bucket_files or metadata.pull_workspace_bucket
    ) and not metadata.bucket_name

    ws_key = (metadata.api_url, metadata.workspace_namespace, metadata.workspace_name)
    ws_info_cache = (
        shared_context.setdefault("workspace_info", {})
        if shared_context is not None
        else {}
    )

    workspace_info: Optional[Dict[str, Any]] = None
    if needs_workspace_metadata:
        if ws_key in ws_info_cache:
            workspace_info = ws_info_cache[ws_key]
        else:
            workspace_info = terra_api.get_workspace(
                workspace_namespace=metadata.workspace_namespace,
                workspace_name=metadata.workspace_name,
                headers=headers,
                api_url=metadata.api_url,
            )
            if shared_context is not None:
                ws_info_cache[ws_key] = workspace_info

    # Only pass user_project when explicitly configured for Requester Pays buckets;
    # passing a workspace's googleProject on a READER workspace causes GCS 403 errors.
    user_project = metadata.billing_project

    storage_client: Any = None
    if needs_gcs:
        storage_cache = (
            shared_context.setdefault("storage_clients", {})
            if shared_context is not None
            else {}
        )
        storage_key = (cred_key, user_project)
        if storage_key in storage_cache:
            storage_client = storage_cache[storage_key]
        else:
            gcs_credentials = terra_api.get_gcs_credentials(credentials)
            storage_client = terra_api.create_storage_client(
                credentials=gcs_credentials,
                project=user_project,
            )
            if shared_context is not None:
                storage_cache[storage_key] = storage_client

    bucket_name: Optional[str] = metadata.bucket_name
    if (metadata.pull_bucket_files or metadata.pull_workspace_bucket) and not bucket_name:
        bucket_name = terra_api.get_workspace_bucket_name(workspace_info or {})

    # Fetch raw entity tables once per workspace and index by Terra subject ID
    grouped_tables: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if metadata.pull_tables or metadata.pull_table_gcs_files:
        tables_cache = (
            shared_context.setdefault("raw_tables", {})
            if shared_context is not None
            else {}
        )
        etypes_key = tuple(metadata.entity_types) if metadata.entity_types is not None else None
        raw_cache_key = (*ws_key, etypes_key)

        if raw_cache_key in tables_cache:
            raw_tables = tables_cache[raw_cache_key]
        else:
            if metadata.entity_types is not None:
                target_entity_types = list(metadata.entity_types)
            else:
                entity_types_map = terra_api.get_entity_types(
                    workspace_namespace=metadata.workspace_namespace,
                    workspace_name=metadata.workspace_name,
                    headers=headers,
                    api_url=metadata.api_url,
                )
                target_entity_types = sorted(entity_types_map.keys())

            raw_tables = {}
            for entity_type in target_entity_types:
                raw_tables[entity_type] = terra_api.get_entities(
                    workspace_namespace=metadata.workspace_namespace,
                    workspace_name=metadata.workspace_name,
                    entity_type=entity_type,
                    headers=headers,
                    api_url=metadata.api_url,
                )
            if shared_context is not None:
                tables_cache[raw_cache_key] = raw_tables

        cross_ref_lookup = terra_utils.build_entity_cross_ref_lookup(
            tables=raw_tables,
            subject_id_column=metadata.subject_id_column,
            subject_id_regex=metadata.subject_id_regex,
        )

        for entity_type, entities in raw_tables.items():
            grouped_tables[entity_type] = terra_utils.group_entities_by_subject(
                entities=entities,
                subject_id_column=metadata.subject_id_column,
                subject_id_regex=metadata.subject_id_regex,
                cross_ref_lookup=cross_ref_lookup,
                fallback_subject_id=metadata.fallback_subject_id,
            )

    # Phase 1: Save per-subject entity tables and collect table-referenced GCS tasks
    all_table_gcs_tasks: List[_TableGcsTask] = []
    resolved_subjects: List[Tuple[Subject, str, Path]] = []

    for subject in subjects:
        terra_subject_id = terra_utils.resolve_terra_subject_id(
            subject=subject,
            metadata_key=metadata.subject_id_metadata_key,
        )
        if not terra_subject_id:
            logger.debug(
                f"Skipping subject {subject.subject_id}: metadata key "
                f"'{metadata.subject_id_metadata_key}' not found."
            )
            continue

        output_dir = terra_utils.build_subject_output_dir(
            lochness_root=lochness_root,
            project_id=terra_data_source.project_id,
            site_id=terra_data_source.site_id,
            subject_id=subject.subject_id,
            modality=metadata.modality,
        )
        resolved_subjects.append((subject, terra_subject_id, output_dir))

        try:
            for entity_type, by_subject in grouped_tables.items():
                matched_entities = by_subject.get(terra_subject_id, [])
                if not matched_entities:
                    continue
                if metadata.pull_tables:
                    _save_subject_entity_table(
                        terra_data_source=terra_data_source,
                        subject=subject,
                        terra_subject_id=terra_subject_id,
                        entity_type=entity_type,
                        matched_entities=matched_entities,
                        output_dir=output_dir,
                        lochness_root=lochness_root,
                        config_file=config_file,
                        dry_run=dry_run,
                        force_redownload=force_redownload,
                        stats=stats,
                    )
                if metadata.pull_table_gcs_files:
                    all_table_gcs_tasks.extend(
                        _collect_subject_table_gcs_tasks(
                            terra_data_source=terra_data_source,
                            subject=subject,
                            terra_subject_id=terra_subject_id,
                            entity_type=entity_type,
                            matched_entities=matched_entities,
                            output_dir=output_dir,
                        )
                    )
        except Exception as e:  # noqa: BLE001  # pylint: disable=broad-except
            stats["errors"] += 1
            logger.error(
                f"Error saving Terra table for subject {subject.subject_id} "
                f"from {terra_data_source.data_source_name}: {e}"
            )
            terra_utils.log_event(
                config_file=config_file,
                log_level="ERROR",
                event="terra_data_pull_subject_error",
                message=(
                    f"Error saving Terra table for subject {subject.subject_id} "
                    f"from {terra_data_source.data_source_name}."
                ),
                project_id=terra_data_source.project_id,
                site_id=terra_data_source.site_id,
                data_source_name=terra_data_source.data_source_name,
                subject_id=subject.subject_id,
                extra={"error_message": str(e)},
            )

    # Phase 2: Download table-referenced GCS files in parallel across all subjects
    if metadata.pull_table_gcs_files and all_table_gcs_tasks:
        _run_table_gcs_tasks(
            tasks=all_table_gcs_tasks,
            terra_data_source=terra_data_source,
            storage_client=storage_client,
            user_project=user_project,
            lochness_root=lochness_root,
            config_file=config_file,
            dry_run=dry_run,
            force_redownload=force_redownload,
            stats=stats,
        )

    # Phase 3: Subject-scoped bucket folders and/or full workspace bucket sync
    for subject, terra_subject_id, output_dir in resolved_subjects:
        try:
            if metadata.pull_bucket_files and bucket_name:
                _pull_subject_bucket_files(
                    terra_data_source=terra_data_source,
                    subject=subject,
                    terra_subject_id=terra_subject_id,
                    bucket_name=bucket_name,
                    output_dir=output_dir,
                    storage_client=storage_client,
                    user_project=user_project,
                    lochness_root=lochness_root,
                    config_file=config_file,
                    dry_run=dry_run,
                    force_redownload=force_redownload,
                    stats=stats,
                )
            if metadata.pull_workspace_bucket and bucket_name:
                _pull_workspace_bucket_files(
                    terra_data_source=terra_data_source,
                    subject=subject,
                    terra_subject_id=terra_subject_id,
                    bucket_name=bucket_name,
                    output_dir=output_dir,
                    storage_client=storage_client,
                    user_project=user_project,
                    lochness_root=lochness_root,
                    config_file=config_file,
                    dry_run=dry_run,
                    force_redownload=force_redownload,
                    stats=stats,
                )
        except Exception as e:  # noqa: BLE001  # pylint: disable=broad-except
            stats["errors"] += 1
            logger.error(
                f"Error pulling Terra bucket data for subject {subject.subject_id} "
                f"from {terra_data_source.data_source_name}: {e}"
            )
            terra_utils.log_event(
                config_file=config_file,
                log_level="ERROR",
                event="terra_data_pull_subject_error",
                message=(
                    f"Error pulling Terra bucket data for subject {subject.subject_id} "
                    f"from {terra_data_source.data_source_name}."
                ),
                project_id=terra_data_source.project_id,
                site_id=terra_data_source.site_id,
                data_source_name=terra_data_source.data_source_name,
                subject_id=subject.subject_id,
                extra={"error_message": str(e)},
            )

    return stats


def pull_all_data(
    config_file: Path,
    project_id: Optional[str] = None,
    site_id: Optional[str] = None,
    subject_id_list: Optional[List[str]] = None,
    source_id: Optional[str] = None,
    dry_run: bool = False,
    force_redownload: bool = False,
    push_to_sink: bool = False,
) -> Dict[str, int]:
    """
    Main function to pull data for all active Terra data sources and subjects.
    """
    total_stats: Dict[str, int] = {
        "tables_saved": 0,
        "files_downloaded": 0,
        "files_skipped": 0,
        "errors": 0,
    }

    terra_utils.log_event(
        config_file=config_file,
        log_level="INFO",
        event="terra_data_pull_start",
        message="Starting Terra data pull process.",
        project_id=project_id,
        site_id=site_id,
        extra={
            "source_id": source_id,
            "dry_run": dry_run,
            "force_redownload": force_redownload,
            "push_to_sink": push_to_sink,
        },
    )

    active_terra_data_sources = TerraDataSource.get_all_terra_data_sources(
        config_file=config_file,
        active_only=True,
    )

    if project_id:
        active_terra_data_sources = [
            ds for ds in active_terra_data_sources if ds.project_id == project_id
        ]
    if site_id:
        active_terra_data_sources = [
            ds for ds in active_terra_data_sources if ds.site_id == site_id
        ]
    if source_id:
        active_terra_data_sources = [
            ds for ds in active_terra_data_sources if ds.data_source_name == source_id
        ]

    if not active_terra_data_sources:
        msg = "No active Terra data sources found for data pull."
        logger.info(msg)
        terra_utils.log_event(
            config_file=config_file,
            log_level="INFO",
            event="terra_data_pull_no_active_sources",
            message=msg,
            project_id=project_id,
            site_id=site_id,
        )
        return total_stats

    msg = f"Found {len(active_terra_data_sources)} active Terra data sources for data pull."
    logger.info(msg)
    terra_utils.log_event(
        config_file=config_file,
        log_level="INFO",
        event="terra_data_pull_active_sources_found",
        message=msg,
        project_id=project_id,
        site_id=site_id,
        extra={"count": len(active_terra_data_sources)},
    )

    shared_context: Dict[str, Any] = {}

    for terra_data_source in active_terra_data_sources:
        subjects_in_db = Subject.get_subjects_for_project_site(
            project_id=terra_data_source.project_id,
            site_id=terra_data_source.site_id,
            config_file=config_file,
        )

        if subject_id_list:
            subjects_in_db = [
                subj for subj in subjects_in_db if subj.subject_id in subject_id_list
            ]

        if not subjects_in_db:
            no_subj_msg = (
                f"No subjects found for "
                f"{terra_data_source.project_id}::{terra_data_source.site_id}."
            )
            logger.info(no_subj_msg)
            terra_utils.log_event(
                config_file=config_file,
                log_level="INFO",
                event="terra_data_pull_no_subjects",
                message=no_subj_msg,
                project_id=terra_data_source.project_id,
                site_id=terra_data_source.site_id,
                data_source_name=terra_data_source.data_source_name,
            )
            continue

        subj_msg = (
            f"Found {len(subjects_in_db)} subjects for "
            f"{terra_data_source.data_source_name}."
        )
        logger.info(subj_msg)
        terra_utils.log_event(
            config_file=config_file,
            log_level="INFO",
            event="terra_data_pull_subjects_found",
            message=subj_msg,
            project_id=terra_data_source.project_id,
            site_id=terra_data_source.site_id,
            data_source_name=terra_data_source.data_source_name,
            extra={"count": len(subjects_in_db)},
        )

        try:
            call_kwargs: Dict[str, Any] = {
                "terra_data_source": terra_data_source,
                "subjects": subjects_in_db,
                "config_file": config_file,
                "dry_run": dry_run,
                "force_redownload": force_redownload,
            }
            if len(active_terra_data_sources) > 1:
                call_kwargs["shared_context"] = shared_context

            ds_stats = fetch_workspace_data_for_subjects(**call_kwargs)
            for key, count in ds_stats.items():
                total_stats[key] += count
        except (RuntimeError, TypeError, ValueError) as e:
            total_stats["errors"] += 1
            logger.error(
                f"Error fetching Terra workspace data for "
                f"{terra_data_source.data_source_name}: {e}"
            )
            terra_utils.log_event(
                config_file=config_file,
                log_level="ERROR",
                event="terra_data_pull_workspace_error",
                message=(
                    f"Error fetching Terra workspace data for "
                    f"{terra_data_source.data_source_name}."
                ),
                project_id=terra_data_source.project_id,
                site_id=terra_data_source.site_id,
                data_source_name=terra_data_source.data_source_name,
                extra={"error_message": str(e)},
            )

    if push_to_sink and not dry_run:
        from lochness.tasks.push_data import push_all_data

        push_all_data(
            config_file=config_file,
            project_id=project_id,
            site_id=site_id,
        )

    terra_utils.log_event(
        config_file=config_file,
        log_level="INFO",
        event="terra_data_pull_complete",
        message="Finished Terra data pull process.",
        project_id=project_id,
        site_id=site_id,
        extra=total_stats,
    )

    return total_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull Terra data for all or specific project/site/source."
    )
    parser.add_argument(
        "--project_id",
        "--project-id",
        dest="project_id",
        type=str,
        default=None,
        help="Project ID to pull data for (optional)",
    )
    parser.add_argument(
        "--site_id",
        "--site-id",
        dest="site_id",
        type=str,
        default=None,
        help="Site ID to pull data for (optional)",
    )
    parser.add_argument(
        "--source_id",
        "--source-id",
        dest="source_id",
        type=str,
        default=None,
        help="Terra data source name to pull data for (optional)",
    )
    parser.add_argument(
        "--subject_ids",
        "--subject-ids",
        dest="subject_ids",
        nargs="+",
        default=None,
        help="Specific subject IDs to pull data for (optional)",
    )
    parser.add_argument(
        "--dry_run",
        "--dry-run",
        dest="dry_run",
        action="store_true",
        help="Validate connectivity and log planned downloads without writing files",
    )
    parser.add_argument(
        "--force_redownload",
        "--force-redownload",
        dest="force_redownload",
        action="store_true",
        help="Ignore cached version checks and redownload matching tables/files",
    )
    parser.add_argument(
        "--push_to_sink",
        "--push-to-sink",
        dest="push_to_sink",
        action="store_true",
        help="Push pulled files to configured data sinks after pulling",
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    logger.info(f"Using config file: {config_file}")
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file,
        module_name=MODULE_NAME,
        logger=logger,
        noisy_modules=NOISY_MODULES,
    )

    logger.info("Starting Terra data pull...")
    summary = pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
        subject_id_list=args.subject_ids,
        source_id=args.source_id,
        dry_run=args.dry_run,
        force_redownload=args.force_redownload,
        push_to_sink=args.push_to_sink,
    )
    logger.info(f"Finished Terra data pull: {summary}")
