#!/usr/bin/env python
"""
Pulls data from Terra (app.terra.bio) for active data sources and subjects.

Supports pulling:
1. Workspace entity data tables (JSON / TSV / CSV) per subject
2. GCS files referenced by ``gs://`` URIs in matched entity table rows
3. Files stored directly under subject folders in the workspace GCS bucket
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
from datetime import datetime
from typing import Any, Dict, List, Optional

from rich.logging import RichHandler

from lochness.helpers import config, fs, logs, utils
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

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def _load_terra_credentials(
    terra_data_source: TerraDataSource,
    config_file: Path,
) -> Any:
    """Retrieve KeyStore entry (if configured) and build Google OAuth2 credentials."""
    metadata = terra_data_source.data_source_metadata
    project_id = terra_data_source.project_id
    identifier = (
        f"{project_id}::{terra_data_source.site_id}::{terra_data_source.data_source_name}"
    )

    cred_dict: Optional[Dict[str, Any]] = None
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

    return terra_api.get_credentials(cred_dict=cred_dict)


def _pull_subject_table_and_gcs_files(
    terra_data_source: TerraDataSource,
    subject: Subject,
    terra_subject_id: str,
    entity_type: str,
    matched_entities: List[Dict[str, Any]],
    output_dir: Path,
    storage_client: Any,
    user_project: Optional[str],
    config_file: Path,
    dry_run: bool,
    force_redownload: bool,
    stats: Dict[str, int],
) -> None:
    """Save entity table rows and/or download referenced GCS files for one subject."""
    metadata = terra_data_source.data_source_metadata

    if metadata.pull_tables:
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

    if metadata.pull_table_gcs_files:
        seen_uris = set()
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

                blob = terra_api.get_gcs_blob_metadata(
                    storage_client=storage_client,
                    bucket_name=bucket_name,
                    blob_name=blob_name,
                    user_project=user_project,
                )
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

                if dry_run:
                    logger.info(
                        f"[DRY RUN] Would download {gcs_uri} to {local_file_path}."
                    )
                else:
                    start_time = datetime.now()
                    terra_api.download_gcs_blob(blob=blob, local_path=local_file_path)
                    terra_utils.record_pulled_file(
                        local_file_path=local_file_path,
                        remote_version=remote_version,
                        subject_id=subject.subject_id,
                        site_id=terra_data_source.site_id,
                        project_id=terra_data_source.project_id,
                        data_source_name=terra_data_source.data_source_name,
                        config_file=config_file,
                        start_time=start_time,
                        pull_metadata_extra={
                            "workspace_namespace": metadata.workspace_namespace,
                            "workspace_name": metadata.workspace_name,
                            "entity_type": entity_type,
                            "attribute_name": attr_name,
                            "terra_subject_id": terra_subject_id,
                            "gcs_uri": gcs_uri,
                        },
                    )
                stats["files_downloaded"] += 1


def _pull_subject_bucket_files(
    terra_data_source: TerraDataSource,
    subject: Subject,
    terra_subject_id: str,
    bucket_name: str,
    output_dir: Path,
    storage_client: Any,
    user_project: Optional[str],
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
            terra_utils.record_pulled_file(
                local_file_path=local_file_path,
                remote_version=remote_version,
                subject_id=subject.subject_id,
                site_id=terra_data_source.site_id,
                project_id=terra_data_source.project_id,
                data_source_name=terra_data_source.data_source_name,
                config_file=config_file,
                start_time=start_time,
                pull_metadata_extra={
                    "workspace_namespace": metadata.workspace_namespace,
                    "workspace_name": metadata.workspace_name,
                    "terra_subject_id": terra_subject_id,
                    "gcs_uri": gcs_uri,
                },
            )
        stats["files_downloaded"] += 1


def fetch_workspace_data_for_subjects(
    terra_data_source: TerraDataSource,
    subjects: List[Subject],
    config_file: Path,
    dry_run: bool = False,
    force_redownload: bool = False,
) -> Dict[str, int]:
    """
    Pull Terra workspace tables and/or GCS files for a list of subjects.

    Authenticates and fetches workspace entity tables once per workspace to
    avoid redundant Terra API requests across subjects.

    Args:
        terra_data_source (TerraDataSource): Configured Terra data source.
        subjects (List[Subject]): Subjects to pull data for.
        config_file (Path): Path to the Lochness configuration file.
        dry_run (bool): If True, log actions without writing files or DB records.
        force_redownload (bool): If True, ignore local/DB version cache.

    Returns:
        Dict[str, int]: Summary counters (``tables_saved``, ``files_downloaded``,
        ``files_skipped``, ``errors``).
    """
    stats: Dict[str, int] = {
        "tables_saved": 0,
        "files_downloaded": 0,
        "files_skipped": 0,
        "errors": 0,
    }

    metadata = terra_data_source.data_source_metadata
    lochness_root: str = config.parse(config_file, "general")["lochness_root"]  # type: ignore[index]

    credentials = _load_terra_credentials(terra_data_source, config_file)
    headers = terra_api.get_auth_headers(credentials)

    needs_gcs = metadata.pull_table_gcs_files or metadata.pull_bucket_files
    needs_workspace_metadata = metadata.pull_bucket_files and not metadata.bucket_name
    workspace_info: Optional[Dict[str, Any]] = None
    if needs_workspace_metadata:
        workspace_info = terra_api.get_workspace(
            workspace_namespace=metadata.workspace_namespace,
            workspace_name=metadata.workspace_name,
            headers=headers,
            api_url=metadata.api_url,
        )

    # Only pass user_project when explicitly configured for Requester Pays buckets;
    # passing a workspace's googleProject on a READER workspace causes GCS 403 errors.
    user_project = metadata.billing_project

    storage_client: Any = None
    if needs_gcs:
        gcs_credentials = terra_api.get_gcs_credentials(credentials)
        storage_client = terra_api.create_storage_client(
            credentials=gcs_credentials,
            project=user_project,
        )

    bucket_name: Optional[str] = metadata.bucket_name
    if metadata.pull_bucket_files and not bucket_name:
        bucket_name = terra_api.get_workspace_bucket_name(workspace_info or {})

    # Fetch entity tables once per workspace and index by Terra subject ID
    grouped_tables: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if metadata.pull_tables or metadata.pull_table_gcs_files:
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

        for entity_type in target_entity_types:
            entities = terra_api.get_entities(
                workspace_namespace=metadata.workspace_namespace,
                workspace_name=metadata.workspace_name,
                entity_type=entity_type,
                headers=headers,
                api_url=metadata.api_url,
            )
            grouped_tables[entity_type] = terra_utils.group_entities_by_subject(
                entities=entities,
                subject_id_column=metadata.subject_id_column,
            )

    # Pull per-subject data into PHOENIX layout
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

        try:
            for entity_type, by_subject in grouped_tables.items():
                matched_entities = by_subject.get(terra_subject_id, [])
                if not matched_entities:
                    continue
                _pull_subject_table_and_gcs_files(
                    terra_data_source=terra_data_source,
                    subject=subject,
                    terra_subject_id=terra_subject_id,
                    entity_type=entity_type,
                    matched_entities=matched_entities,
                    output_dir=output_dir,
                    storage_client=storage_client,
                    user_project=user_project,
                    config_file=config_file,
                    dry_run=dry_run,
                    force_redownload=force_redownload,
                    stats=stats,
                )

            if metadata.pull_bucket_files and bucket_name:
                _pull_subject_bucket_files(
                    terra_data_source=terra_data_source,
                    subject=subject,
                    terra_subject_id=terra_subject_id,
                    bucket_name=bucket_name,
                    output_dir=output_dir,
                    storage_client=storage_client,
                    user_project=user_project,
                    config_file=config_file,
                    dry_run=dry_run,
                    force_redownload=force_redownload,
                    stats=stats,
                )
        except Exception as e:  # noqa: BLE001  # pylint: disable=broad-except
            stats["errors"] += 1
            logger.error(
                f"Error pulling Terra data for subject {subject.subject_id} "
                f"from {terra_data_source.data_source_name}: {e}"
            )
            terra_utils.log_event(
                config_file=config_file,
                log_level="ERROR",
                event="terra_data_pull_subject_error",
                message=(
                    f"Error pulling Terra data for subject {subject.subject_id} "
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
            ds_stats = fetch_workspace_data_for_subjects(
                terra_data_source=terra_data_source,
                subjects=subjects_in_db,
                config_file=config_file,
                dry_run=dry_run,
                force_redownload=force_redownload,
            )
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
    pull_all_data(
        config_file=config_file,
        project_id=args.project_id,
        site_id=args.site_id,
        subject_id_list=args.subject_ids,
        source_id=args.source_id,
        dry_run=args.dry_run,
        force_redownload=args.force_redownload,
        push_to_sink=args.push_to_sink,
    )
    logger.info("Finished Terra data pull.")
