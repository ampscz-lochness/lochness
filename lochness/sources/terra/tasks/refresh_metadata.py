#!/usr/bin/env python
"""
Auto-discovers shared AMP-SCZ Terra workspaces, sites, subjects, and cohort
buckets on app.terra.bio and upserts Projects, KeyStore entries, Sites,
Subjects, and TerraDataSource records into PostgreSQL.
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
import fnmatch
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from rich.logging import RichHandler

from lochness.helpers import config, db, fs, logs, utils
from lochness.models.data_source import DataSource
from lochness.models.keystore import KeyStore
from lochness.models.projects import Project
from lochness.models.sites import Site
from lochness.models.subjects import Subject
from lochness.models.supported_data_source_types import (
    populate_supported_data_source_types,
)
from lochness.sources.terra import api as terra_api
from lochness.sources.terra import utils as terra_utils
from lochness.sources.terra.models.data_source import TerraDataSourceMetadata

MODULE_NAME = "lochness.sources.terra.tasks.refresh_metadata"
NOISY_MODULES = [
    "google.auth.transport.requests",
    "urllib3.connectionpool",
    "urllib3.util.retry",
]

AMP_SCZ_SUBJECT_ID_REGEX = r"^(?:[^-]+-)?([A-Za-z]{2}\d{5})$"
COHORT_SITE_ID = "COHORT"
COHORT_SUBJECT_ID = "WORKSPACE_COHORT"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def infer_project_id(workspace_namespace: str, workspace_name: str) -> str:
    """Infer canonical Lochness project_id from Terra workspace namespace/name."""
    combined = f"{workspace_namespace}/{workspace_name}".lower()
    if "prescient" in combined:
        return "PRESCIENT"
    if "pronet" in combined:
        return "ProNET"
    if "procan" in combined:
        return "ProCAN"
    sanitized = re.sub(r"[^A-Za-z0-9]+", "_", workspace_name).strip("_")
    return sanitized or "TerraProject"


def slugify_workspace(workspace_name: str) -> str:
    """Build a concise alphanumeric slug for disambiguating data source names."""
    slug = re.sub(r"[^A-Za-z0-9]+", "_", workspace_name).strip("_")
    return slug[:40] if slug else "workspace"


def detect_subject_id_column(
    tables: Dict[str, List[Dict[str, Any]]],
    subject_id_regex: str = AMP_SCZ_SUBJECT_ID_REGEX,
) -> str:
    """
    Auto-detect the entity attribute column carrying AMP-SCZ participant IDs
    (preferring ``collaborator_participant_id``, then ``participant``, ``subject_id``,
    ``participant_id``, or ``name``).
    """
    candidates = (
        "collaborator_participant_id",
        "participant",
        "subject_id",
        "participant_id",
        "name",
    )
    best_col = "collaborator_participant_id"
    best_matches = -1

    for col in candidates:
        matches = 0
        for entities in tables.values():
            for entity in entities:
                extracted = terra_utils.extract_entity_subject_id(
                    entity,
                    subject_id_column=col,
                    subject_id_regex=subject_id_regex,
                )
                if extracted:
                    matches += 1
        if matches > best_matches:
            best_matches = matches
            best_col = col

    return best_col


def filter_target_workspaces(
    all_workspaces: List[Dict[str, Any]],
    namespace_patterns: Optional[List[str]] = None,
    include_public: bool = False,
) -> List[Tuple[str, str, Dict[str, Any]]]:
    """
    Filter ``GET /api/workspaces`` results down to private/shared target workspaces.
    """
    selected: List[Tuple[str, str, Dict[str, Any]]] = []
    for entry in all_workspaces:
        if not include_public and bool(entry.get("public", False)):
            continue
        ws = entry.get("workspace") or {}
        ns = str(ws.get("namespace", "")).strip()
        name = str(ws.get("name", "")).strip()
        if not ns or not name:
            continue

        if namespace_patterns:
            full = f"{ns}/{name}"
            matched = any(
                fnmatch.fnmatch(ns.lower(), pat.lower())
                or fnmatch.fnmatch(full.lower(), pat.lower())
                for pat in namespace_patterns
            )
            if not matched:
                continue

        selected.append((ns, name, entry))

    selected.sort(key=lambda item: (item[0], item[1]))
    return selected


def load_seed_credentials(
    config_file: Path,
    b2c_token_json: Optional[Path] = None,
) -> terra_api.TerraCredentials:
    """
    Load Terra credentials from ``--b2c-token-json``, ``config.ini`` ``[terra]``,
    or ``TERRA_REFRESH_TOKEN``, attaching atomic file persistence when rotated.
    """
    cred_dict: Optional[Dict[str, Any]] = None
    token_file_to_update: Optional[Path] = None

    if b2c_token_json is not None:
        resolved_json = b2c_token_json.expanduser().resolve()
        if not resolved_json.is_file():
            raise FileNotFoundError(f"Terra B2C token file not found: {resolved_json}")
        cred_dict = json.loads(resolved_json.read_text(encoding="utf-8"))
        token_file_to_update = resolved_json
    else:
        try:
            terra_cfg = config.parse(config_file, "terra")
            rt = terra_cfg.get("refresh_token")
            if rt:
                cred_dict = {
                    "auth_mode": "terra_b2c",
                    "refresh_token": rt,
                    "client_id": terra_cfg.get(
                        "client_id", terra_api.DEFAULT_TERRA_B2C_CLIENT_ID
                    ),
                    "token_url": terra_cfg.get(
                        "token_url", terra_api.DEFAULT_TERRA_B2C_TOKEN_URL
                    ),
                    "sam_api_url": terra_cfg.get(
                        "sam_api_url", terra_api.DEFAULT_TERRA_SAM_URL
                    ),
                }
        except Exception:  # noqa: BLE001
            cred_dict = None

    creds = terra_api.get_credentials(cred_dict=cred_dict)

    if isinstance(creds, terra_api.TerraB2CCredentials) and token_file_to_update:
        target_path = token_file_to_update

        def _persist_to_file(new_rt: str) -> None:
            updated = creds.to_cred_dict()
            updated["refresh_token"] = new_rt
            with fs.atomic_write(target_path, mode="w", encoding="utf-8") as handle:
                json.dump(updated, handle, indent=2)
            os.chmod(target_path, 0o600)
            logger.info(f"Updated rotated Terra B2C refresh token in {target_path}.")

        creds.on_refresh_token_rotated = _persist_to_file

    return creds


def build_workspace_onboarding_queries(
    workspace_namespace: str,
    workspace_name: str,
    workspace_info: Dict[str, Any],
    tables: Dict[str, List[Dict[str, Any]]],
    cred_dict: Optional[Dict[str, Any]],
    encryption_passphrase: str,
    key_name: str = "terra_prod",
    project_id_override: Optional[str] = None,
    modality: str = "terra",
    subject_id_regex: str = AMP_SCZ_SUBJECT_ID_REGEX,
    pull_workspace_bucket: bool = True,
    max_workers: int = 16,
    api_url: str = terra_api.DEFAULT_TERRA_API_URL,
    disambiguate_source_name: bool = False,
) -> Tuple[List[str], Dict[str, Any]]:
    """
    Construct idempotent SQL upsert queries for Project, KeyStore, Sites,
    Subjects, and TerraDataSource records discovered from a Terra workspace.
    """
    project_id = project_id_override or infer_project_id(
        workspace_namespace, workspace_name
    )
    subject_id_col = detect_subject_id_column(tables, subject_id_regex=subject_id_regex)
    ws_meta = workspace_info.get("workspace") or {}
    bucket_name = ws_meta.get("bucketName")

    # Map canonical subject_id -> aggregated Terra provenance metadata
    subjects_by_site: Dict[str, Dict[str, Dict[str, Any]]] = {}
    matched_entity_count = 0
    unmatched_entity_count = 0

    cross_ref = terra_utils.build_entity_cross_ref_lookup(
        tables=tables,
        subject_id_column=subject_id_col,
        subject_id_regex=subject_id_regex,
    )

    for entity_type, entities in tables.items():
        for entity in entities:
            subj_id = terra_utils.extract_entity_subject_id(
                entity,
                subject_id_column=subject_id_col,
                subject_id_regex=subject_id_regex,
            )
            if not subj_id:
                subj_id = terra_utils.resolve_entity_via_cross_ref(entity, cross_ref)

            if subj_id and len(subj_id) >= 2 and subj_id[:2].isalpha():
                matched_entity_count += 1
                site_id = subj_id[:2].upper()
                site_subjects = subjects_by_site.setdefault(site_id, {})
                meta = site_subjects.setdefault(
                    subj_id,
                    {
                        "terra_workspace": f"{workspace_namespace}/{workspace_name}",
                        "sample_ids": [],
                    },
                )
                ename = str(entity.get("name", "")).strip()
                if entity_type == "sample" and ename and ename not in meta["sample_ids"]:
                    meta["sample_ids"].append(ename)
                attrs = entity.get("attributes")
                if isinstance(attrs, dict) and subject_id_col in attrs:
                    raw_collab = attrs.get(subject_id_col)
                    if isinstance(raw_collab, str) and raw_collab:
                        meta["collaborator_participant_id"] = raw_collab
            else:
                unmatched_entity_count += 1

    queries: List[str] = []
    queries.extend(populate_supported_data_source_types())

    project = Project(
        project_id=project_id,
        project_name=project_id,
        project_is_active=True,
        project_metadata={
            "source": "terra",
            "workspace_namespace": workspace_namespace,
            "workspace_name": workspace_name,
        },
    )
    queries.append(project.to_sql_query())

    keystore_to_use: Optional[str] = None
    if cred_dict is not None:
        keystore_to_use = key_name
        ks = KeyStore(
            key_name=key_name,
            key_value=json.dumps(cred_dict),
            key_type="terra",
            project_id=project_id,
            key_metadata={
                "auth_mode": cred_dict.get("auth_mode", "terra_b2c"),
                "updated_by": MODULE_NAME,
            },
        )
        queries.append(ks.to_sql_query(encryption_passphrase))

    ws_suffix = f"_{slugify_workspace(workspace_name)}" if disambiguate_source_name else ""

    for site_id in sorted(subjects_by_site.keys()):
        site = Site(
            site_id=site_id,
            site_name=f"{project_id} {site_id}",
            project_id=project_id,
            site_is_active=True,
            site_metadata={"discovered_from_terra": f"{workspace_namespace}/{workspace_name}"},
        )
        queries.append(site.to_sql_query())

        for subj_id in sorted(subjects_by_site[site_id].keys()):
            subj = Subject(
                subject_id=subj_id,
                site_id=site_id,
                project_id=project_id,
                subject_metadata={"terra": subjects_by_site[site_id][subj_id]},
            )
            queries.append(subj.to_sql_query())

        ds_meta = TerraDataSourceMetadata(
            workspace_namespace=workspace_namespace,
            workspace_name=workspace_name,
            keystore_name=keystore_to_use,
            modality=modality,
            api_url=api_url,
            pull_tables=True,
            pull_table_gcs_files=True,
            pull_bucket_files=False,
            pull_workspace_bucket=False,
            subject_id_column=subject_id_col,
            subject_id_regex=subject_id_regex,
            max_workers=max_workers,
        )
        ds = DataSource(
            data_source_name=f"{project_id}_{site_id}{ws_suffix}_terra",
            is_active=True,
            site_id=site_id,
            project_id=project_id,
            data_source_type="terra",
            data_source_metadata=ds_meta.model_dump(),
        )
        queries.append(ds.to_sql_query())

    # Always create COHORT site + WORKSPACE_COHORT subject to own cohort-level
    # rows (e.g. sample_set, QC controls) and workspace gs://fc-... bucket blobs.
    cohort_site = Site(
        site_id=COHORT_SITE_ID,
        site_name=f"{project_id} Workspace Cohort",
        project_id=project_id,
        site_is_active=True,
        site_metadata={
            "discovered_from_terra": f"{workspace_namespace}/{workspace_name}",
            "bucket_name": bucket_name,
        },
    )
    queries.append(cohort_site.to_sql_query())

    cohort_subject = Subject(
        subject_id=COHORT_SUBJECT_ID,
        site_id=COHORT_SITE_ID,
        project_id=project_id,
        subject_metadata={
            "terra": {
                "terra_workspace": f"{workspace_namespace}/{workspace_name}",
                "bucket_name": bucket_name,
                "role": "cohort_and_workspace_bucket",
            }
        },
    )
    queries.append(cohort_subject.to_sql_query())

    cohort_ds_meta = TerraDataSourceMetadata(
        workspace_namespace=workspace_namespace,
        workspace_name=workspace_name,
        keystore_name=keystore_to_use,
        modality=modality,
        api_url=api_url,
        pull_tables=True,
        pull_table_gcs_files=True,
        pull_bucket_files=False,
        pull_workspace_bucket=pull_workspace_bucket,
        subject_id_column=subject_id_col,
        subject_id_regex=subject_id_regex,
        fallback_subject_id=COHORT_SUBJECT_ID,
        bucket_name=bucket_name,
        max_workers=max_workers,
    )
    cohort_ds = DataSource(
        data_source_name=f"{project_id}_{COHORT_SITE_ID}{ws_suffix}_terra",
        is_active=True,
        site_id=COHORT_SITE_ID,
        project_id=project_id,
        data_source_type="terra",
        data_source_metadata=cohort_ds_meta.model_dump(),
    )
    queries.append(cohort_ds.to_sql_query())

    total_subjects = sum(len(smap) for smap in subjects_by_site.values())
    summary = {
        "workspace": f"{workspace_namespace}/{workspace_name}",
        "project_id": project_id,
        "bucket_name": bucket_name,
        "entity_types": {k: len(v) for k, v in tables.items()},
        "subject_id_column": subject_id_col,
        "sites_discovered": sorted(subjects_by_site.keys()),
        "site_count": len(subjects_by_site),
        "subject_count": total_subjects,
        "matched_entity_rows": matched_entity_count,
        "cohort_unmatched_entity_rows": unmatched_entity_count,
        "pull_workspace_bucket": pull_workspace_bucket,
    }
    return queries, summary


def refresh_terra_metadata(
    config_file: Path,
    b2c_token_json: Optional[Path] = None,
    key_name: str = "terra_prod",
    namespace_patterns: Optional[List[str]] = None,
    modality: str = "terra",
    pull_workspace_bucket: bool = True,
    max_workers: int = 16,
    api_url: str = terra_api.DEFAULT_TERRA_API_URL,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """
    Discover accessible non-public Terra workspaces matching ``namespace_patterns``
    and populate Lochness PostgreSQL metadata tables.
    """
    creds = load_seed_credentials(config_file=config_file, b2c_token_json=b2c_token_json)
    headers = terra_api.get_auth_headers(creds)

    cred_dict: Optional[Dict[str, Any]] = (
        creds.to_cred_dict()
        if isinstance(creds, terra_api.TerraB2CCredentials)
        else None
    )

    all_workspaces = terra_api.list_workspaces(headers=headers, api_url=api_url)
    non_public = filter_target_workspaces(all_workspaces, namespace_patterns=None)
    logger.info(
        f"Total non-public Terra workspaces shared with account: {len(non_public)} -> "
        f"{[f'{ns}/{nm}' for ns, nm, _ in non_public]}"
    )

    targets = filter_target_workspaces(
        all_workspaces,
        namespace_patterns=namespace_patterns,
    )
    if not targets:
        logger.warning("No matching Terra workspaces found for metadata refresh.")
        return []

    # Check if multiple workspaces map to the same inferred project_id
    project_counts: Dict[str, int] = {}
    for ns, nm, _ in targets:
        pid = infer_project_id(ns, nm)
        project_counts[pid] = project_counts.get(pid, 0) + 1

    passphrase = config.get_encryption_passphrase(config_file)
    summaries: List[Dict[str, Any]] = []

    for ns, nm, ws_entry in targets:
        pid = infer_project_id(ns, nm)
        entity_types_map = terra_api.get_entity_types(
            workspace_namespace=ns,
            workspace_name=nm,
            headers=headers,
            api_url=api_url,
        )
        tables: Dict[str, List[Dict[str, Any]]] = {}
        for etype in sorted(entity_types_map.keys()):
            tables[etype] = terra_api.get_entities(
                workspace_namespace=ns,
                workspace_name=nm,
                entity_type=etype,
                headers=headers,
                api_url=api_url,
            )

        queries, summary = build_workspace_onboarding_queries(
            workspace_namespace=ns,
            workspace_name=nm,
            workspace_info=ws_entry,
            tables=tables,
            cred_dict=cred_dict,
            encryption_passphrase=passphrase,
            key_name=key_name,
            modality=modality,
            pull_workspace_bucket=pull_workspace_bucket,
            max_workers=max_workers,
            api_url=api_url,
            disambiguate_source_name=project_counts.get(pid, 0) > 1,
        )
        summaries.append(summary)
        logger.info(f"Discovered workspace summary: {json.dumps(summary, indent=2)}")

        if dry_run:
            logger.info(
                f"[DRY RUN] Would execute {len(queries)} SQL upsert queries for {ns}/{nm}."
            )
        else:
            db.execute_queries(config_file=config_file, queries=queries, show_commands=False)
            logger.info(
                f"Upserted {summary['site_count']} sites + COHORT and "
                f"{summary['subject_count']} subjects for {ns}/{nm} ({pid})."
            )

    return summaries


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Auto-discover Terra workspaces, sites, and subjects into Lochness DB."
    )
    parser.add_argument(
        "--b2c-token-json",
        type=Path,
        default=None,
        help="Path to Terra Microsoft B2C token JSON (e.g. ~/terra_b2c_token.json)",
    )
    parser.add_argument(
        "--key-name",
        default="terra_prod",
        help="KeyStore entry name (default: terra_prod)",
    )
    parser.add_argument(
        "--namespace-pattern",
        action="append",
        dest="namespace_patterns",
        default=None,
        help=(
            "Glob filter on workspace namespace or namespace/name "
            "(repeatable; default discovers all non-public AMP-SCZ workspaces)"
        ),
    )
    parser.add_argument(
        "--modality",
        default="terra",
        help="PHOENIX modality subdirectory (default: terra)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=16,
        help="Parallel GCS download worker threads per data source (default: 16)",
    )
    parser.add_argument(
        "--no-pull-workspace-bucket",
        action="store_true",
        help="Disable full workspace GCS bucket mirror on COHORT data sources",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect Terra workspaces and log summary without modifying PostgreSQL",
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        logger.error(f"Config file does not exist: {config_file}")
        sys.exit(1)

    logs.configure_logging(
        config_file=config_file,
        module_name=MODULE_NAME,
        logger=logger,
        noisy_modules=NOISY_MODULES,
    )

    patterns = args.namespace_patterns or ["*amp*scz*", "*ampscz*", "*prescient*", "*pronet*"]
    results = refresh_terra_metadata(
        config_file=config_file,
        b2c_token_json=args.b2c_token_json,
        key_name=args.key_name,
        namespace_patterns=patterns,
        modality=args.modality,
        pull_workspace_bucket=not args.no_pull_workspace_bucket,
        max_workers=args.max_workers,
        dry_run=args.dry_run,
    )
    logger.info(f"Completed Terra metadata refresh for {len(results)} workspace(s).")
