#!/usr/bin/env python
"""
Helper script to insert a Terra DataSource entry into the Lochness database.

Usage:
    python test/lochness/sources/terra/insert_terra_data_source.py \
        --name ProCAN_Terra_Genomics \
        --project-id ProCAN \
        --site-id YA \
        --workspace-namespace my-billing-project \
        --workspace-name my-terra-workspace \
        --keystore-name terra_prod
"""

import argparse
import sys
from pathlib import Path

file = Path(__file__).resolve()
root_dir = None
for parent in file.parents:
    if parent.name in ("lochness_v2", "lochness") and (parent / "lochness").is_dir():
        root_dir = parent
        break
if root_dir is None:
    root_dir = file.parents[4]
if str(root_dir) not in sys.path:
    sys.path.append(str(root_dir))

from lochness.helpers import db, utils
from lochness.models.data_source import DataSource
from lochness.models.supported_data_source_types import (
    populate_supported_data_source_types,
)
from lochness.sources.terra.models.data_source import TerraDataSourceMetadata


def main() -> None:
    """Insert a Terra DataSource record into the Lochness database."""
    parser = argparse.ArgumentParser(
        description="Insert a Terra DataSource into the Lochness database."
    )
    parser.add_argument("--name", required=True, help="Unique data source name")
    parser.add_argument("--project-id", required=True, help="Lochness project ID")
    parser.add_argument("--site-id", required=True, help="Lochness site ID")
    parser.add_argument(
        "--workspace-namespace",
        required=True,
        help="Terra billing project / workspace namespace",
    )
    parser.add_argument(
        "--workspace-name",
        required=True,
        help="Terra workspace name",
    )
    parser.add_argument(
        "--keystore-name",
        default=None,
        help="KeyStore entry name containing GCP Service Account JSON (omit for ADC)",
    )
    parser.add_argument(
        "--modality",
        default="terra",
        help="PHOENIX modality subdirectory (default: terra)",
    )
    parser.add_argument(
        "--subject-id-column",
        default="name",
        help="Entity column matching subject ID (default: name)",
    )
    parser.add_argument(
        "--subject-id-metadata-key",
        default=None,
        help="Optional key in subject_metadata for mapping Lochness subject to Terra ID",
    )
    parser.add_argument(
        "--pull-bucket-files",
        action="store_true",
        help="Also sync files directly from subject folders in the workspace GCS bucket",
    )
    parser.add_argument(
        "--bucket-prefix",
        default="",
        help="Optional prefix inside workspace GCS bucket",
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()

    metadata = TerraDataSourceMetadata(
        keystore_name=args.keystore_name,
        workspace_namespace=args.workspace_namespace,
        workspace_name=args.workspace_name,
        modality=args.modality,
        subject_id_column=args.subject_id_column,
        subject_id_metadata_key=args.subject_id_metadata_key,
        pull_bucket_files=args.pull_bucket_files,
        bucket_prefix=args.bucket_prefix,
    )

    data_source = DataSource(
        data_source_name=args.name,
        is_active=True,
        site_id=args.site_id,
        project_id=args.project_id,
        data_source_type="terra",
        data_source_metadata=metadata.model_dump(),
    )

    queries = populate_supported_data_source_types() + [data_source.to_sql_query()]
    db.execute_queries(
        config_file=config_file,
        queries=queries,
        show_commands=False,
    )
    print(
        f"Inserted Terra data source '{args.name}' "
        f"({args.workspace_namespace}/{args.workspace_name}) "
        f"for {args.project_id}::{args.site_id}."
    )


if __name__ == "__main__":
    main()
