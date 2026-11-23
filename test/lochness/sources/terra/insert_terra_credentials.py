#!/usr/bin/env python
"""
Helper script to insert Terra (GCP Service Account) credentials into Lochness KeyStore.

Usage:
    python test/lochness/sources/terra/insert_terra_credentials.py \
        --key-name terra_prod \
        --project-id ProCAN \
        --sa-json /path/to/terra-service-account.json
"""

import argparse
import json
import os
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

from lochness.helpers import config, db, utils
from lochness.models.keystore import KeyStore


def main() -> None:
    """Insert Terra GCP Service Account JSON credentials into Lochness KeyStore."""
    parser = argparse.ArgumentParser(
        description="Insert Terra GCP Service Account credentials into Lochness KeyStore."
    )
    parser.add_argument(
        "--key-name",
        default="terra_prod",
        help="Name of the KeyStore entry (default: terra_prod)",
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="Lochness project ID",
    )
    parser.add_argument(
        "--sa-json",
        type=Path,
        default=None,
        help=(
            "Path to the GCP Service Account JSON key file. "
            "Falls back to TERRA_SA_JSON_PATH or GOOGLE_APPLICATION_CREDENTIALS env var."
        ),
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    sa_path = args.sa_json or (
        Path(os.environ["TERRA_SA_JSON_PATH"])
        if "TERRA_SA_JSON_PATH" in os.environ
        else (
            Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
            if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ
            else None
        )
    )

    if sa_path is None or not sa_path.is_file():
        print(
            "ERROR: Service account JSON file not found. Provide --sa-json or set "
            "TERRA_SA_JSON_PATH / GOOGLE_APPLICATION_CREDENTIALS."
        )
        sys.exit(1)

    sa_info = json.loads(sa_path.read_text(encoding="utf-8"))
    for field in ("client_email", "private_key", "token_uri"):
        if not sa_info.get(field):
            print(f"ERROR: Service account JSON is missing required field '{field}'.")
            sys.exit(1)

    encryption_passphrase = config.get_encryption_passphrase(config_file)
    terra_key = KeyStore(
        key_name=args.key_name,
        key_value=json.dumps(sa_info),
        key_type="terra",
        project_id=args.project_id,
        key_metadata={
            "description": "GCP Service Account credentials for Terra (app.terra.bio)",
            "client_email": sa_info["client_email"],
            "created_by": "script",
        },
    )

    db.execute_queries(
        config_file=config_file,
        queries=terra_key.init_db_table_query(),
        show_commands=False,
    )
    db.execute_queries(
        config_file=config_file,
        queries=[terra_key.to_sql_query(encryption_passphrase=encryption_passphrase)],
        show_commands=False,
    )
    print(
        f"Inserted Terra credentials '{args.key_name}' "
        f"({sa_info['client_email']}) for project '{args.project_id}'."
    )


if __name__ == "__main__":
    main()
