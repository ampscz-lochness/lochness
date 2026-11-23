#!/usr/bin/env python
"""
Helper script to insert Terra credentials into Lochness KeyStore.

Supports both:
1. Terra Microsoft / Azure AD B2C interactive login (no Google account needed):
    python test/lochness/sources/terra/insert_terra_credentials.py \
        --key-name terra_prod \
        --project-id ProCAN \
        --microsoft-login

2. Existing Terra B2C refresh token JSON file:
    python test/lochness/sources/terra/insert_terra_credentials.py \
        --key-name terra_prod \
        --project-id ProCAN \
        --b2c-token-json ~/.config/lochness/terra_b2c_token.json

3. Google Cloud Service Account JSON key:
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
from typing import Any, Dict

file = Path(__file__).resolve()
root_dir = None
for parent in file.parents:
    if parent.name in ("lochness_v2", "lochness") and (parent / "lochness").is_dir():
        root_dir = parent
        break
if root_dir is None:
    root_dir = file.parents[4]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from lochness.helpers import config, db, utils
from lochness.models.keystore import KeyStore
from lochness.sources.terra import api as terra_api


def main() -> None:
    """Insert Terra credentials (Microsoft B2C or GCP Service Account) into KeyStore."""
    parser = argparse.ArgumentParser(
        description="Insert Terra credentials into Lochness KeyStore."
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
        "--microsoft-login",
        action="store_true",
        help="Sign in interactively via Terra Azure AD B2C (Sign in with Microsoft) and store the refresh token",
    )
    parser.add_argument(
        "--b2c-token-json",
        type=Path,
        default=None,
        help="Path to an existing Terra B2C token JSON file containing auth_mode=terra_b2c and refresh_token",
    )
    parser.add_argument(
        "--sa-json",
        type=Path,
        default=None,
        help=(
            "Path to a GCP Service Account JSON key file. "
            "Falls back to TERRA_SA_JSON_PATH or GOOGLE_APPLICATION_CREDENTIALS env var."
        ),
    )
    args = parser.parse_args()

    config_file = utils.get_config_file_path()
    cred_payload: Dict[str, Any]
    key_metadata: Dict[str, Any]

    if args.microsoft_login:
        cred_payload = terra_api.interactive_b2c_login()
        key_metadata = {
            "description": "Terra Azure AD B2C (Microsoft) OAuth2 refresh token",
            "auth_mode": "terra_b2c",
            "created_by": "script",
        }
    elif args.b2c_token_json is not None:
        if not args.b2c_token_json.is_file():
            print(f"ERROR: B2C token JSON file not found: {args.b2c_token_json}")
            sys.exit(1)
        cred_payload = json.loads(args.b2c_token_json.read_text(encoding="utf-8"))
        if cred_payload.get("auth_mode") != "terra_b2c" or not cred_payload.get(
            "refresh_token"
        ):
            print(
                "ERROR: B2C token JSON must include auth_mode='terra_b2c' and refresh_token."
            )
            sys.exit(1)
        key_metadata = {
            "description": "Terra Azure AD B2C (Microsoft) OAuth2 refresh token",
            "auth_mode": "terra_b2c",
            "created_by": "script",
        }
    else:
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
                "ERROR: Provide --microsoft-login, --b2c-token-json, or --sa-json "
                "(or set TERRA_SA_JSON_PATH / GOOGLE_APPLICATION_CREDENTIALS)."
            )
            sys.exit(1)

        cred_payload = json.loads(sa_path.read_text(encoding="utf-8"))
        for field in ("client_email", "private_key", "token_uri"):
            if not cred_payload.get(field):
                print(
                    f"ERROR: Service account JSON is missing required field '{field}'."
                )
                sys.exit(1)
        key_metadata = {
            "description": "GCP Service Account credentials for Terra (app.terra.bio)",
            "auth_mode": "service_account",
            "client_email": cred_payload["client_email"],
            "created_by": "script",
        }

    encryption_passphrase = config.get_encryption_passphrase(config_file)
    terra_key = KeyStore(
        key_name=args.key_name,
        key_value=json.dumps(cred_payload),
        key_type="terra",
        project_id=args.project_id,
        key_metadata=key_metadata,
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
        f"({key_metadata['auth_mode']}) for project '{args.project_id}'."
    )


if __name__ == "__main__":
    main()
