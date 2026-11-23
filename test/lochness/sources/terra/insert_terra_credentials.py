#!/usr/bin/env python
"""
Inserts Terra credentials into the Lochness KeyStore.

Mirrors `insert_sharepoint_credentials.py` for headless (no-GUI) servers:
1. Reads `[terra]` (`refresh_token`) from `config.ini` first.
2. Falls back to `TERRA_REFRESH_TOKEN` environment variable.
3. If neither is configured (and no `--b2c-token-json` or `--sa-json` file is
   provided), prints a clear error message and exits non-interactively.
4. On a laptop with a browser, run `--get-refresh-token` once to sign in with
   Microsoft and print `export TERRA_REFRESH_TOKEN="..."` for the server.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

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

DEFAULT_KEY_NAME = "terra_prod"
DEFAULT_PROJECT_ID = "ProCAN"


def _load_b2c_from_config_or_env(config_file: Path) -> Optional[Dict[str, Any]]:
    """
    Attempt to load Terra B2C credentials from `config.ini` (`[terra]`) or
    environment variables (`TERRA_REFRESH_TOKEN`), mirroring SharePoint setup.
    """
    refresh_token: Optional[str] = None
    client_id: str = terra_api.DEFAULT_TERRA_B2C_CLIENT_ID
    token_url: str = terra_api.DEFAULT_TERRA_B2C_TOKEN_URL
    sam_api_url: str = terra_api.DEFAULT_TERRA_SAM_URL

    try:
        terra_cfg = config.parse(config_file, "terra")
        refresh_token = terra_cfg.get("refresh_token")  # type: ignore[assignment]
        client_id = terra_cfg.get("client_id") or client_id  # type: ignore[assignment]
        token_url = terra_cfg.get("token_url") or token_url  # type: ignore[assignment]
        sam_api_url = terra_cfg.get("sam_api_url") or sam_api_url  # type: ignore[assignment]
        if refresh_token:
            print("Terra credentials found in config.ini under [terra].")
        else:
            print(
                "Terra refresh_token not specified in config.ini. "
                "Checking environment variables..."
            )
    except Exception as e:  # noqa: BLE001
        print(
            "[terra] section not found or incomplete in config.ini. "
            f"Checking environment variables... (Error: {e})"
        )

    if not refresh_token:
        refresh_token = os.environ.get("TERRA_REFRESH_TOKEN")
        client_id = os.environ.get("TERRA_CLIENT_ID", client_id)
        token_url = os.environ.get("TERRA_TOKEN_URL", token_url)
        sam_api_url = os.environ.get("TERRA_SAM_API_URL", sam_api_url)
        if refresh_token:
            print("Terra credentials found in TERRA_REFRESH_TOKEN environment variable.")

    if not refresh_token:
        return None

    return {
        "auth_mode": "terra_b2c",
        "client_id": client_id,
        "token_url": token_url,
        "sam_api_url": sam_api_url,
        "refresh_token": refresh_token,
    }


def main() -> None:
    """Insert Terra credentials into Lochness KeyStore."""
    parser = argparse.ArgumentParser(
        description="Insert Terra credentials into Lochness KeyStore."
    )
    parser.add_argument(
        "--key-name",
        default=DEFAULT_KEY_NAME,
        help=f"Name of the KeyStore entry (default: {DEFAULT_KEY_NAME})",
    )
    parser.add_argument(
        "--project-id",
        default=DEFAULT_PROJECT_ID,
        help=f"Lochness project ID (default: {DEFAULT_PROJECT_ID})",
    )
    parser.add_argument(
        "--get-refresh-token",
        action="store_true",
        help=(
            "Run on a machine with a browser to sign in with Microsoft and print "
            "TERRA_REFRESH_TOKEN for use on a headless server"
        ),
    )
    parser.add_argument(
        "--b2c-token-json",
        type=Path,
        default=None,
        help="Path to an existing Terra B2C token JSON file",
    )
    parser.add_argument(
        "--sa-json",
        type=Path,
        default=None,
        help="Path to a GCP Service Account JSON key file",
    )
    args = parser.parse_args()

    if args.get_refresh_token:
        cred = terra_api.interactive_b2c_login()
        print("\n# Copy and run this on your headless Lochness server:")
        print(f"export TERRA_REFRESH_TOKEN='{cred['refresh_token']}'")
        return

    config_file = utils.get_config_file_path()
    if not config_file.exists():
        print(f"ERROR: Configuration file not found at {config_file}")
        sys.exit(1)

    print(f"Using configuration file: {config_file}")
    cred_payload: Optional[Dict[str, Any]] = None
    key_metadata: Dict[str, Any] = {}

    if args.b2c_token_json is not None:
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
    elif args.sa_json is not None or "TERRA_SA_JSON_PATH" in os.environ:
        sa_path = args.sa_json or Path(os.environ["TERRA_SA_JSON_PATH"])
        if not sa_path.is_file():
            print(f"ERROR: Service account JSON file not found: {sa_path}")
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
    else:
        cred_payload = _load_b2c_from_config_or_env(config_file)
        if cred_payload is None:
            print(
                "ERROR: Terra credentials (refresh_token) not found in config.ini "
                "or environment variables."
            )
            print(
                "Please ensure it is set in config.ini under [terra] (refresh_token=...) "
                "or as an environment variable (TERRA_REFRESH_TOKEN)."
            )
            sys.exit(1)
        key_metadata = {
            "description": "Terra Azure AD B2C (Microsoft) OAuth2 refresh token",
            "auth_mode": "terra_b2c",
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
        f"Successfully inserted Terra credentials '{args.key_name}' "
        f"({key_metadata['auth_mode']}) for project '{args.project_id}'."
    )


if __name__ == "__main__":
    main()
