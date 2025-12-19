"""
Insert CANTAB credentials into the keystore.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root_dir = None  # pylint: disable=invalid-name
for parent in file.parents:
    if parent.name == "lochness_v2":
        root_dir = parent

sys.path.append(str(root_dir))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
import json
from typing import Dict, Any

from rich.logging import RichHandler

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore

MODULE_NAME = "test.lochness.sources.cantab.insert_creds"

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


def insert_webdav_credentials():
    """
    Insert WebDAV credentials into the keystore.
    """
    config_file = utils.get_config_file_path()

    webdav_creds = config.parse(config_file, "webdav-test")
    key_name = webdav_creds["key_name"]
    project_id = webdav_creds["project_id"]
    username = webdav_creds["username"]
    password = webdav_creds["password"]

    webdav_keystore_dict = {
        "username": username,
        "password": password,
    }

    keystore_entry = KeyStore(
        key_name=key_name,  # type: ignore
        project_id=project_id,  # type: ignore
        key_type="webdav",
        key_value=json.dumps(webdav_keystore_dict),
        key_metadata={
            "description": "Test WebDAV credentials",
            "created_by": "test/lochness/sources/webdav/insert_webdav_credentials.py",
        },
    )

    config_file = utils.get_config_file_path()
    encryption_passphrase = config.get_encryption_passphrase(config_file=config_file)

    queries = [keystore_entry.to_sql_query(encryption_passphrase=encryption_passphrase)]

    db.execute_queries(config_file, queries, show_commands=True)
    logger.info(
        f"Inserted WEBDAV credentials with key_name '{key_name}' into the keystore."
    )


if __name__ == "__main__":

    # Keystore Details
    insert_webdav_credentials()
