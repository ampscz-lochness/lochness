#!/usr/bin/env python
"""
Insert filesystem credentials into the keystore for a specific project.
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

import traceback
from typing import Any, Dict, Optional
import logging

from rich.logging import RichHandler

from lochness.sinks.filesystem.credentials import insert_filesystem_cred
from lochness.helpers import config, utils

config_file = utils.get_config_file_path()

logger = logging.getLogger(__name__)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}

logging.basicConfig(**logargs)


def main():
    """
    Main function to insert filesystem credentials.
    """

    filesystem_creds = config.parse(config_file, 'filesystem-datasink-test')

    destination_path: str = filesystem_creds['destination_path']  # type: ignore
    key_name: str = filesystem_creds['key_name']  # type: ignore
    project_id: str = filesystem_creds['project_id']  # type: ignore

    # Optional SSH parameters for remote destinations
    ssh_host: Optional[str] = filesystem_creds.get('ssh_host')  # type: ignore
    ssh_user: Optional[str] = filesystem_creds.get('ssh_user')  # type: ignore
    ssh_key_path: Optional[str] = filesystem_creds.get('ssh_key_path')  # type: ignore
    ssh_port: int = int(filesystem_creds.get('ssh_port', 22))  # type: ignore

    logger.info(
        f"Attempting to insert filesystem credentials with key_name "
        f"'{key_name}' for project '{project_id}'."
    )
    try:
        insert_filesystem_cred(
            key_name=key_name,
            project_id=project_id,
            destination_path=destination_path,
            ssh_host=ssh_host,
            ssh_user=ssh_user,
            ssh_key_path=ssh_key_path,
            ssh_port=ssh_port,
        )
        logger.info("Filesystem credentials inserted successfully.")
    except Exception as e:  # pylint: disable=broad-except
        logger.info(
            f"An error occurred while inserting filesystem credentials: {e}"
        )
        traceback.print_exc()


if __name__ == "__main__":
    main()
