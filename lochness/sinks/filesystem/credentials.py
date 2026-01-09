"""
Inserts and retrieves filesystem credentials in the KeyStore.

Supports both local filesystem paths and remote SSH destinations.
"""
import logging
import json
from typing import Dict, Optional

from lochness.helpers import utils, db, config
from lochness.models.keystore import KeyStore

logger = logging.getLogger(__name__)


def insert_filesystem_cred(
    key_name: str,
    project_id: str,
    destination_path: str,
    ssh_host: Optional[str] = None,
    ssh_user: Optional[str] = None,
    ssh_key_path: Optional[str] = None,
    ssh_port: int = 22,
) -> None:
    """
    Inserts or updates filesystem credentials in the KeyStore.

    For local filesystems, only destination_path is required.
    For remote filesystems via SSH, provide ssh_host, ssh_user, and optionally
    ssh_key_path.

    Args:
        key_name (str): The name to identify these credentials in the keystore.
        project_id (str): The project ID associated with these credentials.
        destination_path (str): The base destination path on the target filesystem.
        ssh_host (Optional[str]): The SSH host for remote destinations.
        ssh_user (Optional[str]): The SSH username for remote destinations.
        ssh_key_path (Optional[str]): Path to the SSH private key file.
        ssh_port (int): The SSH port number. Defaults to 22.
    """
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.get_encryption_passphrase(config_file=config_file)

    filesystem_credentials: Dict[str, Optional[str | int]] = {
        "destination_path": destination_path,
        "ssh_host": ssh_host,
        "ssh_user": ssh_user,
        "ssh_key_path": ssh_key_path,
        "ssh_port": ssh_port,
    }

    my_key = KeyStore(
        key_name=key_name,
        key_value=json.dumps(filesystem_credentials),
        key_type="filesystem",
        project_id=project_id,
        key_metadata={
            "description": "Credentials for filesystem data sink via rsync",
            "created_by": "lochness_script",
        },
    )

    insert_query = my_key.to_sql_query(encryption_passphrase=encryption_passphrase)

    db.execute_queries(
        config_file=config_file,
        queries=[insert_query],
        show_commands=False,
    )
    logger.info(
        f"Inserted/updated filesystem credentials with key_name '{key_name}' "
        f"for project '{project_id}'."
    )


def get_filesystem_cred(
    key_name: str,
    project_id: str,
) -> Dict[str, Optional[str | int]]:
    """
    Retrieves filesystem credentials from the KeyStore.

    Args:
        key_name (str): The name of the credentials in the keystore.
        project_id (str): The project ID associated with these credentials.

    Returns:
        Dict[str, Optional[str | int]]: A dictionary containing filesystem
            credentials including 'destination_path' and optionally SSH details.

    Raises:
        ValueError: If the credentials are not found in the keystore.
    """
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.get_encryption_passphrase(config_file=config_file)

    keystore = KeyStore.get_by_name_and_project(
        config_file,
        key_name,
        project_id,
        encryption_passphrase,
    )
    if keystore:
        return json.loads(keystore.key_value)

    raise ValueError(
        f"Filesystem credentials with key_name '{key_name}' "
        f"for project '{project_id}' not found in keystore"
    )
