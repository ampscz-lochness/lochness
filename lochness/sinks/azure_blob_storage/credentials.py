"""
Inserts and retrieves Azure Blob Storage credentials in the KeyStore.
"""
import json
import logging
from typing import Dict

from lochness.helpers import config, db, utils
from lochness.models.keystore import KeyStore

logger = logging.getLogger(__name__)


def insert_azure_blob_cred(
    key_name: str,
    connection_string: str,
    project_id: str,
) -> None:
    """Inserts or updates Azure Blob Storage credentials in the KeyStore.

    Args:
        key_name (str): The name to identify these credentials in the keystore.
        connection_string (str): The Azure Blob Storage connection string.
        project_id (str): The project ID associated with these credentials.
    """
    config_file = utils.get_config_file_path()
    encryption_passphrase = config.get_encryption_passphrase(config_file=config_file)

    azure_credentials = {
        "connection_string": connection_string,
    }

    my_key = KeyStore(
        key_name=key_name,
        key_value=json.dumps(azure_credentials),
        key_type="azure_blob",
        project_id=project_id,
        key_metadata={
            "description": "Credentials for Azure Blob Storage",
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
        (
            f"Inserted/updated Azure Blob Storage credentials "
            f"with key_name '{key_name}' "
            f"for project '{project_id}'."
        )
    )


def get_azure_blob_cred(
    key_name: str,
    project_id: str,
) -> Dict[str, str]:
    """Retrieves Azure Blob Storage credentials from the KeyStore.

    Args:
        key_name (str): The name of the credentials in the keystore.
        project_id (str): The project ID associated with these credentials.

    Returns:
        Dict[str, str]: A dictionary containing 'connection_string' and 'container_name'.
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
    else:
        raise ValueError(
            f"Azure Blob Storage credentials with key_name '{key_name}' "
            f"for project '{project_id}' "
            f"not found in keystore"
        )
