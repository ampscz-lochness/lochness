"""
Insets and retrieves MinIO credentials in the KeyStore.
"""
import json
import logging
from typing import Dict

from lochness.helpers import config, db, utils
from lochness.models.keystore import KeyStore

logger = logging.getLogger(__name__)


def insert_minio_cred(
    key_name: str,
    access_key: str,
    secret_key: str,
    endpoint_url: str,
    project_id: str,
) -> None:
    """Inserts or updates MinIO credentials in the KeyStore.

    Args:
        key_name (str): The name to identify these credentials in the keystore.
        access_key (str): The MinIO access key.
        secret_key (str): The MinIO secret key.
        endpoint_url (str): The MinIO endpoint URL (e.g., 'http://localhost:9000').
        project_id (str): The project ID associated with these credentials.
    """
    config_file = utils.get_config_file_path()

    encryption_passphrase = config.get_encryption_passphrase(config_file=config_file)

    minio_credentials = {
        "access_key": access_key,
        "secret_key": secret_key,
        "endpoint_url": endpoint_url,
    }

    my_key = KeyStore(
        key_name=key_name,
        key_value=json.dumps(minio_credentials),
        key_type="minio",
        project_id=project_id,
        key_metadata={
            "description": "Credentials for MinIO object storage",
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
        f"Inserted/updated MinIO credentials with key_name '{key_name}' for project '{project_id}'."
    )


def get_minio_cred(
    key_name: str,
    project_id: str,
) -> Dict[str, str]:
    """Retrieves MinIO credentials from the KeyStore.

    Args:
        key_name (str): The name of the credentials in the keystore.
        project_id (str): The project ID associated with these credentials.

    Returns:
        Dict[str, str]: A dictionary containing 'access_key', 'secret_key', and 'endpoint_url'.
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
            f"MinIO credentials with key_name '{key_name}' "
            f"for project '{project_id}' "
            f"not found in keystore"
        )
