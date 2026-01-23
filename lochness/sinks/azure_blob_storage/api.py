"""
Azure Blob Storage API interactions.
"""

import logging
from pathlib import Path
from typing import Dict, Optional

from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


def get_blob_service_client(connection_string: str) -> BlobServiceClient:
    """
    Create a BlobServiceClient using the provided connection string.

    Args:
        connection_string (str): The Azure Blob Storage connection string.

    Returns:
        BlobServiceClient: An instance of BlobServiceClient.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        return blob_service_client
    except AzureError as e:
        logger.error(f"Azure error creating BlobServiceClient: {e}")
        raise


def check_if_container_exists(
    connection_string: str,
    container_name: str,
) -> bool:
    """
    Check if an Azure Blob Storage container exists.

    Args:
        connection_string (str): The Azure Blob Storage connection string.
        container_name (str): The name of the container to check.

    Returns:
        bool: True if the container exists, False otherwise.
    """
    try:
        blob_service_client = get_blob_service_client(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        return container_client.exists()
    except AzureError as e:
        logger.error(f"Azure error checking if container exists: {e}")
        return False


def upload_to_blob(
    connection_string: str,
    container_name: str,
    blob_name: str,
    source_file_path: Path,
    tags: Optional[Dict[str, str]] = None,
    metadata: Optional[Dict[str, str]] = None,
) -> None:
    """
    Upload a file to an Azure Blob Storage container with optional metadata.

    Args:
        connection_string (str): The Azure Blob Storage connection string.
        container_name (str): The name of the container to upload to.
        blob_name (str): The name of the blob (file) in the container.
        source_file_path (Path): The local path to the file to be uploaded.
        tags (Optional[Dict[str, str]]): Optional tags to associate with the blob.
        metadata (Optional[Dict[str, str]]): Optional metadata to associate with the blob.

    Returns:
        None
    """
    blob_service_client = get_blob_service_client(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    if tags is None:
        tags = {}

    # Cast all tags values to strings
    tags = {k: str(v) for k, v in tags.items()}

    if metadata is None:
        metadata = {}

    # Cast all metadata values to strings
    metadata = {k: str(v) for k, v in metadata.items()}

    with open(file=source_file_path, mode="rb") as data:
        container_client.upload_blob(
            name=blob_name, data=data, overwrite=True, metadata=metadata, tags=tags
        )

    return None


def download_from_blob(
    connection_string: str,
    container_name: str,
    blob_name: str,
    destination_file_path: Path,
) -> None:
    """
    Download a file from an Azure Blob Storage container.

    Args:
        connection_string (str): The Azure Blob Storage connection string.
        container_name (str): The name of the container to download from.
        blob_name (str): The name of the blob (file) in the container.
        destination_file_path (Path): The local path where the file will be saved.

    Returns:
        None

    Raises:
        AzureError: If there's an error downloading the blob.
    """
    blob_service_client = get_blob_service_client(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    # Ensure the destination directory exists
    destination_file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file=destination_file_path, mode="wb") as download_file:
        download_stream = blob_client.download_blob()
        download_file.write(download_stream.readall())

    return None
