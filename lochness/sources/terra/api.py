"""
Module for authenticating and interacting with Terra (app.terra.bio) via
the Terra/FireCloud REST API and Google Cloud Storage (GCS).
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import google.auth
import requests
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import storage
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

DEFAULT_TERRA_API_URL = "https://api.firecloud.org"

TERRA_OAUTH_SCOPES: Tuple[str, ...] = (
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/devstorage.read_only",
)


def get_credentials(
    cred_dict: Optional[Dict[str, Any]] = None,
    scopes: Sequence[str] = TERRA_OAUTH_SCOPES,
) -> Credentials:
    """
    Build Google OAuth2 credentials for Terra API and GCS access.

    Supported credential inputs:
    1. ``None`` or ``{"auth_mode": "adc"}``: Uses Google Application Default
       Credentials (ADC), e.g. from ``GOOGLE_APPLICATION_CREDENTIALS`` or a
       GCE/GKE metadata server.
    2. ``{"service_account_file": "/path/to/sa.json"}``: Loads a Google Cloud
       Service Account JSON key from disk.
    3. A Service Account JSON dictionary (either directly or nested under
       ``"service_account_info"``) stored in Lochness ``KeyStore``.

    Args:
        cred_dict (Optional[Dict[str, Any]]): Decrypted keystore dictionary or None.
        scopes (Sequence[str]): OAuth2 scopes required for Terra and GCS.

    Returns:
        Credentials: Configured Google credentials object.

    Raises:
        RuntimeError: If credentials cannot be loaded or validated.
    """
    scope_list = list(scopes)

    if cred_dict is None or cred_dict.get("auth_mode") == "adc":
        logger.info("Authenticating to Terra using Google Application Default Credentials.")
        try:
            credentials, _ = google.auth.default(scopes=scope_list)
            return credentials
        except Exception as e:
            logger.error(f"Failed to obtain Google Application Default Credentials: {e}")
            raise RuntimeError(
                f"Failed to obtain Google Application Default Credentials: {e}"
            ) from e

    if "service_account_file" in cred_dict:
        sa_path = Path(cred_dict["service_account_file"])
        if not sa_path.is_file():
            raise RuntimeError(f"Service account file not found: {sa_path}")
        logger.info(f"Loading Terra service account credentials from {sa_path}.")
        try:
            return service_account.Credentials.from_service_account_file(
                str(sa_path),
                scopes=scope_list,
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to load service account credentials from {sa_path}: {e}"
            ) from e

    sa_info = cred_dict.get("service_account_info", cred_dict)
    if not isinstance(sa_info, dict):
        raise TypeError(
            "Invalid Terra keystore credentials: expected a service account JSON object."
        )

    required_fields = ("client_email", "private_key", "token_uri")
    missing_fields = [field for field in required_fields if not sa_info.get(field)]
    if missing_fields:
        raise RuntimeError(
            "Invalid Terra service account credentials; missing fields: "
            f"{', '.join(missing_fields)}"
        )

    logger.info(
        f"Loading Terra service account credentials for {sa_info['client_email']}."
    )
    try:
        return service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=scope_list,
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to parse Terra service account credentials: {e}"
        ) from e


def get_auth_headers(credentials: Credentials) -> Dict[str, str]:
    """
    Return HTTP Authorization headers with a fresh Google OAuth2 Bearer token.

    Args:
        credentials (Credentials): Google OAuth2 credentials.

    Returns:
        Dict[str, str]: Authorization header dictionary.

    Raises:
        RuntimeError: If refreshing the access token fails.
    """
    try:
        if not credentials.valid or not credentials.token:
            credentials.refresh(GoogleAuthRequest())
    except Exception as e:
        logger.error(f"Failed to refresh Google OAuth2 access token: {e}")
        raise RuntimeError(
            f"Failed to refresh Google OAuth2 access token: {e}"
        ) from e

    if not credentials.token:
        raise RuntimeError("Google OAuth2 credentials did not yield an access token.")

    return {"Authorization": f"Bearer {credentials.token}"}


def get_workspace(
    workspace_namespace: str,
    workspace_name: str,
    headers: Dict[str, str],
    api_url: str = DEFAULT_TERRA_API_URL,
    timeout: int = 120,
) -> Dict[str, Any]:
    """
    Retrieve workspace metadata from the Terra/FireCloud REST API.

    Args:
        workspace_namespace (str): Terra billing project / workspace namespace.
        workspace_name (str): Terra workspace name.
        headers (Dict[str, str]): Authorization headers with Bearer token.
        api_url (str): Terra/FireCloud API base URL.
        timeout (int): HTTP request timeout in seconds.

    Returns:
        Dict[str, Any]: Workspace metadata payload.

    Raises:
        RuntimeError: If the API call fails.
    """
    base_url = api_url.rstrip("/")
    ns = quote(workspace_namespace, safe="")
    name = quote(workspace_name, safe="")
    url = f"{base_url}/api/workspaces/{ns}/{name}"

    logger.info(
        f"Fetching Terra workspace metadata for {workspace_namespace}/{workspace_name}..."
    )
    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code != 200:
        logger.error(
            f"Failed to get Terra workspace {workspace_namespace}/{workspace_name} "
            f"(HTTP {response.status_code}): {response.text}"
        )
        raise RuntimeError(
            f"Failed to get Terra workspace {workspace_namespace}/{workspace_name} "
            f"(HTTP {response.status_code}): {response.text}"
        )

    return response.json()


def get_workspace_bucket_name(workspace_info: Dict[str, Any]) -> str:
    """
    Extract the GCS bucket name from a Terra workspace metadata response.

    Args:
        workspace_info (Dict[str, Any]): Response from ``get_workspace``.

    Returns:
        str: GCS bucket name (e.g. ``fc-...``).

    Raises:
        RuntimeError: If ``bucketName`` is missing from the workspace metadata.
    """
    workspace = workspace_info.get("workspace", {})
    bucket_name = workspace.get("bucketName")
    if not bucket_name:
        raise RuntimeError("Terra workspace metadata does not contain a 'bucketName'.")
    return bucket_name


def get_entity_types(
    workspace_namespace: str,
    workspace_name: str,
    headers: Dict[str, str],
    api_url: str = DEFAULT_TERRA_API_URL,
    timeout: int = 120,
) -> Dict[str, Any]:
    """
    List all entity types (data tables) available in a Terra workspace.

    Args:
        workspace_namespace (str): Terra billing project / workspace namespace.
        workspace_name (str): Terra workspace name.
        headers (Dict[str, str]): Authorization headers with Bearer token.
        api_url (str): Terra/FireCloud API base URL.
        timeout (int): HTTP request timeout in seconds.

    Returns:
        Dict[str, Any]: Mapping of entity type name to schema/count metadata.

    Raises:
        RuntimeError: If the API call fails.
    """
    base_url = api_url.rstrip("/")
    ns = quote(workspace_namespace, safe="")
    name = quote(workspace_name, safe="")
    url = f"{base_url}/api/workspaces/{ns}/{name}/entities"

    logger.info(
        f"Listing Terra entity types for {workspace_namespace}/{workspace_name}..."
    )
    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code != 200:
        logger.error(
            f"Failed to list Terra entity types for {workspace_namespace}/{workspace_name} "
            f"(HTTP {response.status_code}): {response.text}"
        )
        raise RuntimeError(
            f"Failed to list Terra entity types for {workspace_namespace}/{workspace_name} "
            f"(HTTP {response.status_code}): {response.text}"
        )

    entity_types = response.json()
    logger.info(f"Found {len(entity_types)} entity types: {list(entity_types.keys())}")
    return entity_types


def get_entities(
    workspace_namespace: str,
    workspace_name: str,
    entity_type: str,
    headers: Dict[str, str],
    api_url: str = DEFAULT_TERRA_API_URL,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    """
    Fetch all rows of a specific entity table from a Terra workspace.

    Args:
        workspace_namespace (str): Terra billing project / workspace namespace.
        workspace_name (str): Terra workspace name.
        entity_type (str): Entity table name (e.g. ``sample``, ``participant``).
        headers (Dict[str, str]): Authorization headers with Bearer token.
        api_url (str): Terra/FireCloud API base URL.
        timeout (int): HTTP request timeout in seconds.

    Returns:
        List[Dict[str, Any]]: List of entity records.

    Raises:
        RuntimeError: If the API call fails.
    """
    base_url = api_url.rstrip("/")
    ns = quote(workspace_namespace, safe="")
    name = quote(workspace_name, safe="")
    etype = quote(entity_type, safe="")
    url = f"{base_url}/api/workspaces/{ns}/{name}/entities/{etype}"

    logger.info(
        f"Fetching Terra entities of type '{entity_type}' from "
        f"{workspace_namespace}/{workspace_name}..."
    )
    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code != 200:
        logger.error(
            f"Failed to fetch Terra entities '{entity_type}' for "
            f"{workspace_namespace}/{workspace_name} "
            f"(HTTP {response.status_code}): {response.text}"
        )
        raise RuntimeError(
            f"Failed to fetch Terra entities '{entity_type}' for "
            f"{workspace_namespace}/{workspace_name} "
            f"(HTTP {response.status_code}): {response.text}"
        )

    entities = response.json()
    logger.info(f"Fetched {len(entities)} '{entity_type}' entities.")
    return entities


def create_storage_client(
    credentials: Credentials,
    project: Optional[str] = None,
) -> storage.Client:
    """
    Create an authenticated Google Cloud Storage client.

    Args:
        credentials (Credentials): Google OAuth2 credentials.
        project (Optional[str]): Optional GCP project ID.

    Returns:
        storage.Client: Authenticated GCS client.
    """
    return storage.Client(credentials=credentials, project=project)


def parse_gcs_uri(gcs_uri: str) -> Tuple[str, str]:
    """
    Parse a ``gs://bucket/path/to/object`` URI into ``(bucket_name, blob_name)``.

    Args:
        gcs_uri (str): Google Cloud Storage URI.

    Returns:
        Tuple[str, str]: ``(bucket_name, blob_name)``.

    Raises:
        ValueError: If the URI is not a valid GCS object URI.
    """
    if not isinstance(gcs_uri, str) or not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI (must start with 'gs://'): {gcs_uri!r}")

    remainder = gcs_uri[5:]
    if "/" not in remainder:
        raise ValueError(
            f"Invalid GCS URI (missing object path after bucket): {gcs_uri!r}"
        )

    bucket_name, blob_name = remainder.split("/", 1)
    if not bucket_name or not blob_name or blob_name.endswith("/"):
        raise ValueError(f"Invalid GCS object URI: {gcs_uri!r}")

    return bucket_name, blob_name


def list_gcs_blobs(
    storage_client: storage.Client,
    bucket_name: str,
    prefix: str = "",
    user_project: Optional[str] = None,
) -> List[Any]:
    """
    List all non-directory blobs in a GCS bucket matching an optional prefix.

    Args:
        storage_client (storage.Client): Authenticated GCS client.
        bucket_name (str): GCS bucket name.
        prefix (str): Optional blob key prefix.
        user_project (Optional[str]): Optional GCP project for Requester Pays buckets.

    Returns:
        List[Any]: List of GCS ``Blob`` objects (excluding directory placeholders).
    """
    bucket = storage_client.bucket(bucket_name, user_project=user_project)
    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    result = [blob for blob in blobs if not blob.name.endswith("/")]
    logger.info(
        f"Found {len(result)} blobs in gs://{bucket_name}/{prefix}."
    )
    return result


def get_gcs_blob_metadata(
    storage_client: storage.Client,
    bucket_name: str,
    blob_name: str,
    user_project: Optional[str] = None,
    timeout: int = 120,
) -> Any:
    """
    Fetch metadata for a single GCS blob (including hashes/generation/size).

    Args:
        storage_client (storage.Client): Authenticated GCS client.
        bucket_name (str): GCS bucket name.
        blob_name (str): Blob path within the bucket.
        user_project (Optional[str]): Optional GCP project for Requester Pays buckets.
        timeout (int): Request timeout in seconds.

    Returns:
        Any: Reloaded GCS ``Blob`` object.

    Raises:
        RuntimeError: If the blob metadata cannot be retrieved.
    """
    bucket = storage_client.bucket(bucket_name, user_project=user_project)
    blob = bucket.blob(blob_name)
    try:
        blob.reload(timeout=timeout)
    except Exception as e:
        raise RuntimeError(
            f"Failed to fetch metadata for gs://{bucket_name}/{blob_name}: {e}"
        ) from e
    return blob


def download_gcs_blob(
    blob: Any,
    local_path: Path,
    timeout: int = 300,
) -> None:
    """
    Download a GCS blob atomically to ``local_path``.

    Writes to a hidden ``.part`` temporary file in the destination directory and
    replaces ``local_path`` only after the download completes.

    Args:
        blob (Any): GCS ``Blob`` object.
        local_path (Path): Local destination path.
        timeout (int): Download timeout in seconds.

    Raises:
        RuntimeError: If the download fails.
    """
    local_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = local_path.with_name(f".{local_path.name}.{os.getpid()}.part")
    logger.info(f"Downloading gs://{blob.bucket.name}/{blob.name} to {local_path}...")
    try:
        blob.download_to_filename(str(temp_path), timeout=timeout)
        os.replace(temp_path, local_path)
        logger.info(f"Downloaded {local_path}.")
    except Exception as e:
        raise RuntimeError(
            f"Failed to download gs://{blob.bucket.name}/{blob.name} to {local_path}: {e}"
        ) from e
    finally:
        temp_path.unlink(missing_ok=True)
