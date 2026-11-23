"""
Module for authenticating and interacting with Terra (app.terra.bio) via
the Terra/FireCloud REST API and Google Cloud Storage (GCS).
"""

import base64
import logging
import os
import secrets
import time
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import google.auth
import requests
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import storage
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as GoogleTokenCredentials

logger = logging.getLogger(__name__)

DEFAULT_TERRA_API_URL = "https://api.firecloud.org"
DEFAULT_TERRA_SAM_URL = "https://sam.dsde-prod.broadinstitute.org"
DEFAULT_TERRA_B2C_AUTHORIZE_URL = (
    "https://terraprodb2c.b2clogin.com/terraprodb2c.onmicrosoft.com/"
    "b2c_1a_signup_signin_tsps_prod/oauth2/v2.0/authorize"
)
DEFAULT_TERRA_B2C_TOKEN_URL = (
    "https://terraprodb2c.b2clogin.com/terraprodb2c.onmicrosoft.com/"
    "b2c_1a_signup_signin_tsps_prod/oauth2/v2.0/token"
)
DEFAULT_TERRA_B2C_CLIENT_ID = "4aefc4ac-d5be-4440-ae9a-aefe4ceb098c"
DEFAULT_TERRA_B2C_CALLBACK_PORT = 10444

TERRA_OAUTH_SCOPES: Tuple[str, ...] = (
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/devstorage.read_only",
)

GCS_READ_ONLY_SCOPES: Tuple[str, ...] = (
    "https://www.googleapis.com/auth/devstorage.read_only",
)


class TerraB2CCredentials:
    """
    OAuth2 credentials backed by Terra's Azure AD B2C refresh token
    (used for Microsoft / institutional Terra accounts).

    Supports:
    - Refreshing the Terra B2C Bearer token for Terra/FireCloud REST API calls.
    - Exchanging the Terra B2C token via Terra Sam
      (``POST /api/google/v1/user/petServiceAccount/token``) for a short-lived
      Google Cloud Storage access token without requiring a Google account.
    """

    def __init__(
        self,
        refresh_token: str,
        client_id: str = DEFAULT_TERRA_B2C_CLIENT_ID,
        token_url: str = DEFAULT_TERRA_B2C_TOKEN_URL,
        sam_api_url: str = DEFAULT_TERRA_SAM_URL,
        access_token: Optional[str] = None,
        expires_in: Optional[int] = None,
    ) -> None:
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.token_url = token_url
        self.sam_api_url = sam_api_url.rstrip("/")
        self.token: Optional[str] = access_token
        self._expires_at: float = (
            time.monotonic() + max(int(expires_in) - 60, 0)
            if access_token and expires_in
            else 0.0
        )

    @property
    def valid(self) -> bool:
        """Return True if a non-expired access token is currently cached."""
        return bool(self.token) and time.monotonic() < self._expires_at

    def refresh(self, timeout: int = 60) -> None:
        """Exchange the stored B2C refresh token for a fresh Terra access token."""
        basic_auth = base64.b64encode(f"{self.client_id}:".encode()).decode()
        response = requests.post(
            self.token_url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {basic_auth}",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            },
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to refresh Terra B2C token (HTTP {response.status_code}): "
                f"{response.text}"
            )

        payload = response.json()
        new_token = payload.get("access_token") or payload.get("id_token")
        if not new_token:
            raise RuntimeError("Terra B2C token refresh response did not include a token.")

        self.token = new_token
        if payload.get("refresh_token"):
            self.refresh_token = payload["refresh_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._expires_at = time.monotonic() + max(expires_in - 60, 0)

    def get_gcs_access_token(
        self,
        scopes: Sequence[str] = GCS_READ_ONLY_SCOPES,
        timeout: int = 60,
    ) -> str:
        """
        Exchange the Terra B2C token via Terra Sam for a temporary Google OAuth2
        access token belonging to the user's Terra Pet Service Account.
        """
        if not self.valid:
            self.refresh(timeout=timeout)

        url = f"{self.sam_api_url}/api/google/v1/user/petServiceAccount/token"
        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            json=list(scopes),
            timeout=timeout,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to exchange Terra B2C token for Sam pet service account GCS "
                f"token (HTTP {response.status_code}): {response.text}"
            )

        pet_token = response.json()
        if not isinstance(pet_token, str) or not pet_token:
            raise RuntimeError("Terra Sam returned an invalid pet service account token.")
        return pet_token


TerraCredentials = Credentials | TerraB2CCredentials


def interactive_b2c_login(
    client_id: str = DEFAULT_TERRA_B2C_CLIENT_ID,
    authorize_url: str = DEFAULT_TERRA_B2C_AUTHORIZE_URL,
    token_url: str = DEFAULT_TERRA_B2C_TOKEN_URL,
    sam_api_url: str = DEFAULT_TERRA_SAM_URL,
    port: int = DEFAULT_TERRA_B2C_CALLBACK_PORT,
    timeout: int = 60,
) -> Dict[str, Any]:
    """
    Perform interactive browser OAuth2 login against Terra Azure AD B2C
    (supporting Microsoft / institutional accounts) and return a KeyStore-ready
    credential dictionary containing ``refresh_token``.
    """
    redirect_uri = f"http://localhost:{port}"
    scopes = f"offline_access openid email profile {client_id}"
    state = secrets.token_urlsafe(24)
    result_box: Dict[str, Optional[str]] = {"code": None, "error": None}

    class _CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            if "error" in params:
                result_box["error"] = params.get("error_description", params["error"])[0]
                self.send_response(400)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<h3>Terra login failed. You can close this tab.</h3>")
                return
            if "code" in params:
                if params.get("state", [None])[0] != state:
                    result_box["error"] = "OAuth state mismatch"
                    self.send_response(400)
                    self.end_headers()
                    return
                result_box["code"] = params["code"][0]
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(
                    b"<h3>Terra sign-in succeeded! You can close this tab.</h3>"
                )
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", port), _CallbackHandler)
    query = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": scopes,
            "state": state,
            "prompt": "login",
        }
    )
    auth_url = f"{authorize_url}?{query}"
    logger.info("Opening Terra B2C sign-in page in your browser...")
    webbrowser.open(auth_url)

    try:
        while result_box["code"] is None and result_box["error"] is None:
            server.handle_request()
    finally:
        server.server_close()

    if result_box["error"]:
        raise RuntimeError(f"Terra B2C login failed: {result_box['error']}")

    basic_auth = base64.b64encode(f"{client_id}:".encode()).decode()
    resp = requests.post(
        token_url,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic_auth}",
        },
        data={
            "grant_type": "authorization_code",
            "code": result_box["code"],
            "redirect_uri": redirect_uri,
        },
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Terra B2C authorization code exchange failed (HTTP {resp.status_code}): "
            f"{resp.text}"
        )

    payload = resp.json()
    refresh_token = payload.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("Terra B2C token response did not include a refresh_token.")

    return {
        "auth_mode": "terra_b2c",
        "client_id": client_id,
        "token_url": token_url,
        "sam_api_url": sam_api_url,
        "refresh_token": refresh_token,
    }


def get_credentials(
    cred_dict: Optional[Dict[str, Any]] = None,
    scopes: Sequence[str] = TERRA_OAUTH_SCOPES,
) -> TerraCredentials:
    """
    Build credentials for Terra REST API and GCS access.

    Supported credential inputs:
    1. ``{"auth_mode": "terra_b2c", "refresh_token": "..."}``: Terra Azure AD B2C
       refresh token (Microsoft / institutional Terra login). Automatically
       exchanges via Terra Sam for GCS Pet Service Account tokens when needed.
    2. ``None`` or ``{"auth_mode": "adc"}``: Uses Google Application Default
       Credentials (ADC), e.g. from ``GOOGLE_APPLICATION_CREDENTIALS`` or a
       GCE/GKE metadata server.
    3. ``{"service_account_file": "/path/to/sa.json"}``: Loads a Google Cloud
       Service Account JSON key from disk.
    4. A Service Account JSON dictionary (either directly or nested under
       ``"service_account_info"``) stored in Lochness ``KeyStore``.

    Args:
        cred_dict (Optional[Dict[str, Any]]): Decrypted keystore dictionary or None.
        scopes (Sequence[str]): OAuth2 scopes required for Google auth modes.

    Returns:
        TerraCredentials: Configured ``TerraB2CCredentials`` or Google ``Credentials``.

    Raises:
        RuntimeError: If credentials cannot be loaded or validated.
    """
    scope_list = list(scopes)

    if cred_dict is not None and cred_dict.get("auth_mode") == "terra_b2c":
        refresh_token = cred_dict.get("refresh_token")
        if not refresh_token:
            raise RuntimeError(
                "Invalid Terra B2C credentials; missing 'refresh_token'."
            )
        logger.info("Authenticating to Terra using Azure AD B2C (Microsoft) refresh token.")
        return TerraB2CCredentials(
            refresh_token=refresh_token,
            client_id=cred_dict.get("client_id", DEFAULT_TERRA_B2C_CLIENT_ID),
            token_url=cred_dict.get("token_url", DEFAULT_TERRA_B2C_TOKEN_URL),
            sam_api_url=cred_dict.get("sam_api_url", DEFAULT_TERRA_SAM_URL),
        )

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


def get_auth_headers(credentials: TerraCredentials) -> Dict[str, str]:
    """
    Return HTTP Authorization headers with a fresh Terra Bearer token.

    Supports both ``TerraB2CCredentials`` (Microsoft / Azure AD B2C) and
    Google ``Credentials`` (Service Account / ADC).

    Args:
        credentials (TerraCredentials): Terra B2C or Google OAuth2 credentials.

    Returns:
        Dict[str, str]: Authorization header dictionary.

    Raises:
        RuntimeError: If refreshing the access token fails.
    """
    try:
        if isinstance(credentials, TerraB2CCredentials):
            if not credentials.valid or not credentials.token:
                credentials.refresh()
        elif not credentials.valid or not credentials.token:
            credentials.refresh(GoogleAuthRequest())
    except Exception as e:
        logger.error(f"Failed to refresh Terra OAuth2 access token: {e}")
        raise RuntimeError(
            f"Failed to refresh Terra OAuth2 access token: {e}"
        ) from e

    if not credentials.token:
        raise RuntimeError("Terra credentials did not yield an access token.")

    return {"Authorization": f"Bearer {credentials.token}"}


def get_gcs_credentials(
    credentials: TerraCredentials,
    scopes: Sequence[str] = GCS_READ_ONLY_SCOPES,
) -> Credentials:
    """
    Obtain Google Cloud Storage credentials from either ``TerraB2CCredentials``
    (via Terra Sam Pet Service Account token exchange) or Google ``Credentials``.
    """
    if isinstance(credentials, TerraB2CCredentials):
        pet_token = credentials.get_gcs_access_token(scopes=scopes)
        return GoogleTokenCredentials(token=pet_token)
    return credentials


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
