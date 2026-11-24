"""
Module for authenticating and interacting with Terra (app.terra.bio) via
the Terra/FireCloud REST API and Google Cloud Storage (GCS).
"""

import base64
import logging
import os
import secrets
import threading
import time
import urllib.parse
import webbrowser
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import quote

import google.auth
import requests
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.cloud import storage
from google.oauth2 import service_account
from requests.adapters import HTTPAdapter

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
DEFAULT_TERRA_B2C_REMOTE_REDIRECT_URI = "https://services.terra.bio/pipelines/cli-auth"

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
        on_refresh_token_rotated: Optional[Callable[[str], None]] = None,
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
        self.on_refresh_token_rotated = on_refresh_token_rotated
        self._lock = threading.Lock()

    @property
    def valid(self) -> bool:
        """Return True if a non-expired access token is currently cached."""
        return bool(self.token) and time.monotonic() < self._expires_at

    def to_cred_dict(self) -> Dict[str, Any]:
        """Serialize current B2C credential state for storing in KeyStore."""
        return {
            "auth_mode": "terra_b2c",
            "client_id": self.client_id,
            "token_url": self.token_url,
            "sam_api_url": self.sam_api_url,
            "refresh_token": self.refresh_token,
        }

    def _refresh_unlocked(self, timeout: int = 60) -> None:
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
        rotated = payload.get("refresh_token")
        if rotated and rotated != self.refresh_token:
            self.refresh_token = rotated
            if self.on_refresh_token_rotated is not None:
                try:
                    self.on_refresh_token_rotated(rotated)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"Failed to persist rotated Terra B2C refresh token: {e}")
        expires_in = int(payload.get("expires_in", 3600))
        self._expires_at = time.monotonic() + max(expires_in - 60, 0)

    def refresh(self, timeout: int = 60) -> None:
        """Exchange the stored B2C refresh token for a fresh Terra access token."""
        with self._lock:
            self._refresh_unlocked(timeout=timeout)

    def ensure_valid(self, timeout: int = 60) -> str:
        """Thread-safely ensure a valid cached B2C token and return it."""
        if self.valid and self.token:
            return self.token
        with self._lock:
            if not self.valid or not self.token:
                self._refresh_unlocked(timeout=timeout)
            if not self.token:
                raise RuntimeError("Terra B2C credentials did not yield an access token.")
            return self.token

    def get_gcs_access_token(
        self,
        scopes: Sequence[str] = GCS_READ_ONLY_SCOPES,
        timeout: int = 60,
    ) -> str:
        """
        Exchange the Terra B2C token via Terra Sam for a temporary Google OAuth2
        access token belonging to the user's Terra Pet Service Account.
        """
        bearer_token = self.ensure_valid(timeout=timeout)

        url = f"{self.sam_api_url}/api/google/v1/user/petServiceAccount/token"
        response = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {bearer_token}",
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


class TerraSamGCSCredentials(Credentials):
    """
    Auto-refreshing Google Cloud Storage credentials backed by
    ``TerraB2CCredentials`` and Terra Sam Pet Service Account token exchange.

    Google Pet Service Account tokens expire after 1 hour; subclassing
    ``google.auth.credentials.Credentials`` enables ``google-cloud-storage``
    to automatically invoke ``refresh()`` during multi-hour transfers.
    """

    def __init__(
        self,
        b2c_credentials: TerraB2CCredentials,
        scopes: Sequence[str] = GCS_READ_ONLY_SCOPES,
        refresh_lifetime_minutes: int = 50,
    ) -> None:
        super().__init__()
        self._b2c_credentials = b2c_credentials
        self._scopes: Tuple[str, ...] = tuple(scopes)
        self._refresh_lifetime_minutes = refresh_lifetime_minutes
        self._lock = threading.Lock()
        self.refresh(None)

    def refresh(self, request: Any) -> None:
        """Fetch a fresh Sam Pet Service Account GCS token."""
        with self._lock:
            pet_token = self._b2c_credentials.get_gcs_access_token(scopes=self._scopes)
            self.token = pet_token
            self.expiry = (
                datetime.now(UTC).replace(tzinfo=None)
                + timedelta(minutes=self._refresh_lifetime_minutes)
            )


TerraCredentials = Credentials | TerraB2CCredentials


def headless_b2c_login(
    client_id: str = DEFAULT_TERRA_B2C_CLIENT_ID,
    authorize_url: str = DEFAULT_TERRA_B2C_AUTHORIZE_URL,
    token_url: str = DEFAULT_TERRA_B2C_TOKEN_URL,
    sam_api_url: str = DEFAULT_TERRA_SAM_URL,
    redirect_uri: str = DEFAULT_TERRA_B2C_REMOTE_REDIRECT_URI,
    code_input_fn: Callable[[str], str] = input,
    timeout: int = 60,
) -> Dict[str, Any]:
    """
    Headless (no-GUI) Terra Azure AD B2C login flow for remote servers,
    analogous to SharePoint's MSAL device-code flow.

    1. Prints an authorization URL that can be opened in any browser on another
       machine (e.g. your laptop) to sign in with Microsoft.
    2. After signing in, Terra redirects the browser to
       ``https://services.terra.bio/pipelines/cli-auth`` which displays a
       verification code.
    3. Prompts for that code in the server terminal and exchanges it for a
       long-lived ``refresh_token`` to store in Lochness ``KeyStore``.
    """
    scopes = f"offline_access openid email profile {client_id}"
    query = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": scopes,
            "prompt": "login",
            "brand": "scientificServices",
        }
    )
    auth_url = f"{authorize_url}?{query}"
    logger.info(
        "Headless Terra sign-in: open the following URL in any browser on your "
        f"laptop/desktop and sign in with Microsoft:\n\n{auth_url}\n"
    )
    print(
        "\n=== Headless Terra Microsoft Sign-In ===\n"
        "1. Open this URL in any browser (e.g. on your laptop):\n\n"
        f"   {auth_url}\n\n"
        "2. Click 'Sign in with Microsoft' and complete login.\n"
        "3. Copy the verification code displayed in your browser and paste it below.\n"
    )
    auth_code = code_input_fn("Enter verification code from browser: ").strip()
    if not auth_code:
        raise RuntimeError("No verification code entered for headless Terra B2C login.")

    basic_auth = base64.b64encode(f"{client_id}:".encode()).decode()
    resp = requests.post(
        token_url,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {basic_auth}",
        },
        data={
            "grant_type": "authorization_code",
            "code": auth_code,
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
        env_refresh_token = (
            os.environ.get("TERRA_REFRESH_TOKEN")
            if cred_dict is None
            else None
        )
        if env_refresh_token:
            logger.info(
                "Authenticating to Terra using TERRA_REFRESH_TOKEN environment variable."
            )
            return TerraB2CCredentials(
                refresh_token=env_refresh_token,
                client_id=os.environ.get(
                    "TERRA_CLIENT_ID", DEFAULT_TERRA_B2C_CLIENT_ID
                ),
                token_url=os.environ.get(
                    "TERRA_TOKEN_URL", DEFAULT_TERRA_B2C_TOKEN_URL
                ),
                sam_api_url=os.environ.get(
                    "TERRA_SAM_API_URL", DEFAULT_TERRA_SAM_URL
                ),
            )

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
            credentials.ensure_valid()
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
    (via auto-refreshing Terra Sam Pet Service Account token exchange) or
    Google ``Credentials``.
    """
    if isinstance(credentials, TerraB2CCredentials):
        return TerraSamGCSCredentials(b2c_credentials=credentials, scopes=scopes)
    return credentials


def list_workspaces(
    headers: Dict[str, str],
    api_url: str = DEFAULT_TERRA_API_URL,
    fields: Optional[str] = None,
    timeout: int = 120,
) -> List[Dict[str, Any]]:
    """
    List all Terra workspaces accessible to the authenticated user.

    Args:
        headers (Dict[str, str]): Authorization headers with Bearer token.
        api_url (str): Terra/FireCloud API base URL.
        fields (Optional[str]): Optional comma-separated field projection.
        timeout (int): HTTP request timeout in seconds.

    Returns:
        List[Dict[str, Any]]: Workspace summary objects from ``GET /api/workspaces``.
    """
    base_url = api_url.rstrip("/")
    url = f"{base_url}/api/workspaces"
    params = {"fields": fields} if fields else None

    logger.info("Listing accessible Terra workspaces...")
    response = requests.get(url, headers=headers, params=params, timeout=timeout)
    if response.status_code != 200:
        logger.error(
            f"Failed to list Terra workspaces (HTTP {response.status_code}): "
            f"{response.text}"
        )
        raise RuntimeError(
            f"Failed to list Terra workspaces (HTTP {response.status_code}): "
            f"{response.text}"
        )

    workspaces = response.json()
    logger.info(f"Found {len(workspaces)} accessible Terra workspaces.")
    return workspaces


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
    pool_maxsize: int = 64,
) -> storage.Client:
    """
    Create an authenticated Google Cloud Storage client configured with an
    expanded HTTP connection pool for parallel downloads.

    Args:
        credentials (Credentials): Google OAuth2 credentials.
        project (Optional[str]): Optional GCP project ID.
        pool_maxsize (int): Maximum pooled HTTPS connections per host.

    Returns:
        storage.Client: Authenticated GCS client.
    """
    client = storage.Client(credentials=credentials, project=project)
    http_session = getattr(client, "_http", None)
    if http_session is not None and hasattr(http_session, "mount"):
        adapter = HTTPAdapter(
            pool_connections=pool_maxsize,
            pool_maxsize=pool_maxsize,
        )
        http_session.mount("https://", adapter)
        http_session.mount("http://", adapter)
    return client


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


def iter_gcs_blobs(
    storage_client: storage.Client,
    bucket_name: str,
    prefix: str = "",
    user_project: Optional[str] = None,
) -> Iterator[Any]:
    """
    Stream all non-directory blobs in a GCS bucket matching an optional prefix.
    """
    bucket = storage_client.bucket(bucket_name, user_project=user_project)
    blobs = storage_client.list_blobs(bucket, prefix=prefix)
    for blob in blobs:
        if not blob.name.endswith("/"):
            yield blob


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
    result = list(
        iter_gcs_blobs(
            storage_client=storage_client,
            bucket_name=bucket_name,
            prefix=prefix,
            user_project=user_project,
        )
    )
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
    timeout: int = 7200,
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
    temp_path = local_path.with_name(
        f".{local_path.name}.{os.getpid()}.{threading.get_ident()}.part"
    )
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
