"""
Module for authenticating and interacting with SharePoint via Microsoft Graph API.
"""

import logging
from typing import Dict, List, Optional

import msal
import requests

logger = logging.getLogger(__name__)


def get_auth_headers(
    client_id: str, tenant_id: str, client_secret: Optional[str] = None
) -> Dict:
    """
    Authenticate using either client credentials or device flow.
    Returns headers with access token.

    Args:
        client_id (str): The client ID of the Azure AD application.
        tenant_id (str): The tenant ID of the Azure AD application.
        client_secret (Optional[str]): The client secret for client credentials flow.

    Returns:
        Dict: Headers containing the access token for authorization.
    Raises:
        RuntimeError: If authentication fails.
    """
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes_client = ["https://graph.microsoft.com/.default"]
    scopes_device = ["Files.Read.All", "Sites.Read.All", "User.Read"]

    def _client_credentials_flow() -> Optional[Dict]:
        logger.info("Attempting authentication using client credentials...")
        app = msal.ConfidentialClientApplication(
            client_id, authority=authority, client_credential=client_secret
        )
        result = app.acquire_token_for_client(scopes=scopes_client)
        if not result or "access_token" not in result:
            logger.warning("Client credentials authentication failed.")
            logger.warning("Error: %s", result["error"] if result else "No result")
            logger.warning(
                "Error description: %s",
                result["error_description"] if result else "No result",
            )
            logger.warning(
                "Correlation ID: %s",
                result["correlation_id"] if result else "No result",
            )
            return None
        logger.info("Access token acquired via client credentials.")
        headers = {"Authorization": f"Bearer {result['access_token']}"}
        return headers

    def _device_flow() -> Dict:
        logger.info("Logging in using device flow...")
        app = msal.PublicClientApplication(client_id, authority=authority)
        flow = app.initiate_device_flow(scopes=scopes_device)
        if "user_code" not in flow:
            logger.error(f"Failed to start device flow: {flow}")
            raise RuntimeError(f"Failed to start device flow: {flow}")
        logger.info(
            f"Please go to {flow['verification_uri']} "
            f"and enter code: {flow['user_code']}"
        )
        result = app.acquire_token_by_device_flow(flow)
        if "access_token" not in result:
            logger.error("Authentication failed: %s", result.get("error_description"))
            raise RuntimeError(
                f"Authentication failed: {result.get('error_description')}"
            )
        logger.info("Access token acquired via device flow.")
        headers = {"Authorization": f"Bearer {result['access_token']}"}
        return headers

    # Try client credentials flow if client_secret is provided
    if client_secret:
        try:
            headers = _client_credentials_flow()
            if headers:
                return headers
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f"Client credentials error: {e}. Falling back to device flow.")

    # Fallback to device flow
    return _device_flow()


def get_site_id(
    headers: Dict,
    site_path: str,
    timeout: int = 120,
) -> str:
    """
    Retrieve the SharePoint site ID for a given site path.

    Args:
        headers (Dict): Authorization headers with access token.
        site_path (str): SharePoint site path (e.g., "contoso.sharepoint.com:/sites/ProCAN").
        timeout (int): Request timeout in seconds.

    Returns:
        str: The SharePoint site ID.

    Raises:
        RuntimeError: If the site cannot be retrieved.
    """
    site_name = site_path.split(":")[-1]
    logger.info(f"Looking up site ID for /sites/{site_name}...")
    site_url = f"https://graph.microsoft.com/v1.0/sites/{site_path}"

    resp = requests.get(site_url, headers=headers, timeout=timeout)
    logger.debug(f"resp: {resp}")
    if resp.status_code != 200:
        logger.error(f"Failed to get SharePoint site: {resp.text}")
        raise RuntimeError(f"Failed to get SharePoint site: {resp.text}")

    site_id = resp.json().get("id")
    logger.info(f"Site ID retrieved: {site_id}")
    return site_id


def get_drives(site_id: str, headers: Dict, timeout: int = 120) -> List[Dict]:
    """
    List document libraries (drives) in the specified SharePoint site.

    Args:
        site_id (str): The SharePoint site ID.
        headers (Dict): Headers containing the access token.
        timeout (int): Request timeout in seconds.

    Returns:
        List[Dict]: A list of document libraries (drives) in the site.

    Raises:
        RuntimeError: If the drives cannot be retrieved.
    """
    logger.info(f"Listing document libraries in site: {site_id}...")
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    response = requests.get(url, headers=headers, timeout=timeout)

    if response.status_code != 200:
        logger.error(f"Failed to get drives: {response.text}")
        raise RuntimeError(f"Failed to get drives: {response.text}")

    drives = response.json().get("value", [])
    drive_names = [drive.get("name", "Unnamed") for drive in drives]
    logger.info(f"Document libraries found: {drive_names}")

    return drives


def list_drive_root(drive_id: str, headers: Dict, timeout: int = 120) -> List[Dict]:
    """
    List items in the root folder of a SharePoint drive.

    Args:
        drive_id (str): The SharePoint drive ID.
        headers (Dict): Authorization headers with access token.
        timeout (int): Request timeout in seconds.

    Returns:
        List[Dict]: Items in the root folder.

    Raises:
        RuntimeError: If the request fails.
    """
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
    response = requests.get(url, headers=headers, timeout=timeout)

    if response.status_code != 200:
        logger.error(f"Failed to list items in drive root: {response.text}")
        raise RuntimeError(f"Failed to list items in drive root: {response.text}")

    items = response.json().get("value", [])
    logger.info(f"Found {len(items)} items in drive root.")
    return items


def list_folder_items(
    drive_id: str,
    folder_id: str,
    headers: Dict[str, str],
    timeout: int = 120,
) -> List[Dict]:
    """
    List items in a specific folder within a SharePoint drive.

    Args:
        drive_id (str): The SharePoint drive ID.
        folder_id (str): The folder ID within the drive.
        headers (Dict[str, str]): Authorization headers with access token.
        timeout (int): Request timeout in seconds.

    Returns:
        List[Dict]: Items in the specified folder.

    Raises:
        RuntimeError: If the request fails.
    """
    url = (
        f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children"
    )
    response = requests.get(url, headers=headers, timeout=timeout)

    if response.status_code != 200:
        logger.error(f"Failed to list items in folder: {response.text}")
        raise RuntimeError(f"Failed to list items in folder: {response.text}")

    items = response.json().get("value", [])
    logger.info(f"Found {len(items)} items in folder {folder_id}.")
    return items
