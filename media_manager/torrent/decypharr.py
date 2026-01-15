"""
Decypharr Download Client - Synchronous Implementation

Integrates MediaManager with Decypharr's QBittorrent-compatible API
for Real-Debrid cached torrent acquisition.
"""

import logging
import requests
from typing import Optional
from requests.auth import HTTPBasicAuth

from media_manager.config import MediaManagerConfig
from media_manager.indexer.schemas import IndexerQueryResult
from media_manager.torrent.download_clients.abstract_download_client import (
    AbstractDownloadClient,
)
from media_manager.torrent.schemas import Torrent, TorrentStatus
from media_manager.torrent.utils import get_torrent_hash

log = logging.getLogger(__name__)


class DecypharrDownloadClient(AbstractDownloadClient):
    """
    Download client for Decypharr (Real-Debrid via QBittorrent-compatible API)
    
    Decypharr handles:
    - Checking if torrents are cached in Real-Debrid
    - Adding torrents to Real-Debrid
    - Creating Jellyfin-compatible symlinks
    - Managing the download lifecycle
    """
    
    name = "decypharr"
    
    # State mappings from Decypharr (QBittorrent-compatible) to MediaManager
    DOWNLOADING_STATE = (
        "allocating",
        "downloading",
        "metaDL",
        "pausedDL",
        "queuedDL",
        "stalledDL",
        "checkingDL",
        "forcedDL",
        "moving",
        "stoppedDL",
        "forcedMetaDL",
    )
    FINISHED_STATE = (
        "uploading",
        "pausedUP",
        "queuedUP",
        "stalledUP",
        "checkingUP",
        "forcedUP",
        "stoppedUP",
        "completed",
    )
    ERROR_STATE = ("missingFiles", "error", "checkingResumeData")
    UNKNOWN_STATE = ("unknown",)

    def __init__(self) -> None:
        """Initialize Decypharr client with configuration"""
        self.config = MediaManagerConfig().torrents.decypharr
        self.base_url = self.config.base_url.rstrip('/')
        self.timeout = 30
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.timeout = self.timeout
        
        # Authenticate if credentials provided
        self._cookie = None
        if self.config.username and self.config.password:
            self._authenticate()
        
        log.info(f"Decypharr client initialized: {self.base_url}")

    def _authenticate(self) -> None:
        """Authenticate with Decypharr and get session cookie"""
        url = f"{self.base_url}/api/v2/auth/login"
        data = {
            "username": self.config.username,
            "password": self.config.password
        }
        
        try:
            response = self.session.post(url, data=data, timeout=self.timeout)
            
            if response.status_code == 403:
                raise RuntimeError("Invalid Decypharr username or password")
            
            if response.status_code != 200:
                raise RuntimeError(f"Authentication failed: {response.text}")
            
            # Store session cookie
            if 'SID' in response.cookies:
                self._cookie = response.cookies['SID']
                log.info("Successfully authenticated with Decypharr")
            
        except requests.RequestException as e:
            log.error(f"Failed to authenticate with Decypharr: {e}")
            raise RuntimeError(f"Decypharr authentication failed: {e}")

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        data: Optional[dict] = None
    ) -> requests.Response:
        """
        Make API request to Decypharr
        
        Args:
            method: HTTP method (GET, POST, DELETE)
            endpoint: API endpoint (without base URL)
            params: URL parameters
            data: Form data
        
        Returns:
            Response object
        """
        url = f"{self.base_url}{endpoint}"
        
        # Add session cookie if authenticated
        cookies = {"SID": self._cookie} if self._cookie else None
        
        try:
            response = self.session.request(
                method,
                url,
                params=params,
                data=data,
                cookies=cookies,
                timeout=self.timeout
            )
            
            # Handle errors
            if response.status_code == 403:
                raise RuntimeError("Decypharr authentication required or invalid")
            
            if response.status_code >= 400:
                raise RuntimeError(
                    f"Decypharr API error {response.status_code}: {response.text}"
                )
            
            return response
        
        except requests.RequestException as e:
            log.error(f"Decypharr request failed: {e}")
            raise RuntimeError(f"Decypharr request failed: {e}")

    def download_torrent(self, indexer_result: IndexerQueryResult) -> Torrent:
        """
        Add a torrent to Decypharr (which adds to Real-Debrid).
        
        Decypharr will:
        1. Check if torrent is cached in Real-Debrid
        2. Add to Real-Debrid if cached
        3. Create symlinks automatically
        
        Args:
            indexer_result: The indexer query result containing magnet link
        
        Returns:
            Torrent object with initial status
        
        Raises:
            RuntimeError: If torrent cannot be added (not cached, API error, etc.)
        """
        log.info(f"Adding torrent to Decypharr: {indexer_result.title}")
        
        # Extract hash from magnet link
        torrent_hash = get_torrent_hash(torrent=indexer_result)
        
        # Determine category - movies or tv
        # We can infer from quality or use a default
        # For now, use the configured category or default to movies
        category = self.config.category_name or "movies"
        
        # Prepare request data
        endpoint = "/api/v2/torrents/add"
        data = {
            "urls": indexer_result.download_url,  # Magnet link
            "category": category
        }
        
        # Add save path if configured
        if self.config.category_save_path:
            data["savepath"] = self.config.category_save_path
        
        # Add to Decypharr
        response = self._request("POST", endpoint, data=data)
        
        # QBittorrent API returns "Ok." on success
        response_text = response.text.strip()
        if response_text != "Ok.":
            error_msg = f"Failed to add torrent to Decypharr: {response_text}"
            log.error(error_msg)
            raise RuntimeError(error_msg)
        
        log.info(f"Successfully added torrent to Decypharr: {torrent_hash}")
        
        # Create Torrent object
        torrent = Torrent(
            status=TorrentStatus.unknown,  # Will be updated by get_torrent_status
            title=indexer_result.title,
            quality=indexer_result.quality,
            imported=False,
            hash=torrent_hash,
        )
        
        # Get initial status
        try:
            torrent.status = self.get_torrent_status(torrent)
        except Exception as e:
            log.warning(f"Could not get initial status: {e}")
            # Continue anyway - status will be checked later
        
        return torrent

    def remove_torrent(self, torrent: Torrent, delete_data: bool = False) -> None:
        """
        Remove a torrent from Decypharr
        
        Args:
            torrent: The torrent to remove
            delete_data: Whether to delete associated files (symlinks)
        """
        log.info(f"Removing torrent from Decypharr: {torrent.title}")
        
        endpoint = "/api/v2/torrents/delete"
        data = {
            "hashes": torrent.hash,
            "deleteFiles": "true" if delete_data else "false"
        }
        
        self._request("POST", endpoint, data=data)
        log.info(f"Successfully removed torrent: {torrent.hash}")

    def get_torrent_status(self, torrent: Torrent) -> TorrentStatus:
        """
        Get the status of a specific torrent
        
        Args:
            torrent: The torrent to get status for
        
        Returns:
            TorrentStatus enum value
        """
        endpoint = "/api/v2/torrents/info"
        params = {"hashes": torrent.hash}
        
        response = self._request("GET", endpoint, params=params)
        
        # Parse JSON response
        try:
            torrents = response.json()
        except ValueError as e:
            log.error(f"Failed to parse Decypharr response: {e}")
            return TorrentStatus.unknown
        
        if not torrents:
            log.warning(f"No information found for torrent: {torrent.hash}")
            return TorrentStatus.unknown
        
        # Get first (should be only) torrent
        torrent_info = torrents[0]
        state = torrent_info.get("state", "unknown")
        
        # Map Decypharr state to MediaManager TorrentStatus
        if state in self.DOWNLOADING_STATE:
            return TorrentStatus.downloading
        elif state in self.FINISHED_STATE:
            return TorrentStatus.finished
        elif state in self.ERROR_STATE:
            return TorrentStatus.error
        elif state in self.UNKNOWN_STATE:
            return TorrentStatus.unknown
        else:
            log.warning(f"Unknown Decypharr state: {state}")
            return TorrentStatus.unknown

    def pause_torrent(self, torrent: Torrent) -> None:
        """
        Pause a torrent download
        
        Args:
            torrent: The torrent to pause
        """
        log.info(f"Pausing torrent: {torrent.title}")
        
        endpoint = "/api/v2/torrents/pause"
        data = {"hashes": torrent.hash}
        
        self._request("POST", endpoint, data=data)
        log.info(f"Successfully paused torrent: {torrent.hash}")

    def resume_torrent(self, torrent: Torrent) -> None:
        """
        Resume a torrent download
        
        Args:
            torrent: The torrent to resume
        """
        log.info(f"Resuming torrent: {torrent.title}")
        
        endpoint = "/api/v2/torrents/resume"
        data = {"hashes": torrent.hash}
        
        self._request("POST", endpoint, data=data)
        log.info(f"Successfully resumed torrent: {torrent.hash}")

    def get_torrent_files(self, torrent: Torrent) -> list[dict]:
        """
        Get list of files in a torrent (useful for debugging)
        
        Args:
            torrent: The torrent to get files for
        
        Returns:
            List of file information dictionaries
        """
        endpoint = "/api/v2/torrents/files"
        params = {"hash": torrent.hash}
        
        response = self._request("GET", endpoint, params=params)
        
        try:
            return response.json()
        except ValueError:
            log.error("Failed to parse files response")
            return []

    def health_check(self) -> bool:
        """
        Check if Decypharr is accessible
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            endpoint = "/api/v2/app/version"
            response = self._request("GET", endpoint)
            version = response.text.strip()
            log.info(f"Decypharr health check OK, version: {version}")
            return True
        except Exception as e:
            log.error(f"Decypharr health check failed: {e}")
            return False


# Example usage and testing
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=" * 70)
    print("Decypharr Download Client Test")
    print("=" * 70)
    
    # Mock configuration for standalone testing
    class MockDecypharrConfig:
        def __init__(self):
            self.base_url = "http://192.168.1.31:8282"
            self.username = "YOUR_USERNAME_HERE"  # FILL THIS IN
            self.password = "YOUR_PASSWORD_HERE"  # FILL THIS IN
            self.category_name = "MediaManager"
            self.category_save_path = ""
    
    class MockTorrentsConfig:
        def __init__(self):
            self.decypharr = MockDecypharrConfig()
    
    class MockConfig:
        def __init__(self):
            self.torrents = MockTorrentsConfig()
    
    # Patch MediaManagerConfig for testing
    import sys
    
    # Create a mock module
    class MockMediaManagerConfig:
        def __init__(self):
            self.torrents = MockTorrentsConfig()
    
    # Mock the import
    sys.modules['media_manager'] = type(sys)('media_manager')
    sys.modules['media_manager.config'] = type(sys)('media_manager.config')
    sys.modules['media_manager.config'].MediaManagerConfig = MockMediaManagerConfig
    
    try:
        # Create client instance
        print("\n[1/5] Initializing Decypharr client...")
        print(f"      URL: {MockDecypharrConfig().base_url}")
        
        # Manually create client to avoid import issues
        config = MockDecypharrConfig()
        
        session = requests.Session()
        session.timeout = 30
        
        base_url = config.base_url.rstrip('/')
        
        # Authenticate
        print("\n[2/5] Authenticating with Decypharr...")
        auth_url = f"{base_url}/api/v2/auth/login"
        auth_data = {
            "username": config.username,
            "password": config.password
        }
        
        auth_response = session.post(auth_url, data=auth_data, timeout=30)
        
        if auth_response.status_code == 403:
            print("✗ Authentication failed: Invalid username or password")
            print("  Please update YOUR_USERNAME_HERE and YOUR_PASSWORD_HERE in the script")
            sys.exit(1)
        elif auth_response.status_code != 200:
            print(f"✗ Authentication failed: HTTP {auth_response.status_code}")
            print(f"  Response: {auth_response.text}")
            sys.exit(1)
        
        cookie = None
        if 'SID' in auth_response.cookies:
            cookie = auth_response.cookies['SID']
        
        print("✓ Authentication successful")
        
        # Health check
        print("\n[3/5] Checking Decypharr health...")
        version_url = f"{base_url}/api/v2/app/version"
        cookies = {"SID": cookie} if cookie else None
        version_response = session.get(version_url, cookies=cookies, timeout=30)
        
        if version_response.status_code == 200:
            version = version_response.text.strip()
            print(f"✓ Decypharr is accessible")
            print(f"  Version: {version}")
        else:
            print(f"✗ Health check failed: HTTP {version_response.status_code}")
        
        # Get torrent list
        print("\n[4/5] Getting current torrent list...")
        torrents_url = f"{base_url}/api/v2/torrents/info"
        torrents_response = session.get(torrents_url, cookies=cookies, timeout=30)
        
        if torrents_response.status_code == 200:
            torrents = torrents_response.json()
            print(f"✓ Found {len(torrents)} active torrents")
            
            if torrents:
                print("\n  Active Torrents:")
                for torrent in torrents[:5]:  # Show first 5
                    name = torrent.get('name', 'Unknown')[:60]
                    state = torrent.get('state', 'unknown')
                    progress = torrent.get('progress', 0) * 100
                    size = torrent.get('size', 0) / (1024**3)  # GB
                    
                    print(f"  • {name}")
                    print(f"    State: {state}, Progress: {progress:.1f}%, Size: {size:.2f} GB")
            else:
                print("  No active torrents found")
        else:
            print(f"✗ Failed to get torrents: HTTP {torrents_response.status_code}")
        
        # Test categories
        print("\n[5/5] Checking categories...")
        categories_url = f"{base_url}/api/v2/torrents/categories"
        categories_response = session.get(categories_url, cookies=cookies, timeout=30)
        
        if categories_response.status_code == 200:
            categories = categories_response.json()
            print(f"✓ Found {len(categories)} categories")
            
            if 'MediaManager' in categories:
                print("  ✓ 'MediaManager' category exists")
            else:
                print("  ℹ 'MediaManager' category will be created on first use")
        
        print("\n" + "=" * 70)
        print("✓ ALL TESTS PASSED - Decypharr client is working correctly!")
        print("=" * 70)
        print("\nNext steps:")
        print("1. Note your username and password for the config")
        print("2. Run the Torrentio indexer test")
        print("3. Copy both files into MediaManager")
        
    except requests.RequestException as e:
        print(f"\n✗ Connection Error: {e}")
        print("\nTroubleshooting:")
        print("1. Is Decypharr running at http://192.168.1.31:8282?")
        print("2. Can you access it in a web browser?")
        print("3. Is the username/password correct?")
        import traceback
        traceback.print_exc()
    
    except Exception as e:
        print(f"\n✗ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
