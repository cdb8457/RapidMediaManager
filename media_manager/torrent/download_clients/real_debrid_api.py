import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

class RealDebridAPI:
    """Standalone API Client for interacting with Real-Debrid REST API."""
    
    BASE_URL = "https://api.real-debrid.com/rest/1.0"

    def __init__(self, api_token: str):
        self.api_token = api_token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_token}"
        })

    def add_magnet(self, magnet_link: str) -> str:
        """Adds a magnet link to Real-Debrid and returns the torrent ID."""
        url = f"{self.BASE_URL}/torrents/addMagnet"
        response = self.session.post(url, data={"magnet": magnet_link})
        response.raise_for_status()
        return response.json().get("id", "")

    def get_torrents(self) -> list[dict[str, Any]]:
        """Gets the list of active/completed torrents in the Debrid queue."""
        url = f"{self.BASE_URL}/torrents"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_torrent_info(self, torrent_id: str) -> dict[str, Any]:
        """Gets info for a torrent, including caching status and file IDs."""
        url = f"{self.BASE_URL}/torrents/info/{torrent_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def select_files(self, torrent_id: str, file_ids: str = "all") -> None:
        """Selects which files in the torrent to download ('all' or comma-separated string)."""
        url = f"{self.BASE_URL}/torrents/selectFiles/{torrent_id}"
        response = self.session.post(url, data={"files": file_ids})
        response.raise_for_status()

    def unrestrict_link(self, link: str) -> dict[str, Any]:
        """Unrestricts a cached hoster link into a direct download/streaming URL."""
        url = f"{self.BASE_URL}/unrestrict/link"
        response = self.session.post(url, data={"link": link})
        response.raise_for_status()
        return response.json()

    def delete_torrent(self, torrent_id: str) -> None:
        """Deletes a torrent from the Debrid queue."""
        url = f"{self.BASE_URL}/torrents/delete/{torrent_id}"
        response = self.session.delete(url)
        if response.status_code != 204:
            response.raise_for_status()
