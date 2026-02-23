import logging
from typing import Any, Dict

import requests

log = logging.getLogger(__name__)

class FlaresolverrAPI:
    """
    Standalone API Client for interacting with Flaresolverr (Decypharr).
    Routes HTTP requests through a headless browser proxy to bypass Cloudflare.
    """
    
    def __init__(self, host_url: str = "http://localhost:8191"):
        """
        Initialize the Flaresolverr proxy client.
        :param host_url: The URL to the Flaresolverr instance (e.g., http://localhost:8191)
        """
        self.host_url = f"{host_url.rstrip('/')}/v1"
        self.session = requests.Session()

    def fetch(self, target_url: str, method: str = "GET", max_timeout: int = 60000) -> Dict[str, Any]:
        """
        Fetches an external URL using the Flaresolverr proxy.
        
        :param target_url: The Cloudflare-protected URL to scrape
        :param method: HTTP method (GET or POST)
        :param max_timeout: Max time in milliseconds for Flaresolverr to attempt solving
        :return: A dictionary containing the raw HTML/XML content under the 'solution' key
        """
        
        payload = {
            "cmd": f"request.{method.lower()}",
            "url": target_url,
            "maxTimeout": max_timeout
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        response = self.session.post(self.host_url, json=payload, headers=headers)
        response.raise_for_status()
        
        return response.json()
