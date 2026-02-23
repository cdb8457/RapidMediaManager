import logging

from media_manager.config import MediaManagerConfig
from media_manager.indexer.schemas import IndexerQueryResult
from media_manager.torrent.download_clients.abstract_download_client import (
    AbstractDownloadClient,
)
from media_manager.torrent.schemas import Torrent, TorrentStatus
from media_manager.torrent.download_clients.real_debrid_api import RealDebridAPI

log = logging.getLogger(__name__)

class RealDebridDownloadClient(AbstractDownloadClient):
    """
    Adapter that integrates the RealDebridAPI into the MediaManager DownloadManager.
    Treats caching a torrent on Real-Debrid like a standard torrent download.
    """

    def __init__(self) -> None:
        self._name = "real_debrid"
        
        # NOTE: We temporarily fetch from environment until config.py is updated fully
        # In a later step, this will point directly to MediaManagerConfig().torrents.real_debrid.api_key
        api_key = MediaManagerConfig().torrents.real_debrid.api_key
        if not api_key:
             raise ValueError("Real-Debrid API key is not configured.")
             
        self.api = RealDebridAPI(api_token=api_key)

    @property
    def name(self) -> str:
        return self._name

    def download_torrent(self, indexer_result: IndexerQueryResult) -> Torrent:
        log.info(f"Sending magnet to Real-Debrid: {indexer_result.title}")
        
        # 1. Send the magnet link to RD
        torrent_id = self.api.add_magnet(indexer_result.magnet_uri)
        
        # 2. Tell RD to select files to initiate the cache checking/downloading
        self.api.select_files(torrent_id, "all")
        
        # 3. Construct the internal Torrent schema
        return Torrent(
            title=indexer_result.title,
            size=indexer_result.size,
            hash=indexer_result.info_hash,
            id=torrent_id,  # We store the RD ID as the internal torrent ID
            progress=0.0,
            status=TorrentStatus.DOWNLOADING,
            download_client=self.name,
            eta=0,
            availability=0.0,
            category="debrid",
        )

    def remove_torrent(self, torrent: Torrent, delete_data: bool = False) -> None:
        log.info(f"Removing torrent from Real-Debrid: {torrent.title}")
        self.api.delete_torrent(torrent.id)

    def get_torrent_status(self, torrent: Torrent) -> TorrentStatus:
        info = self.api.get_torrent_info(torrent.id)
        rd_status = info.get("status")
        
        if rd_status == "downloaded":
            # Torrent is fully cached and ready to unrestrict
            return TorrentStatus.COMPLETED
        elif rd_status in ["downloading", "queued", "magnet_conversion"]:
            return TorrentStatus.DOWNLOADING
        elif rd_status == "waiting_files_selection":
             # Should not happen since we auto-select on add, but handle it securely
             self.api.select_files(torrent.id, "all")
             return TorrentStatus.DOWNLOADING
        elif rd_status == "error":
             return TorrentStatus.ERROR
        elif rd_status == "virus":
             return TorrentStatus.ERROR
        else:
            return TorrentStatus.UNKNOWN

    def pause_torrent(self, torrent: Torrent) -> None:
        # Real-Debrid clouds do not support pausing
        log.warning(f"Pause requested for Debrid link {torrent.title}, but pausing is unsupported.")
        pass

    def resume_torrent(self, torrent: Torrent) -> None:
         # Real-Debrid clouds do not support pausing
        pass
