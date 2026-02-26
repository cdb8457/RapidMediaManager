import logging
import uuid
from typing import Any

from fastapi import APIRouter, Form, Request, Response, status

from media_manager.config import MediaManagerConfig
from media_manager.torrent.download_clients.real_debrid_api import RealDebridAPI

log = logging.getLogger(__name__)

router = APIRouter()

def get_rd_client() -> RealDebridAPI:
    config = MediaManagerConfig()
    return RealDebridAPI(api_token=config.torrents.real_debrid.api_key)

@router.post("/auth/login")
async def login() -> Response:
    """Spoof qBittorrent login."""
    response = Response(content="Ok.", status_code=status.HTTP_200_OK)
    response.set_cookie(key="SID", value=str(uuid.uuid4()))
    return response

@router.get("/app/webapiVersion")
async def webapi_version() -> Response:
    """Spoof qBittorrent WebAPI Version."""
    return Response(content="2.8.19", media_type="text/plain")

@router.post("/torrents/add")
async def add_torrent(urls: str = Form(...)) -> Response:
    """
    Intercept magnet links from Sonarr/Radarr and send them directly to Real-Debrid.
    """
    magnet_link = urls.strip()
    log.info(f"Intercepted magnet link destined for qBittorrent: {magnet_link[:60]}...")
    
    rd_client = get_rd_client()
    try:
        # Add magnet to Real-Debrid
        torrent_id = rd_client.add_magnet(magnet_link)
        # Auto-select all files to start the caching process
        rd_client.select_files(torrent_id=torrent_id, file_ids="all")
        return Response(content="Ok.", media_type="text/plain")
    except Exception as e:
        log.error(f"Failed to proxy torrent to Real-Debrid: {e}")
        return Response(content="Fails.", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@router.get("/torrents/info")
async def get_torrents_info() -> list[dict[str, Any]]:
    """
    Fetch Real-Debrid active transfers and map them to qBittorrent state variables.
    This tricks Radarr/Sonarr into thinking the torrent has successfully completed.
    """
    rd_client = get_rd_client()
    try:
        rd_torrents = rd_client.get_torrents()
    except Exception as e:
        log.error(f"Failed to fetch Real-Debrid torrents for qBittorrent proxy: {e}")
        return []

    qb_torrents = []
    
    for t in rd_torrents:
        # Map Real-Debrid status to qBittorrent states
        # qB states: error, missingFiles, uploading, pausedUP, queuedUP, stalledUP, checkingUP, forcedUP, allocating, downloading, metaDL, pausedDL, queuedDL, stalledDL, checkingDL, forcedDL, checkingResumeData, moving
        rd_status = t.get("status")
        
        # When RD is "downloaded", we will set state to "uploading" (completed)
        if rd_status == "downloaded":
            qb_state = "uploading"
            progress = 1.0
        elif rd_status == "downloading":
            qb_state = "downloading"
            progress = t.get("progress", 0) / 100.0
        elif rd_status == "magnet_conversion":
            qb_state = "metaDL"
            progress = 0.0
        elif rd_status == "magnet_error" or rd_status == "error":
            qb_state = "error"
            progress = 0.0
        else:
            qb_state = "downloading"
            progress = t.get("progress", 0) / 100.0

        qb_torrents.append({
            "hash": t.get("hash", ""),
            "name": t.get("filename", "Unknown"),
            "size": t.get("bytes", 0),
            "progress": progress,
            "dlspeed": t.get("speed", 0),
            "upspeed": 0,
            "priority": 0,
            "num_seeds": t.get("seeders", 0),
            "num_leechs": 0,
            "num_incomplete": 0,
            "ratio": 1.0,
            "eta": 0,
            "state": qb_state,
            "seq_dl": False,
            "f_l_piece_prio": False,
            "category": "",
            "super_seeding": False,
            "force_start": False,
            # Spoof the save path to be a standard downloads folder so Sonarr tries to import from it
            "save_path": "/data/torrents/mediamanager/"
        })
        
    return qb_torrents

@router.post("/torrents/delete")
async def delete_torrents(hashes: str = Form(...), deleteFiles: str = Form(None)) -> Response:
    """
    Delete torrents from Real-Debrid using their hash
    """
    rd_client = get_rd_client()
    hash_list = hashes.split("|")
    try:
        rd_torrents = rd_client.get_torrents()
        for h in hash_list:
            # Find the Real-Debrid ID matching this hash
            torrent = next((t for t in rd_torrents if t.get("hash", "").lower() == h.lower()), None)
            if torrent:
                rd_client.delete_torrent(torrent.get("id"))
                
        return Response(content="Ok.", media_type="text/plain")
    except Exception as e:
        log.error(f"Failed to delete Real-Debrid torrents for qBittorrent proxy: {e}")
        return Response(content="Fails.", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
