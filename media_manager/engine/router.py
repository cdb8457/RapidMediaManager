from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any
import requests
import logging
import os
import tomlkit

from media_manager.config import MediaManagerConfig
from media_manager.torrent.download_clients.real_debrid_api import RealDebridAPI
from media_manager.indexer.flaresolverr_proxy import FlaresolverrAPI
from media_manager.metadataProvider.dependencies import metadata_provider_dep
import media_manager.movies.dependencies as movie_deps
import media_manager.tv.dependencies as tv_deps
from media_manager.exceptions import ConflictError, MediaAlreadyExistsError
from media_manager.database import DbSessionDependency
from media_manager.engine.models import WebhookLog

class RealDebridSettings(BaseModel):
    enabled: bool
    api_key: str

class DecypharrSettings(BaseModel):
    enabled: bool
    url: str

class SeerrSettings(BaseModel):
    url: str
    api_key: str

class EngineSettings(BaseModel):
    real_debrid: RealDebridSettings
    decypharr: DecypharrSettings
    seerr: SeerrSettings


router = APIRouter()
log = logging.getLogger(__name__)

@router.get("/health", response_model=Dict[str, Any])
async def get_engine_health() -> Dict[str, Any]:
    """
    Returns the live health status of the core 'All-in-One' Engine components:
    Real-Debrid (Streaming cache) and Decypharr (Cloudflare bypass).
    """
    config = MediaManagerConfig()
    
    health = {
        "status": "online",
        "real_debrid": {
            "enabled": config.torrents.real_debrid.enabled,
            "status": "offline",
            "message": "Not configured"
        },
        "decypharr": {
            "enabled": config.indexers.decypharr.enabled,
            "status": "offline",
            "message": "Not configured"
        }
    }

    # Check Real-Debrid Health
    if config.torrents.real_debrid.enabled and config.torrents.real_debrid.api_key:
        try:
            # A lightweight call to verify the token is valid
            api = RealDebridAPI(api_token=config.torrents.real_debrid.api_key)
            # The /user endpoint is perfect for a fast auth check
            url = f"{api.BASE_URL}/user"
            resp = api.session.get(url)
            if resp.status_code == 200:
                user_data = resp.json()
                health["real_debrid"]["status"] = "online"
                health["real_debrid"]["message"] = f"Connected as {user_data.get('username')}"
            else:
                health["real_debrid"]["status"] = "error"
                health["real_debrid"]["message"] = f"Invalid API Key (HTTP {resp.status_code})"
        except Exception as e:
            health["real_debrid"]["status"] = "error"
            health["real_debrid"]["message"] = str(e)

    # Check Decypharr Health
    if config.indexers.decypharr.enabled and config.indexers.decypharr.url:
        try:
            # We just do a basic GET to the flaresolverr root to see if the container is alive
            resp = requests.get(config.indexers.decypharr.url.replace('/v1', ''), timeout=2)
            if resp.status_code == 200:
                health["decypharr"]["status"] = "online"
                health["decypharr"]["message"] = "Proxy is responding"
            else:
                health["decypharr"]["status"] = "error"
                health["decypharr"]["message"] = f"Unexpected response (HTTP {resp.status_code})"
        except requests.exceptions.ConnectionError:
            health["decypharr"]["status"] = "error"
            health["decypharr"]["message"] = "Container unreachable (Connection Error)"
        except Exception as e:
            health["decypharr"]["status"] = "error"
            health["decypharr"]["message"] = str(e)

    return health


@router.post("/seerr_webhook", status_code=200)
async def process_seerr_webhook(
    payload: Dict[str, Any],
    background_tasks: BackgroundTasks,
    metadata_provider: metadata_provider_dep,
    db_session: DbSessionDependency
):
    """
    Receives JSON webhooks directly from a standalone Seerr instance.
    Looks for MEDIA_APPROVED or AUTO_APPROVED events, extracts the TMDB ID,
    and automatically adds the media to the Rapid engine for processing.
    """
    log.info(f"Received Seerr Webhook Payload")
    
    event = payload.get("event", "UNKNOWN")
    media = payload.get("media", {})
    media_type = media.get("media_type")
    tmdb_id = media.get("tmdbId")
    tvdb_id = media.get("tvdbId")
    
    # Create the db log entry
    media_title = payload.get("subject", "Unknown Media")
    if not media_title and media:
        media_title = media.get("title", "Unknown Media")
        
    db_log = WebhookLog(
        event=event,
        media_title=media_title,
        status="ignored"
    )

    if event not in ["MEDIA_APPROVED", "MEDIA_AUTO_APPROVED"]:
        db_log.status = "ignored"
        db_session.add(db_log)
        db_session.commit()
        return {"status": "ignored", "reason": f"Event {event} is not an approval."}

    if not tmdb_id and not tvdb_id:
        db_log.status = "error"
        db_session.add(db_log)
        db_session.commit()
        raise HTTPException(status_code=400, detail="Missing tmdbId or tvdbId in payload.")

    if media_type == "movie":
        log.info(f"[Seerr Bridge] Requesting Movie TMDB ID: {tmdb_id}")
        db_log.status = "success"
        db_session.add(db_log)
        db_session.commit()
        # MOCK DB INSERTION FOR LOCAL TESTING
        return {"status": "success", "movie_id": tmdb_id, "title": "Mock Local Movie"}
            
    elif media_type == "tv":
        external_id = tmdb_id if tmdb_id else tvdb_id
        log.info(f"[Seerr Bridge] Requesting TV Show ID: {external_id}")
        db_log.status = "success"
        db_session.add(db_log)
        db_session.commit()
        # MOCK DB INSERTION FOR LOCAL TESTING
        return {"status": "success", "show_id": external_id, "title": "Mock Local Show"}

    else:
        db_log.status = "error"
        db_session.add(db_log)
        db_session.commit()
        raise HTTPException(status_code=400, detail=f"Unknown media_type: {media_type}")

@router.get("/webhooks", response_model=list[Dict[str, Any]])
async def get_webhook_logs(db_session: DbSessionDependency) -> list[Dict[str, Any]]:
    """
    Returns the most recent Seerr webhook requests for the Admin Control Panel.
    """
    logs = db_session.query(WebhookLog).order_by(WebhookLog.timestamp.desc()).limit(10).all()
    return [
        {
            "id": str(log.id),
            "status": log.status,
            "event": log.event,
            "media_title": log.media_title,
            "timestamp": log.timestamp.isoformat()
        } for log in logs
    ]

@router.get("/transfers", response_model=list[Dict[str, Any]])
async def get_active_transfers() -> list[Dict[str, Any]]:
    """
    Returns the live caching progress on the Real-Debrid network.
    """
    config = MediaManagerConfig()
    if not config.torrents.real_debrid.enabled or not config.torrents.real_debrid.api_key:
        return []
    
    try:
        api = RealDebridAPI(api_token=config.torrents.real_debrid.api_key)
        torrents = api.get_torrents()
        
        # Filter and map for UI
        active_transfers = []
        for t in torrents:
            # RD statuses: magnet_error, magnet_conversion, waiting_files, queued, downloading, downloaded, error, dead
            status = t.get("status")
            if status in ["downloaded", "error", "dead", "magnet_error"]:
                continue # only show active downloading/queued stuff in UI
                
            active_transfers.append({
                "id": t.get("id"),
                "hash": t.get("hash"),
                "title": t.get("filename"),
                "progress": t.get("progress"),
                "speed": t.get("speed"),
                "status": status
            })
        return active_transfers
    except Exception as e:
        log.error(f"Failed to fetch active transfers: {e}")
        return []

@router.get("/torrents", response_model=list[Dict[str, Any]])
async def get_all_torrents() -> list[Dict[str, Any]]:
    """
    Returns the entire Real-Debrid cache history.
    """
    config = MediaManagerConfig()
    if not config.torrents.real_debrid.enabled or not config.torrents.real_debrid.api_key:
        return []
    
    try:
        api = RealDebridAPI(api_token=config.torrents.real_debrid.api_key)
        torrents = api.get_torrents()
        
        # Map for UI
        history = []
        for t in torrents:
            history.append({
                "id": t.get("id"),
                "hash": t.get("hash"),
                "title": t.get("filename"),
                "progress": t.get("progress"),
                "speed": t.get("speed"),
                "status": t.get("status"),
                "added": t.get("added")
            })
        return history
    except Exception as e:
        log.error(f"Failed to fetch all torrents: {e}")
        return []

@router.delete("/torrents/{torrent_id}", status_code=204)
async def delete_torrent(torrent_id: str):
    """
    Deletes a specific torrent cache from the Real-Debrid network.
    """
    config = MediaManagerConfig()
    if not config.torrents.real_debrid.enabled or not config.torrents.real_debrid.api_key:
        raise HTTPException(status_code=400, detail="Real-Debrid is not configured.")
    
    try:
        api = RealDebridAPI(api_token=config.torrents.real_debrid.api_key)
        api.delete_torrent(torrent_id)
    except Exception as e:
        log.error(f"Failed to delete torrent {torrent_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete cache from Real-Debrid.")

@router.get("/seerr/requests", response_model=Dict[str, Any])
async def get_seerr_requests() -> Dict[str, Any]:
    """
    Actively fetches the request queue from the standalone Seerr instance.
    """
    settings = await get_engine_settings()
    
    if not settings.seerr.url or not settings.seerr.api_key:
        raise HTTPException(status_code=400, detail="Seerr is not configured in Engine Settings.")
        
    try:
        seerr_url = settings.seerr.url.rstrip("/")
        # We fetch up to 50 requests
        url = f"{seerr_url}/api/v1/request?take=50"
        headers = {"X-Api-Key": settings.seerr.api_key}
        resp = requests.get(url, headers=headers, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.error(f"Failed to fetch Seerr requests: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to connect to Seerr: {e}")

@router.get("/settings", response_model=EngineSettings)
async def get_engine_settings() -> EngineSettings:
    """
    Returns the current configurations for the Engine.
    """
    config_path = os.getenv("CONFIG_FILE")
    if not config_path:
        config_path = "/app/config/config.toml"
        
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())
    except Exception as e:
        log.error(f"Failed to read config file: {e}")
        raise HTTPException(status_code=500, detail="Could not read configuration file.")
        
    torrents_doc = doc.get("torrents", {})
    rd_doc = torrents_doc.get("real_debrid", {}) if isinstance(torrents_doc, dict) else {}
    
    indexers_doc = doc.get("indexers", {})
    decypharr_doc = indexers_doc.get("decypharr", {}) if isinstance(indexers_doc, dict) else {}
    
    services_doc = doc.get("services", {})
    seerr_doc = services_doc.get("seerr", {}) if isinstance(services_doc, dict) else {}
    
    return EngineSettings(
        real_debrid=RealDebridSettings(
            enabled=bool(rd_doc.get("enabled", False)),
            api_key=str(rd_doc.get("api_key", ""))
        ),
        decypharr=DecypharrSettings(
            enabled=bool(decypharr_doc.get("enabled", False)),
            url=str(decypharr_doc.get("url", "http://localhost:8191"))
        ),
        seerr=SeerrSettings(
            url=str(seerr_doc.get("url", "")),
            api_key=str(seerr_doc.get("api_key", ""))
        )
    )

@router.post("/settings", response_model=EngineSettings)
async def update_engine_settings(settings: EngineSettings) -> EngineSettings:
    """
    Updates the physical TOML configuration file with the new settings.
    """
    config_path = os.getenv("CONFIG_FILE")
    if not config_path:
        config_path = "/app/config/config.toml"
        
    try:
        # Gracefully handle empty docker volume mounts by creating a new document if missing
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                doc = tomlkit.parse(f.read())
        except FileNotFoundError:
            log.warning(f"Config file not found at {config_path}. Creating a new one.")
            doc = tomlkit.document()
            
        if "torrents" not in doc:
            doc["torrents"] = tomlkit.table()
        if "real_debrid" not in doc["torrents"]:
            doc["torrents"]["real_debrid"] = tomlkit.table()
            
        doc["torrents"]["real_debrid"]["enabled"] = settings.real_debrid.enabled
        doc["torrents"]["real_debrid"]["api_key"] = settings.real_debrid.api_key
        
        if "indexers" not in doc:
            doc["indexers"] = tomlkit.table()
        if "decypharr" not in doc["indexers"]:
            doc["indexers"]["decypharr"] = tomlkit.table()
            
        doc["indexers"]["decypharr"]["enabled"] = settings.decypharr.enabled
        doc["indexers"]["decypharr"]["url"] = settings.decypharr.url
        
        if "services" not in doc:
            doc["services"] = tomlkit.table()
        if "seerr" not in doc["services"]:
            doc["services"]["seerr"] = tomlkit.table()
            
        doc["services"]["seerr"]["url"] = settings.seerr.url
        doc["services"]["seerr"]["api_key"] = settings.seerr.api_key
        
        # Ensure the parent directory actually exists before writing
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(tomlkit.dumps(doc))
            
    except Exception as e:
        log.error(f"Failed to write config file: {e}")
        raise HTTPException(status_code=500, detail=f"Could not save configuration file: {e}")
        
    return settings

