import email.utils
from datetime import datetime, timezone
from xml.etree.ElementTree import Element, SubElement, tostring

from fastapi import APIRouter, Request, Response, Depends

from media_manager.indexer.dependencies import indexer_service_dep
from media_manager.indexer.schemas import IndexerQueryResult

router = APIRouter()

CAPABILITIES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<caps>
    <server version="1.0" title="RapidEngine Proxy" />
    <searching>
        <search available="yes" supportedParams="q" />
        <tv-search available="yes" supportedParams="q,season,ep" />
        <movie-search available="yes" supportedParams="q" />
    </searching>
    <categories>
        <category id="2000" name="Movies" />
        <category id="2030" name="Movies/HD" />
        <category id="2040" name="Movies/3D" />
        <category id="2045" name="Movies/UHD" />
        <category id="5000" name="TV" />
        <category id="5030" name="TV/SD" />
        <category id="5040" name="TV/HD" />
        <category id="5045" name="TV/UHD" />
    </categories>
</caps>
"""

@router.get("/api")
async def torznab_api(
    request: Request,
    indexer_service: indexer_service_dep,
    t: str = "search",
    q: str = "",
    apikey: str = "",
):
    """
    Mock Torznab Proxy API meant for Sonarr/Radarr.
    Intercepts standard Torznab queries, passes them to Rapid Engine's multi-indexer service
    (which handles Decypharr proxies locally), and reformats the results into the Torznab XML standard.
    """
    if t == "caps":
        return Response(content=CAPABILITIES_XML, media_type="application/xml")

    # For any search (movie, tvsearch, search)
    results: list[IndexerQueryResult] = indexer_service.search(query=q, is_tv=(t == "tvsearch" or t == "tv-search"))

    # Generate XML feed
    rss = Element("rss", {"version": "2.0", "xmlns:torznab": "http://torznab.com/schemas/2015/feed"})
    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text = "RapidEngine Proxy"
    SubElement(channel, "description").text = "RapidEngine proxy for configured indexers"
    
    for r in results:
        item = SubElement(channel, "item")
        SubElement(item, "title").text = r.title
        SubElement(item, "guid").text = str(r.id)
        # We map the download string since Radarr will try to 'grab' it
        SubElement(item, "link").text = r.download_url
        SubElement(item, "pubDate").text = email.utils.format_datetime(datetime.now(timezone.utc))
        SubElement(item, "description").text = r.title
        
        enclosure = SubElement(item, "enclosure", url=r.download_url, length=str(r.size), type="application/x-bittorrent")
        
        # Torznab attributes necessary for Arr parsing
        SubElement(item, "torznab:attr", name="seeders", value=str(r.seeders))
        SubElement(item, "torznab:attr", name="peers", value=str(r.seeders))
        SubElement(item, "torznab:attr", name="size", value=str(r.size))
        
        category_id = "5000" if (t == "tvsearch" or t == "tv-search") else "2000"
        SubElement(item, "torznab:attr", name="category", value=category_id)

    xml_str = tostring(rss, encoding="utf-8").decode("utf-8")
    return Response(content='<?xml version="1.0" encoding="utf-8"?>\n' + xml_str, media_type="application/xml")
