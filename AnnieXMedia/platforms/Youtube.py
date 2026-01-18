# Authored By Certified Coders © 2025
import asyncio
import aiohttp
import contextlib
import json
import os
import re
import time
from typing import Dict, List, Optional, Tuple, Union, Any

import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from youtubesearchpython.aio import VideosSearch, Playlist

from AnnieXMedia.utils.cookie_handler import COOKIE_PATH
from AnnieXMedia.utils.downloader import media_download
from AnnieXMedia.utils.errors import capture_internal_err
from AnnieXMedia.utils.formatters import time_to_seconds
from AnnieXMedia.utils.tuning import YTDLP_TIMEOUT

# === Constants ===
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")

# === Helpers ===
def _cookiefile_path() -> Optional[str]:
    """Return COOKIE_PATH only if it exists and appears valid."""
    path = str(COOKIE_PATH)
    try:
        if not path or not os.path.exists(path) or os.path.getsize(path) <= 0:
            return None
        with open(path, "rb") as f:
            header = f.read(256)
        if b"Netscape HTTP Cookie File" in header:
            return path
        return None
    except Exception:
        return None

def _cookies_args() -> List[str]:
    path = _cookiefile_path()
    return ["--cookies", path] if path else []

async def _exec_proc(*args: str) -> Tuple[bytes, bytes]:
    cmd_args = list(args)
    if "yt-dlp" in cmd_args[0]:
        cmd_args.insert(1, "--no-cache-dir")

    proc = await asyncio.create_subprocess_exec(
        *cmd_args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        return await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
    except asyncio.TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
        return b"", b"timeout"

def _dig(data: Any, *path: Union[str, int]) -> Any:
    cur = data
    for p in path:
        if isinstance(p, str):
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                return None
        elif isinstance(p, int):
            if isinstance(cur, list) and 0 <= p < len(cur):
                cur = cur[p]
            else:
                return None
        else:
            return None
    return cur

# === Main Class ===
class YouTubeAPI:
    def __init__(self) -> None:
        self.base_url = "https://www.youtube.com/watch?v="
        self.playlist_url = "https://youtube.com/playlist?list="
        self._url_pattern = re.compile(r"(?:youtube\.com|youtu\.be)")

    def _prepare_link(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        if isinstance(videoid, str) and videoid.strip():
            return self.base_url + videoid.strip()

        link = link.strip()
        if "youtu.be" in link:
            return self.base_url + link.split("/")[-1].split("?")[0]
        elif "youtube.com/shorts/" in link or "youtube.com/live/" in link:
            return self.base_url + link.split("/")[-1].split("?")[0]
        
        return link.split("&")[0]

    def _extract_id_from_url(self, url: str) -> Optional[str]:
        """Attempts to extract a valid 11-char YouTube ID from a URL."""
        if "v=" in url:
            vid = url.split("v=")[1].split("&")[0]
            if len(vid) == 11:
                return vid
        elif "youtu.be/" in url:
            vid = url.split("youtu.be/")[1].split("?")[0]
            if len(vid) == 11:
                return vid
        elif "shorts/" in url:
            vid = url.split("shorts/")[1].split("?")[0]
            if len(vid) == 11:
                return vid
        return None

    # === URL Handling ===
    @capture_internal_err
    async def exists(self, link: str, videoid: Union[str, bool, None] = None) -> bool:
        return bool(self._url_pattern.search(self._prepare_link(link, videoid)))

    @capture_internal_err
    async def url(self, message: Message) -> Optional[str]:
        msgs = [message] + ([message.reply_to_message] if message.reply_to_message else [])
        for msg in msgs:
            text = msg.text or msg.caption or ""
            entities = (msg.entities or []) + (msg.caption_entities or [])
            for ent in entities:
                if ent.type == MessageEntityType.URL:
                    return text[ent.offset: ent.offset + ent.length].split("&si")[0]
                if ent.type == MessageEntityType.TEXT_LINK:
                    return ent.url.split("&si")[0]
        return None

    # === Metadata Fetching Methods ===
    async def _raw_youtube_search(self, query: str) -> Optional[Dict]:
        """Manual implementation of InnerTube search (Fast)."""
        endpoint = "https://www.youtube.com/youtubei/v1/search?key=AIzaSyCDCG4LCrByczUR8oYZKj-43dW-JqVIPHk"
        payload = {
            "context": {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": "2.20250101.01.00", 
                    "hl": "en",
                    "gl": "IN",
                },
            },
            "query": query,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json=payload) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
            
            # Extract Data
            root = _dig(
                data,
                "contents",
                "twoColumnSearchResultsRenderer",
                "primaryContents",
                "sectionListRenderer",
                "contents",
            )
            
            # Recursive parse helper
            def parse(node):
                if isinstance(node, list):
                    for item in node:
                        res = parse(item)
                        if res: return res
                elif isinstance(node, dict):
                    vr = _dig(node, "videoRenderer")
                    if vr:
                        vid_id = vr.get("videoId")
                        title = _dig(vr, "title", "runs", 0, "text")
                        duration = _dig(vr, "lengthText", "simpleText")
                        thumb = _dig(vr, "thumbnail", "thumbnails", 0, "url")
                        if vid_id and title:
                            return {
                                "id": vid_id,
                                "title": title,
                                "duration": duration or "00:00",
                                "thumbnails": [{"url": thumb}],
                                "thumbnail": thumb,
                                "link": f"https://www.youtube.com/watch?v={vid_id}"
                            }
                    for val in node.values():
                        res = parse(val)
                        if res: return res
                return None

            return parse(root)
        except Exception:
            return None

    @capture_internal_err
    async def _fetch_video_info(self, query: str) -> Optional[Dict]:
        """
        Attempts to fetch info for EVERY URL using multiple methods.
        Returns None only if all methods fail.
        """
        q = self._prepare_link(query)
        vid_id = self._extract_id_from_url(q)
        search_term = vid_id if vid_id else q

        # METHOD 1: Raw InnerTube Search (Fastest)
        raw = await self._raw_youtube_search(search_term)
        if raw:
            return raw

        # METHOD 2: yt-dlp (Robust)
        try:
            search_query = q if q.startswith("http") else f"ytsearch1:{q}"
            stdout, _ = await _exec_proc(
                "yt-dlp",
                *(_cookies_args()),
                "--dump-json",
                "--no-warnings",
                "--ignore-errors",
                search_query
            )
            if stdout:
                info = json.loads(stdout.decode())
                if "entries" in info:
                    info = info["entries"][0] if info["entries"] else {}
                if info:
                    return {
                        "id": info.get("id"),
                        "title": info.get("title"),
                        "duration": info.get("duration_string") or str(info.get("duration", 0)),
                        "thumbnails": [{"url": info.get("thumbnail", "")}],
                        "thumbnail": info.get("thumbnail", ""),
                        "link": info.get("webpage_url", q)
                    }
        except Exception:
            pass

        # METHOD 3: youtubesearchpython (Library Fallback)
        try:
            data = await VideosSearch(search_term, limit=1).next()
            res = data.get("result", [])
            if res:
                r = res[0]
                return {
                    "id": r.get("id"),
                    "title": r.get("title"),
                    "duration": r.get("duration"),
                    "thumbnails": r.get("thumbnails", []),
                    "thumbnail": r.get("thumbnails", [{}])[0].get("url", ""),
                    "link": r.get("link")
                }
        except Exception:
            pass

        return None

    def _get_fallback_info(self, link: str, videoid: Union[str, bool, None] = None) -> Dict:
        """Constructs a dummy 'Unknown' object if an ID is available."""
        final_link = self._prepare_link(link, videoid)
        vid_id = self._extract_id_from_url(final_link)
        
        # Determine ID: Explicit arg > Extracted > None
        final_id = None
        if isinstance(videoid, str) and len(videoid) == 11:
            final_id = videoid
        elif vid_id:
            final_id = vid_id

        if final_id:
            return {
                "id": final_id,
                "title": "Unknown",
                "duration": "00:00",
                "thumb": "https://telegra.ph/file/18804533035c91b5c468e.jpg", # Optional default thumb
                "link": f"https://www.youtube.com/watch?v={final_id}",
                "duration_min": "00:00"
            }
        
        raise ValueError(f"Could not fetch info for '{link}' and no Track ID found.")

    # === Public Data Methods ===

    @capture_internal_err
    async def track(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[Dict, str]:
        # 1. Try to fetch real info
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        
        # 2. If fetch failed, try to construct Unknown fallback
        if not info:
            try:
                fallback = self._get_fallback_info(link, videoid)
                # Map fallback to track format
                return {
                    "title": fallback["title"],
                    "link": fallback["link"],
                    "vidid": fallback["id"],
                    "duration_min": fallback["duration_min"],
                    "thumb": fallback["thumb"],
                }, fallback["id"]
            except ValueError as e:
                raise e

        # 3. Process success info
        thumbs = info.get("thumbnails", [{}])
        thumb_url = info.get("thumbnail") or (thumbs[-1].get("url") if thumbs else "") or ""
        thumb = thumb_url.split("?")[0]

        details = {
            "title": info.get("title") or "Unknown",
            "link": info.get("link") or self._prepare_link(link, videoid),
            "vidid": info.get("id", ""),
            "duration_min": info.get("duration") or "00:00",
            "thumb": thumb,
        }
        return details, info.get("id", "")

    @capture_internal_err
    async def details(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], int, str, str]:
        
        info = await self._fetch_video_info(self._prepare_link(link, videoid))

        if not info:
            try:
                fallback = self._get_fallback_info(link, videoid)
                return fallback["title"], fallback["duration_min"], 0, fallback["thumb"], fallback["id"]
            except ValueError as e:
                raise e

        title = info.get("title") or "Unknown"
        dt = info.get("duration") or "00:00"
        try:
            ds = int(time_to_seconds(dt)) if dt else 0
        except:
            ds = 0
        
        thumbs = info.get("thumbnails", [{}])
        thumb_url = info.get("thumbnail") or (thumbs[-1].get("url") if thumbs else "") or ""
        thumb = thumb_url.split("?")[0]

        return title, dt, ds, thumb, info.get("id", "")

    @capture_internal_err
    async def title(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        if not info:
            try:
                return self._get_fallback_info(link, videoid)["title"]
            except:
                return ""
        return info.get("title", "")

    @capture_internal_err
    async def duration(self, link: str, videoid: Union[str, bool, None] = None) -> Optional[str]:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        if not info:
            try:
                return self._get_fallback_info(link, videoid)["duration_min"]
            except:
                return None
        return info.get("duration")

    @capture_internal_err
    async def thumbnail(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        if not info:
            try:
                return self._get_fallback_info(link, videoid)["thumb"]
            except:
                return ""
        return (info.get("thumbnail") or info.get("thumbnails", [{}])[-1].get("url", "")).split("?")[0]

    # === Media & Formats ===
    @capture_internal_err
    async def video(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[int, str]:
        link = self._prepare_link(link, videoid)
        stdout, stderr = await _exec_proc(
            "yt-dlp",
            *(_cookies_args()),
            "-g",
            "-f",
            "best[height<=?720][width<=?1280]",
            link,
        )
        return (1, stdout.decode().split("\n")[0]) if stdout else (0, stderr.decode())

    @capture_internal_err
    async def playlist(
        self, link: str, limit: int, user_id, videoid: Union[str, bool, None] = None
    ) -> List[str]:
        if videoid:
            link = self.playlist_url + str(videoid)
        link = self._prepare_link(link).split("&")[0]

        try:
            plist = await Playlist.get(link)
            items = [video.get("id") for video in plist.get("videos", [])[:limit] if video.get("id")]
            if items: return items
        except Exception:
            pass

        stdout, _ = await _exec_proc(
            "yt-dlp",
            *(_cookies_args()),
            "-i",
            "--get-id",
            "--flat-playlist",
            "--playlist-end",
            str(limit),
            "--skip-download",
            link,
        )
        items = stdout.decode().strip().split("\n") if stdout else []
        return [i for i in items if i]

    @capture_internal_err
    async def slider(
        self, link: str, query_type: int, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], str, str]:
        # Slider needs real results, fallback not appropriate here
        try:
            data = await VideosSearch(self._prepare_link(link, videoid), limit=10).next()
            results = data.get("result", [])
        except Exception:
            query = self._prepare_link(link, videoid)
            stdout, _ = await _exec_proc(
                "yt-dlp", 
                *(_cookies_args()), 
                "--dump-json", 
                "--default-search", 
                "ytsearch10", 
                "--no-playlist", 
                query
            )
            results = []
            if stdout:
                for line in stdout.decode().split("\n"):
                    if not line: continue
                    try:
                        v = json.loads(line)
                        results.append({
                            "title": v.get("title"),
                            "duration": v.get("duration_string"),
                            "thumbnails": [{"url": v.get("thumbnail")}],
                            "id": v.get("id")
                        })
                    except: pass
        
        if not results or query_type >= len(results):
            raise IndexError(f"Query type index {query_type} out of range")
        r = results[query_type]
        return (
            r.get("title", ""),
            r.get("duration"),
            r.get("thumbnails", [{}])[-1].get("url", "").split("?")[0],
            r.get("id", ""),
        )

    # ✅ Download uses media_download helper
    @capture_internal_err
    async def download(
        self,
        link: str,
        mystic,
        *,
        video: Union[bool, str, None] = None,
        videoid: Union[str, bool, None] = None,
    ) -> Union[Tuple[str, Optional[bool]], Tuple[None, None]]:
        link = self._prepare_link(link, videoid)
        
        # We need a title for the download filename
        title = await self.title(link)
        if not title or title == "Unknown":
            # If title is unknown, try to use ID
            vid_id = self._extract_id_from_url(link)
            title = vid_id if vid_id else "Audio_Clip"

        if video:
            if await self.is_live(link):
                status, stream_url = await self.video(link)
                return (stream_url, None) if status == 1 else (None, None)
            
            p = await media_download(link, "video", title)
            return (p, True) if p else (None, None)

        p = await media_download(link, "audio", title)
        return (p, True) if p else (None, None)
