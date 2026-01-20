# Authored By Certified Coders © 2025
import asyncio
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
from AnnieXMedia.utils.tuning import YTDLP_TIMEOUT, YOUTUBE_META_MAX, YOUTUBE_META_TTL


# === Caches ===
CACHE_FILE = "youtube_cache.json"
_cache_lock = asyncio.Lock()

# === Concurrency Control (Fix for Too Many Open Files) ===
_API_SEMAPHORE = asyncio.Semaphore(6)


# === Constants ===
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")


# === Helpers ===
def _cookiefile_path() -> Optional[str]:
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
    async with _API_SEMAPHORE:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            return await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
        except asyncio.TimeoutError:
            with contextlib.suppress(Exception):
                proc.kill()
            return b"", b"timeout"


# === JSON Cache Helpers ===
async def _get_from_cache(key: str) -> Optional[Any]:
    async with _cache_lock:
        if not os.path.exists(CACHE_FILE):
            return None
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            if key in data:
                timestamp, value = data[key]
                if time.time() - timestamp < YOUTUBE_META_TTL:
                    return value
        except Exception:
            pass
    return None


async def _save_to_cache(key: str, value: Any):
    async with _cache_lock:
        try:
            if os.path.exists(CACHE_FILE):
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = {}
        except Exception:
            data = {}

        if len(data) > YOUTUBE_META_MAX:
            data.clear()

        data[key] = (time.time(), value)

        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception:
            pass


@capture_internal_err
async def cached_youtube_search(query: str) -> List[Dict]:
    key = f"q:{query}"
    cached_result = await _get_from_cache(key)
    if cached_result is not None:
        return cached_result

    try:
        async with _API_SEMAPHORE:
            data = await VideosSearch(query, limit=1).next()
        result = data.get("result", [])
    except Exception:
        result = []

    if result:
        await _save_to_cache(key, result)

    return result


# === Main Class ===
class YouTubeAPI:
    def __init__(self) -> None:
        self.base_url = "https://www.youtube.com/watch?v="
        self.playlist_url = "https://youtube.com/playlist?list="
        self._url_pattern = re.compile(r"(?:youtube\.com|youtu\.be)")

    def _prepare_link(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        if isinstance(videoid, str) and videoid.strip():
            link = self.base_url + videoid.strip()
        link = link.strip()
        if "youtu.be" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        elif "youtube.com/shorts/" in link or "youtube.com/live/" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        return link.split("&")[0]

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

    async def _ensure_watch_url(self, maybe_query_or_url: str) -> Optional[str]:
        prepared = self._prepare_link(maybe_query_or_url)
        if prepared.startswith("http"):
            return prepared
        data = await cached_youtube_search(prepared)
        if not data:
            return None
        vid = data[0].get("id")
        return self.base_url + vid if vid else None

    @capture_internal_err
    async def _fetch_video_info(self, query: str, *, use_cache: bool = True) -> Optional[Dict]:
        q = self._prepare_link(query)
        if use_cache and not q.startswith("http"):
            res = await cached_youtube_search(q)
            return res[0] if res else None
        
        async with _API_SEMAPHORE:
            data = await VideosSearch(q, limit=1).next()
        result = data.get("result", [])
        return result[0] if result else None

    @capture_internal_err
    async def is_live(self, link: str) -> bool:
        prepared = self._prepare_link(link)
        stdout, _ = await _exec_proc("yt-dlp", *(_cookies_args()), "--dump-json", prepared)
        if not stdout:
            return False
        try:
            info = json.loads(stdout.decode())
            return bool(info.get("is_live"))
        except json.JSONDecodeError:
            return False

    @capture_internal_err
    async def details(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], int, str, str]:
        prepared_link = self._prepare_link(link, videoid)

        try:
            info = await self._fetch_video_info(prepared_link)
            if not info:
                raise ValueError("No results from youtubesearchpython (VideosSearch)")
        except Exception as search_err:
            raise ValueError("Video not found", {"cause": str(search_err)}) from search_err

        # --- FALLBACK LOGIC ---
        dt = info.get("duration") or "00:00"  # Fallback to 00:00
        title = info.get("title") or "Unknown" # Fallback to Unknown
        
        ds = int(time_to_seconds(dt)) if dt != "00:00" else 0
        thumb = (
            info.get("thumbnail")
            or info.get("thumbnails", [{}])[-1].get("url", "")
        ).split("?")[0]

        return title, dt, ds, thumb, info.get("id", "")

    @capture_internal_err
    async def title(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return (info.get("title") or "Unknown") if info else "Unknown"

    @capture_internal_err
    async def duration(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return (info.get("duration") or "00:00") if info else "00:00"

    @capture_internal_err
    async def thumbnail(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return (
            info.get("thumbnail")
            or info.get("thumbnails", [{}])[-1].get("url", "")
        ).split("?")[0] if info else ""

    @capture_internal_err
    async def track(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[Dict, str]:
        prepared_link = self._prepare_link(link, videoid)

        try:
            info = await self._fetch_video_info(prepared_link)
            if not info:
                raise ValueError(f"No results for: '{prepared_link}'")
        except Exception as search_err:
            yt_link = prepared_link
            if not yt_link.startswith("http"):
                yt_link = f"ytsearch1:{prepared_link}"

            stdout, stderr = await _exec_proc(
                "yt-dlp", *(_cookies_args()), "--dump-json", "--no-warnings", yt_link
            )

            if not stdout:
                raise ValueError(f"Failed to fetch track info: {search_err}")

            try:
                info = json.loads(stdout.decode())
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON response from yt-dlp")

        thumb = (
            info.get("thumbnail")
            or info.get("thumbnails", [{}])[-1].get("url", "")
        ).split("?")[0]

        # --- FALLBACK LOGIC APPLIED HERE ---
        details = {
            "title": info.get("title") or "Unknown",
            "link": info.get("webpage_url", prepared_link),
            "vidid": info.get("id", ""),
            "duration_min": info.get("duration") or "00:00",
            "thumb": thumb,
        }
        return details, info.get("id", "")

    @capture_internal_err
    async def video(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[int, str]:
        link = self._prepare_link(link, videoid)
        stdout, stderr = await _exec_proc(
            "yt-dlp", *(_cookies_args()), "-g", "-f", "best[height<=?720][width<=?1280]", link,
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
            async with _API_SEMAPHORE:
                plist = await Playlist.get(link)
            items = [video.get("id") for video in plist.get("videos", [])[:limit] if video.get("id")]
            if items:
                return items
        except Exception:
            pass

        stdout, _ = await _exec_proc(
            "yt-dlp", *(_cookies_args()), "-i", "--get-id", "--flat-playlist", 
            "--playlist-end", str(limit), "--skip-download", link,
        )
        items = stdout.decode().strip().split("\n") if stdout else []
        return [i for i in items if i]

    @capture_internal_err
    async def formats(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[List[Dict], str]:
        link = self._prepare_link(link, videoid)
        key = f"f:{link}"

        cached_result = await _get_from_cache(key)
        if cached_result:
            return cached_result[0], cached_result[1]

        opts = {"quiet": True}
        if cf := _cookiefile_path():
            opts["cookiefile"] = cf

        out: List[Dict] = []
        try:
            async with _API_SEMAPHORE:
                loop = asyncio.get_running_loop()
                info = await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(opts).extract_info(link, download=False))
                
            for fmt in info.get("formats", []):
                if "dash" in str(fmt.get("format", "")).lower(): continue
                if not any(k in fmt for k in ("filesize", "filesize_approx")): continue
                
                size = fmt.get("filesize") or fmt.get("filesize_approx")
                if not size: continue
                
                out.append({
                    "format": fmt["format"],
                    "filesize": size,
                    "format_id": fmt["format_id"],
                    "ext": fmt["ext"],
                    "format_note": fmt.get("format_note", ""),
                    "yturl": link,
                })
        except Exception:
            pass

        await _save_to_cache(key, (out, link))
        return out, link

    @capture_internal_err
    async def slider(
        self, link: str, query_type: int, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], str, str]:
        async with _API_SEMAPHORE:
            data = await VideosSearch(self._prepare_link(link, videoid), limit=10).next()
        results = data.get("result", [])
        if not results or query_type >= len(results):
            raise IndexError(f"Query index {query_type} out of range")
        r = results[query_type]
        return (
            r.get("title") or "Unknown",
            r.get("duration") or "00:00",
            r.get("thumbnails", [{}])[-1].get("url", "").split("?")[0],
            r.get("id", ""),
        )

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

        if video:
            if await self.is_live(link):
                status, stream_url = await self.video(link)
                if status == 1:
                    return stream_url, None
                return None, None

            title = await self.title(link)
            p = await media_download(link, "video", title)
            return (p, True) if p else (None, None)

        title = await self.title(link)
        p = await media_download(link, "audio", title)
        return (p, True) if p else (None, None)
