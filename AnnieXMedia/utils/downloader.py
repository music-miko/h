# Authored By Certified Coders © 2025
import asyncio
import contextlib
import glob
import os
import re
from typing import Dict, Optional

import aiofiles
import aiohttp
from aiohttp import TCPConnector
from yt_dlp import YoutubeDL

from AnnieXMedia.core.dir import CACHE_DIR, DOWNLOAD_DIR
from AnnieXMedia.utils.cookie_handler import COOKIE_PATH as _COOKIES_FILE
from AnnieXMedia.utils.tuning import CHUNK_SIZE, SEM
from config import API_KEY, API_URL, VIDEO_API_URL
from AnnieXMedia.logging import LOGGER

LOGGER = LOGGER(__name__)

# We treat:
#   API_URL      -> base "https://deadlinetech.site"
#   VIDEO_API_URL -> just a toggle; real endpoint is same /song/{video_id}
USE_AUDIO_API = bool(API_URL and API_KEY)
USE_VIDEO_API = bool(VIDEO_API_URL and API_KEY)

_inflight: Dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()

YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")


def log_download_source(title: str, source: str) -> None:
    LOGGER.info(f"Track '{title}' - Downloaded by {source}")


def extract_video_id(link: str) -> str:
    if not link:
        return ""
    s = link.strip()
    if YOUTUBE_ID_RE.match(s):
        return s
    if "v=" in s:
        return s.split("v=")[-1].split("&")[0]
    last = s.split("/")[-1].split("?")[0]
    if YOUTUBE_ID_RE.match(last):
        return last
    return ""


def get_cookie_file() -> Optional[str]:
    """
    Only return cookie file if it exists & looks like Netscape format.
    Prevents yt-dlp from spamming 'does not look like a Netscape format cookies file'.
    """
    try:
        if not _COOKIES_FILE:
            return None
        if not os.path.exists(_COOKIES_FILE) or os.path.getsize(_COOKIES_FILE) <= 0:
            return None

        with open(_COOKIES_FILE, "rb") as f:
            header = f.read(256)

        if b"Netscape HTTP Cookie File" in header:
            return _COOKIES_FILE

        LOGGER.warning(
            f"Cookie file '{_COOKIES_FILE}' exists but is not Netscape format. Ignoring it for yt-dlp."
        )
        return None
    except Exception as e:
        LOGGER.warning(f"Error while checking cookie file '{_COOKIES_FILE}': {e}")
        return None


def find_cached_file(video_id: str, media_type: Optional[str] = None) -> Optional[str]:
    """
    Look for a cached file for this video id.

    - For audio: accept audio-type extensions (webm audio, mp3, m4a, etc.).
    - For video: only accept video containers we know we use for video (mp4, mkv).
      This avoids using an audio-only .webm as a 'video' file and breaking pytgcalls.
    """
    if not video_id:
        return None

    if media_type == "video":
        exts = ("mp4", "mkv")
    elif media_type == "audio":
        exts = ("mp3", "m4a", "webm", "opus", "ogg")
    else:
        exts = ("mp3", "m4a", "webm", "mp4", "mkv", "opus", "ogg")

    for ext in exts:
        path = os.path.join(DOWNLOAD_DIR, f"{video_id}.{ext}")
        if os.path.exists(path):
            return path

    return None


def get_ytdlp_base_opts() -> Dict[str, object]:
    opts = {
        "outtmpl": f"{DOWNLOAD_DIR}/%(id)s.%(ext)s",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "overwrites": False,
        "continuedl": True,
        "noprogress": True,
        "concurrent_fragment_downloads": 16,
        "http_chunk_size": 1 << 20,
        "socket_timeout": 15,
        "retries": 1,
        "fragment_retries": 1,
        "cachedir": str(CACHE_DIR),
        "ignoreerrors": True,
        "merge_output_format": "mp4",
    }
    if cookiefile := get_cookie_file():
        opts["cookiefile"] = cookiefile
    return opts


async def get_http_session() -> aiohttp.ClientSession:
    global _session
    if _session and not _session.closed:
        return _session
    async with _session_lock:
        if _session and not _session.closed:
            return _session
        timeout = aiohttp.ClientTimeout(total=600, sock_connect=20, sock_read=60)
        connector = TCPConnector(limit=0, ttl_dns_cache=300, enable_cleanup_closed=True)
        _session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return _session


async def close_http_session() -> None:
    global _session
    async with _session_lock:
        if _session and not _session.closed:
            await _session.close()
        _session = None


async def download_file(url: str, out_path: str) -> Optional[str]:
    if not url:
        return None
    try:
        session = await get_http_session()
        async with session.get(url) as resp:
            if resp.status != 200:
                LOGGER.warning(f"API file download failed with status {resp.status} for URL: {url}")
                return None
            async with aiofiles.open(out_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    if not chunk:
                        break
                    await f.write(chunk)
        if os.path.exists(out_path):
            return out_path
        LOGGER.warning(f"Download finished but file not found on disk: {out_path}")
        return None
    except Exception as e:
        LOGGER.error(f"Exception while downloading file from API: {e}", exc_info=True)
        return None


# ============== Deadlinetech.site API integration ==============

async def _deadlinetech_download(link: str, media_type: str) -> Optional[str]:
    """
    Call Deadlinetech song endpoint:

        GET {API_URL}/song/{video_id}?media_type=audio|video&api_key=...

    Handles BOTH:
      1) JSON response (like merged_api.py) :
           {
             "status": "done",
             "file_path": "downloads/ID.ext",
             "ext": "m4a/mp4",
             ...
           }

         - If file_path is HTTP URL → download it.
         - If file_path is relative/local:
             a) if exists locally, use it
             b) else build remote URL: {API_URL}/{file_path} and download

      2) Direct binary audio/video response.
    """
    if not API_URL or not API_KEY:
        return None

    vid = extract_video_id(link)
    if not vid:
        LOGGER.warning(f"_deadlinetech_download: could not extract video id from link: {link}")
        return None

    base = API_URL.rstrip("/")
    url = f"{base}/song/{vid}"
    params = {"media_type": media_type, "api_key": API_KEY}

    try:
        session = await get_http_session()
        LOGGER.info(f"Deadlinetech API {media_type} request: {url} params={params}")
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                LOGGER.warning(
                    f"Deadlinetech API returned HTTP {resp.status} for {vid}. "
                    f"Body: {text[:200]}"
                )
                return None

            content_type = (resp.headers.get("Content-Type") or "").lower()

            # ---- Case 1: JSON API (merged_api style) ----
            if "application/json" in content_type or "text/json" in content_type:
                try:
                    data = await resp.json()
                except Exception as e:
                    LOGGER.error(f"Deadlinetech JSON parse error for {vid}: {e}", exc_info=True)
                    return None

                status = str(data.get("status", "")).lower()
                if status and status not in {"done", "success", "ok", "completed", "finished"}:
                    LOGGER.warning(
                        f"Deadlinetech JSON non-success status for {vid}: "
                        f"status='{status}' data={str(data)[:200]}"
                    )
                    return None

                file_path = (
                    data.get("file_path")
                    or data.get("path")
                    or data.get("file")
                    or data.get("local_path")
                )

                if not file_path:
                    LOGGER.warning(
                        f"Deadlinetech JSON response missing file_path for {vid}: {str(data)[:200]}"
                    )
                    return None

                # If API gives HTTP URL in JSON, download it
                if str(file_path).startswith(("http://", "https://")):
                    ext = data.get("ext") or ("mp4" if media_type == "video" else "m4a")
                    ext = str(ext).split("/")[-1].lower() if ext else (
                        "mp4" if media_type == "video" else "m4a"
                    )
                    out_path = os.path.join(DOWNLOAD_DIR, f"{vid}.{ext}")
                    LOGGER.info(
                        f"Deadlinetech JSON returned remote URL for {vid}, downloading to {out_path}"
                    )
                    return await download_file(file_path, out_path)

                # Local / relative path from API
                local_path = str(file_path)

                # 1) If absolute and exists, use it directly
                if os.path.isabs(local_path) and os.path.exists(local_path):
                    return local_path

                # 2) If exists inside our DOWNLOAD_DIR, use it
                candidate = os.path.join(DOWNLOAD_DIR, os.path.basename(local_path))
                if os.path.exists(candidate):
                    return candidate

                # 3) Otherwise, treat it as a relative path on Deadlinetech server
                #    and download via HTTP, e.g.:
                #    file_path = "downloads/ID.webm"
                #    -> https://deadlinetech.site/downloads/ID.webm
                remote_url = f"{base}/{local_path.lstrip('/')}"
                # Decide extension from data or from file_path itself
                ext = (
                    data.get("ext")
                    or os.path.splitext(local_path)[1].lstrip(".")
                    or ("mp4" if media_type == "video" else "m4a")
                )
                ext = str(ext).split("/")[-1].lower()
                out_path = os.path.join(DOWNLOAD_DIR, f"{vid}.{ext}")
                LOGGER.info(
                    f"Deadlinetech JSON file_path not local; downloading from {remote_url} "
                    f"to {out_path}"
                )
                return await download_file(remote_url, out_path)

            # ---- Case 2: Direct binary stream ----
            if "audio" in content_type:
                ext = "m4a" if "mpeg" in content_type or "mp4" in content_type else "webm"
            elif "video" in content_type:
                ext = "mp4" if "mp4" in content_type or "x-m4v" in content_type else "mkv"
            else:
                ext = "mp4" if media_type == "video" else "m4a"

            out_path = os.path.join(DOWNLOAD_DIR, f"{vid}.{ext}")
            LOGGER.info(
                f"Deadlinetech binary response for {vid} ({media_type}), writing to {out_path}"
            )

            async with aiofiles.open(out_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    if not chunk:
                        break
                    await f.write(chunk)

            if os.path.exists(out_path):
                return out_path

            LOGGER.warning(
                f"Deadlinetech binary download finished but file not found for {vid}: {out_path}"
            )
            return None

    except Exception as e:
        LOGGER.error(f"Deadlinetech API error for {vid}: {e}", exc_info=True)
        return None


async def api_download_audio(link: str) -> Optional[str]:
    if not USE_AUDIO_API:
        return None
    return await _deadlinetech_download(link, media_type="audio")


async def api_download_video(link: str) -> Optional[str]:
    if not (USE_VIDEO_API or USE_AUDIO_API):
        return None
    return await _deadlinetech_download(link, media_type="video")


# ============== yt-dlp helpers & fallbacks ==============

def get_final_path_from_info(info: Optional[Dict]) -> Optional[str]:
    if not info:
        return None

    vid = info.get("id")
    if not vid:
        return None

    ext = info.get("ext")
    if ext:
        p = os.path.join(DOWNLOAD_DIR, f"{vid}.{ext}")
        if os.path.exists(p):
            return p

    matches = sorted(
        glob.glob(os.path.join(DOWNLOAD_DIR, f"{vid}.*")),
        key=os.path.getmtime,
        reverse=True,
    )
    return matches[0] if matches else None


def normalize_ytdlp_link(link: str) -> str:
    if not link:
        return link

    s = link.strip()

    if s.startswith(("http://", "https://")):
        return s

    if YOUTUBE_ID_RE.match(s):
        return s

    if "youtu" in s:
        if s.startswith(("http://", "https://")):
            return s
        return "https://" + s

    return f"ytsearch1:{s}"


def download_with_ytdlp_sync(link: str, fmt: str) -> Optional[str]:
    try:
        norm_link = normalize_ytdlp_link(link)
        opts = get_ytdlp_base_opts()
        opts["format"] = fmt

        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(norm_link, download=False)
            if path := get_final_path_from_info(info):
                return path
            ydl.download([norm_link])
            return get_final_path_from_info(info)
    except Exception as e:
        LOGGER.error(f"yt-dlp download failed for link='{link}': {e}", exc_info=True)
        return None


async def run_with_semaphore(coro):
    async with SEM:
        return await coro


async def deduplicate_download(key: str, runner):
    async with _inflight_lock:
        if fut := _inflight.get(key):
            return await fut
        fut = asyncio.get_running_loop().create_future()
        _inflight[key] = fut
    try:
        result = await runner()
        fut.set_result(result)
        return result
    except Exception as e:
        fut.set_exception(e)
        return None
    finally:
        async with _inflight_lock:
            _inflight.pop(key, None)


async def yt_dlp_download(link: str, type: str, title: str = "") -> Optional[str]:
    """
    Main entry:

    1) Try Deadlinetech API (audio/video) if enabled.
    2) If API fails OR returns invalid data → fall back to yt-dlp.
    3) Uses type-aware cache to avoid using audio file for video, etc.
    """
    loop = asyncio.get_running_loop()
    vid = extract_video_id(link)

    if cached := find_cached_file(vid, type):
        if title:
            LOGGER.info(
                f"Track '{title}' - Served from cache "
                f"({'video' if type == 'video' else 'audio'})"
            )
        return cached

    if type == "audio":
        key = f"audio:{link}"

        async def run():
            if USE_AUDIO_API:
                api_result = await api_download_audio(link)
                if api_result and os.path.exists(api_result):
                    log_download_source(title or "Unknown", "API")
                    return api_result

            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(
                    None,
                    download_with_ytdlp_sync,
                    link,
                    "bestaudio[ext=webm][acodec=opus]",
                )
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                if title:
                    log_download_source(title, "yt-dlp")
                return ytdlp_result

            return None

        return await deduplicate_download(key, run)

    elif type == "video":
        key = f"video:{link}"

        async def run():
            if USE_VIDEO_API or USE_AUDIO_API:
                api_result = await api_download_video(link)
                if api_result and os.path.exists(api_result):
                    log_download_source(title or "Unknown", "API")
                    return api_result

            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(
                    None,
                    download_with_ytdlp_sync,
                    link,
                    "(bestvideo[height<=?720][width<=?1280][ext=mp4])+(bestaudio)",
                )
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                if title:
                    log_download_source(title, "yt-dlp")
                return ytdlp_result

            return None

        return await deduplicate_download(key, run)

    return None
