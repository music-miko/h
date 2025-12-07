# Authored By Certified Coders © 2025
import asyncio
import glob
import os
import re
import urllib.parse
import uuid
from typing import Dict, Optional

import aiofiles
import aiohttp
from aiohttp import TCPConnector
from yt_dlp import YoutubeDL

from AnnieXMedia.core.dir import CACHE_DIR, DOWNLOAD_DIR
from AnnieXMedia.utils.cookie_handler import COOKIE_PATH as _COOKIES_FILE
from AnnieXMedia.utils.tuning import CHUNK_SIZE, SEM
from config import API_KEY, API_URL, API_KEY2, API_URL2
from AnnieXMedia.logging import LOGGER as _LOGGER

LOGGER = _LOGGER(__name__)

# Try to import Telegram app & errors (optional, used for t.me CDN links)
try:
    from AnnieXMedia import app as TG_APP
except Exception:  # pragma: no cover - optional dependency
    TG_APP = None

try:
    from pyrogram import errors as tg_errors
except Exception:  # pragma: no cover - optional dependency
    tg_errors = None

# Old Deadlinetech (merged_api) toggles – used ONLY for video now
USE_VIDEO_API = bool(API_URL2 and API_KEY2)

_inflight: Dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()

YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")

API_RETRIES = 3


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
            f"Cookie file '{_COOKIES_FILE}' exists but is not Netscape format. "
            "Ignoring it for yt-dlp."
        )
        return None
    except Exception as e:
        LOGGER.warning(f"Error while checking cookie file '{_COOKIES_FILE}': {e}")
        return None


def find_cached_file(video_id: str, media_type: Optional[str] = None) -> Optional[str]:
    """
    Look for a cached file for this video id on the *bot* server.

    - For audio: accept audio-type extensions (webm audio, mp3, m4a, etc.).
    - For video: only accept video containers (mp4, mkv).
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
    opts: Dict[str, object] = {
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
    """
    Generic HTTP downloader used for simple binary endpoints.
    """
    if not url:
        return None
    try:
        session = await get_http_session()
        async with session.get(url) as resp:
            if resp.status != 200:
                LOGGER.warning(f"HTTP download failed with status {resp.status} for URL: {url}")
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
        LOGGER.error(f"Exception while downloading file from {url}: {e}", exc_info=True)
        return None


# =======================================================================
# Old Deadlinetech merged API integration – used ONLY for VIDEO now
# =======================================================================


async def _deadlinetech_download(link: str, media_type: str) -> Optional[str]:
    """
    Call Deadlinetech merged API for VIDEO:

        GET {API_URL2}/song/{video_id}?media_type=video&api_key=...&return_file=true
    """
    if not API_URL2 or not API_KEY2:
        return None

    vid = extract_video_id(link)
    if not vid:
        LOGGER.warning(f"_deadlinetech_download: could not extract video id from link: {link}")
        return None

    base = API_URL2.rstrip("/")
    url = f"{base}/song/{vid}"
    params = {
        "media_type": media_type,
        "api_key": API_KEY2,
        "return_file": "true",
    }

    try:
        session = await get_http_session()
        LOGGER.info(f"Deadlinetech API {media_type} request: {url} params={params}")
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                text = ""
                try:
                    text = await resp.text()
                except Exception:
                    pass
                LOGGER.warning(
                    f"Deadlinetech API returned HTTP {resp.status} for {vid}. "
                    f"Body: {text[:200]}"
                )
                return None

            content_type = (resp.headers.get("Content-Type") or "").lower()
            dispo = resp.headers.get("Content-Disposition") or ""

            ext = None

            if "filename=" in dispo:
                filename = dispo.split("filename=")[-1].strip('";\' ')
                if "." in filename:
                    ext = filename.rsplit(".", 1)[-1].lower()

            if not ext:
                if media_type == "video":
                    if "webm" in content_type:
                        ext = "webm"
                    else:
                        ext = "mp4"
                else:
                    # Shouldn't reach here anymore for audio, but keep generic.
                    if "webm" in content_type or "opus" in content_type or "ogg" in content_type:
                        ext = "webm"
                    elif "mpeg" in content_type or "mp4" in content_type or "aac" in content_type:
                        ext = "m4a"
                    elif "mp3" in content_type:
                        ext = "mp3"
                    else:
                        ext = "m4a"

            out_path = os.path.join(DOWNLOAD_DIR, f"{vid}.{ext}")
            LOGGER.info(
                f"Deadlinetech binary response for {vid} ({media_type}), "
                f"Content-Type='{content_type}', saving to {out_path}"
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


async def api_download_video(link: str) -> Optional[str]:
    """
    Video download using old Deadlinetech merged API.
    """
    if not USE_VIDEO_API:
        return None
    return await _deadlinetech_download(link, media_type="video")


# =======================================================================
# Fallen-style Track API integration (for AUDIO – like fallenapi.py)
# =======================================================================


def _get_api_headers() -> Dict[str, str]:
    return {
        "X-API-Key": API_KEY or "",
        "Accept": "application/json",
    }


async def api_get_track(link: str) -> Optional[Dict]:
    """
    Call the new Fallen Track API:

        GET {API_URL}/track?url=<urlencoded_original_url>
    """
    if not API_URL or not API_KEY:
        return None

    base = API_URL.rstrip("/")
    encoded_url = urllib.parse.quote(link, safe="")
    endpoint = f"{base}/track?url={encoded_url}"

    for attempt in range(1, API_RETRIES + 1):
        try:
            session = await get_http_session()
            async with session.get(endpoint, headers=_get_api_headers()) as resp:
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    text = await resp.text()
                    LOGGER.warning(
                        f"[Track API] Non-JSON response (status {resp.status}): {text[:200]}"
                    )
                    return None

                if resp.status == 200:
                    if isinstance(data, dict):
                        return data
                    LOGGER.warning(
                        f"[Track API] Unexpected payload type: {type(data)}; expected dict"
                    )
                    return None

                error_msg = None
                if isinstance(data, dict):
                    error_msg = data.get("error") or data.get("message")
                    status_code = data.get("status", resp.status)
                else:
                    status_code = resp.status
                LOGGER.warning(
                    f"[Track API ERROR] {error_msg or 'Unexpected error'} "
                    f"(status {status_code})"
                )
                return None

        except aiohttp.ClientError as e:
            LOGGER.warning(
                f"[Track API NETWORK ERROR] Attempt {attempt}/{API_RETRIES} failed: {e}"
            )
        except asyncio.TimeoutError:
            LOGGER.warning(
                f"[Track API TIMEOUT] Attempt {attempt}/{API_RETRIES} exceeded timeout."
            )
        except Exception as e:
            LOGGER.warning(f"[Track API UNEXPECTED ERROR] {e}")

        await asyncio.sleep(1)

    LOGGER.warning("[Track API FAILED] All retry attempts exhausted.")
    return None


async def api_download_cdn(cdn_url: str) -> Optional[str]:
    """
    Download file from a generic CDN URL, saving into DOWNLOAD_DIR.
    """
    if not cdn_url:
        return None

    for attempt in range(1, API_RETRIES + 1):
        try:
            session = await get_http_session()
            async with session.get(cdn_url) as resp:
                if resp.status != 200:
                    LOGGER.warning(
                        f"[CDN HTTP {resp.status}] Failed to download from {cdn_url}"
                    )
                    return None

                cd = resp.headers.get("Content-Disposition")
                if cd:
                    match = re.findall(r'filename="?([^";]+)"?', cd)
                    filename = match[0] if match else None
                else:
                    filename = None

                if not filename:
                    filename = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4()[:8]}.mp3"

                out_path = os.path.join(DOWNLOAD_DIR, filename)

                async with aiofiles.open(out_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        if not chunk:
                            break
                        await f.write(chunk)

            if os.path.exists(out_path):
                LOGGER.info(f"[CDN] Download complete: {out_path}")
                return out_path

            LOGGER.warning(
                f"[CDN] Download finished but file not found on disk: {out_path}"
            )
            return None

        except aiohttp.ClientError as e:
            LOGGER.warning(
                f"[CDN NETWORK ERROR] Attempt {attempt}/{API_RETRIES} failed: {e}"
            )
        except asyncio.TimeoutError:
            LOGGER.warning(
                f"[CDN TIMEOUT] Attempt {attempt}/{API_RETRIES} exceeded timeout."
            )
        except Exception as e:
            LOGGER.warning(f"[CDN UNEXPECTED ERROR] {e}")

        await asyncio.sleep(1)

    LOGGER.warning("[CDN FAILED] All retry attempts exhausted.")
    return None


async def api_download_track(link: str) -> Optional[str]:
    """
    High-level wrapper for Fallen audio:

    - Call Track API to fetch metadata (cdnurl, etc.).
    - If cdnurl is a t.me link and a Telegram client is available, download via Telegram.
    - Otherwise, download from CDN via HTTP.
    """
    track = await api_get_track(link)
    if not track:
        LOGGER.warning("[Track API] No track metadata found.")
        return None

    cdn_url = None
    if isinstance(track, dict):
        cdn_url = track.get("cdnurl") or track.get("url")

    if not cdn_url:
        LOGGER.warning("[Track API] Response did not contain 'cdnurl'.")
        return None

    # If API returned a Telegram message link (t.me), use Telegram client when available.
    tg_match = re.match(r"https?://t\.me/([^/]+)/(\d+)", cdn_url)
    if tg_match and TG_APP is not None and tg_errors is not None:
        chat, msg_id = tg_match.groups()
        try:
            msg_id_int = int(msg_id)
        except ValueError:
            msg_id_int = None

        if msg_id_int is not None:
            for attempt in range(1, API_RETRIES + 1):
                # Build a unique filename to avoid 'downloads.temp' collisions
                unique_name = os.path.join(
                    DOWNLOAD_DIR,
                    f"fallen_{chat}_{msg_id_int}_{uuid.uuid4().hex[:6]}"
                )
                try:
                    msg = await TG_APP.get_messages(chat_id=chat, message_ids=msg_id_int)
                    file_path = await msg.download(file_name=unique_name)
                    LOGGER.info(f"[Track API] Telegram media downloaded: {file_path}")
                    return file_path
                except tg_errors.FloodWait as e:  # type: ignore[attr-defined]
                    LOGGER.warning(
                        f"[Track API TG FLOODWAIT] Sleeping {e.value}s before retry."
                    )
                    await asyncio.sleep(e.value)
                except Exception as e:
                    # If destination path already exists and file is present, reuse it
                    if "Destination path" in str(e) and os.path.exists(unique_name):
                        LOGGER.info(
                            f"[Track API] Reusing existing Telegram file: {unique_name}"
                        )
                        return unique_name
                    LOGGER.warning(f"[Track API TG DOWNLOAD ERROR] {e}")
                    break  # Don't retry non-FloodWait errors

    # Fallback: HTTP download from CDN
    return await api_download_cdn(cdn_url)


# =======================================================================
# yt-dlp helpers & fallbacks
# =======================================================================


def get_final_path_from_info(info: Optional[Dict]) -> Optional[str]:
    """
    Given yt-dlp's extracted info dict, try to locate the final downloaded file.
    """
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
    """
    If `link` is not a valid URL or a direct YouTube ID, treat it as a search query.
    """
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
    """
    Synchronous wrapper around yt-dlp.
    """
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
    - AUDIO:
        * Try Fallen Track API (new) first.
        * Fallback to yt-dlp audio.
    - VIDEO:
        * Try old Deadlinetech /song API first.
        * Fallback to yt-dlp video.
    - Type-aware cache so audio/video don't mix.
    """
    loop = asyncio.get_running_loop()
    vid = extract_video_id(link)
    cache_media_type = "audio" if type == "audio" else "video"

    # Type-aware cache lookup on bot server
    if vid and (cached := find_cached_file(vid, cache_media_type)):
        if title:
            LOGGER.info(
                f"Track '{title}' - Served from cache "
                f"({'video' if type == 'video' else 'audio'})"
            )
        return cached

    # Build dedup key: use video id if possible, otherwise normalized link
    dedup_id = vid or normalize_ytdlp_link(link)
    key = f"{type}:{dedup_id}"

    # AUDIO MODE (Fallen API + yt-dlp)
    if type == "audio":

        async def run():
            # 1) Fallen Track API (new)
            track_result = await api_download_track(link)
            if track_result and os.path.exists(track_result):
                log_download_source(title or "Unknown", "Fallen Track API")
                return track_result

            # 2) Fallback to yt-dlp with cookies (first attempt: opus/webm)
            primary_fmt = "bestaudio[ext=webm][acodec=opus]"
            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(
                    None,
                    download_with_ytdlp_sync,
                    link,
                    primary_fmt,
                )
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                if title:
                    log_download_source(title, f"yt-dlp ({primary_fmt})")
                return ytdlp_result

            # 3) Second attempt: generic bestaudio/best
            fallback_fmt = "bestaudio/best"
            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(
                    None,
                    download_with_ytdlp_sync,
                    link,
                    fallback_fmt,
                )
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                if title:
                    log_download_source(title, f"yt-dlp ({fallback_fmt})")
                return ytdlp_result

            return None

        return await deduplicate_download(key, run)

    # VIDEO MODE (Old API + yt-dlp)
    elif type == "video":

        async def run():
            # 1) Old Deadlinetech /song API
            if USE_VIDEO_API:
                api_result = await api_download_video(link)
                if api_result and os.path.exists(api_result):
                    log_download_source(title or "Unknown", "Deadlinetech Video API")
                    return api_result

            # 2) Fallback to yt-dlp with cookies (best 720p mp4 + audio)
            primary_fmt = (
                "(bestvideo[height<=?720][width<=?1280][ext=mp4])"
                "+(bestaudio/best)"
            )
            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(
                    None,
                    download_with_ytdlp_sync,
                    link,
                    primary_fmt,
                )
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                if title:
                    log_download_source(title, f"yt-dlp ({primary_fmt})")
                return ytdlp_result

            # 3) Fallback: single best mp4 or best overall
            fallback_fmt = "best[ext=mp4]/best"
            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(
                    None,
                    download_with_ytdlp_sync,
                    link,
                    fallback_fmt,
                )
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                if title:
                    log_download_source(title, f"yt-dlp ({fallback_fmt})")
                return ytdlp_result

            return None

        return await deduplicate_download(key, run)

    return None
