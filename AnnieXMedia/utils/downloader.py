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
    try:
        if _COOKIES_FILE and os.path.exists(_COOKIES_FILE) and os.path.getsize(_COOKIES_FILE) > 0:
            return _COOKIES_FILE
    except Exception:
        pass
    return None


def find_cached_file(video_id: str) -> Optional[str]:
    if not video_id:
        return None
    for ext in ("mp3", "m4a", "webm", "mp4", "mkv"):
        path = f"{DOWNLOAD_DIR}/{video_id}.{ext}"
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


async def _poll_download_api(
    poll_url: str,
    vid: str,
    default_ext: str,
    is_video: bool,
    max_wait: float = 180.0,
    poll_interval: float = 1.5,
) -> Optional[str]:
    """
    Generic polling helper for audio/video API.

    - Accepts multiple 'in-progress' and 'success' status values.
    - Stops after max_wait seconds.
    - Tries several common keys for URL / format / filename.
    """
    media_type = "video" if is_video else "audio"

    try:
        session = await get_http_session()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max_wait

        while True:
            if loop.time() > deadline:
                LOGGER.warning(f"{media_type.capitalize()} API poll timeout for {vid}")
                return None

            try:
                async with session.get(poll_url) as r:
                    if r.status != 200:
                        text = await r.text()
                        LOGGER.warning(
                            f"{media_type.capitalize()} API returned "
                            f"HTTP {r.status} for {vid}. Body: {text[:200]}"
                        )
                        await asyncio.sleep(poll_interval)
                        continue

                    data = await r.json()
            except Exception as e:
                LOGGER.error(
                    f"Error while calling {media_type} API for {vid}: {e}",
                    exc_info=True,
                )
                await asyncio.sleep(poll_interval)
                continue

            # Try to read a status field in a flexible way
            raw_status = (
                data.get("status")
                or data.get("state")
                or data.get("result")
                or data.get("phase")
            )
            status = str(raw_status).lower() if raw_status is not None else ""

            # Values that mean it's still working
            in_progress_statuses = {
                "downloading",
                "processing",
                "pending",
                "queued",
                "working",
                "busy",
                "start",
            }

            # Values that mean it's done / success
            success_statuses = {
                "done",
                "finished",
                "completed",
                "success",
                "ok",
            }

            if status in in_progress_statuses or (not status and not data.get("link") and not data.get("url") and not data.get("download_url")):
                LOGGER.debug(
                    f"{media_type.capitalize()} API still processing {vid}: "
                    f"status='{status}' data={str(data)[:200]}"
                )
                await asyncio.sleep(poll_interval)
                continue

            if status not in success_statuses:
                LOGGER.warning(
                    f"{media_type.capitalize()} API returned terminal status for {vid}: "
                    f"status='{status}' data={str(data)[:200]}"
                )
                return None

            # Try several possible keys for the download URL
            dl_url = data.get("link") or data.get("url") or data.get("download_url")
            if not dl_url:
                LOGGER.warning(
                    f"{media_type.capitalize()} API indicated success but no download URL for {vid}. "
                    f"Data: {str(data)[:200]}"
                )
                return None

            # Try to determine extension
            fmt_raw = data.get("format") or data.get("ext") or default_ext
            fmt = str(fmt_raw).split("/")[-1].strip().lower() if fmt_raw else default_ext
            if not fmt:
                fmt = default_ext

            # Try to use provided filename if exists
            filename = data.get("filename") or data.get("file_name")
            if filename:
                safe_name = os.path.basename(str(filename))
                out_path = os.path.join(DOWNLOAD_DIR, safe_name)
            else:
                out_path = os.path.join(DOWNLOAD_DIR, f"{vid}.{fmt}")

            LOGGER.info(
                f"{media_type.capitalize()} API ready for {vid}, downloading to '{out_path}' "
                f"with format '{fmt}'"
            )
            return await download_file(dl_url, out_path)

    except Exception as e:
        LOGGER.error(
            f"Unexpected exception in {media_type} API polling for {vid}: {e}",
            exc_info=True,
        )
        return None


async def api_download_audio(link: str) -> Optional[str]:
    if not USE_AUDIO_API:
        return None

    vid = extract_video_id(link)
    if not vid:
        LOGGER.warning(f"api_download_audio: could not extract video id from link: {link}")
        return None

    base = API_URL.rstrip("/")
    poll_url = f"{base}/song/{vid}?api={API_KEY}"

    LOGGER.info(f"Starting audio API download for {vid} with URL: {poll_url}")
    return await _poll_download_api(
        poll_url=poll_url,
        vid=vid,
        default_ext="webm",
        is_video=False,
    )


async def api_download_video(link: str) -> Optional[str]:
    if not USE_VIDEO_API:
        return None

    vid = extract_video_id(link)
    if not vid:
        LOGGER.warning(f"api_download_video: could not extract video id from link: {link}")
        return None

    base = VIDEO_API_URL.rstrip("/")
    poll_url = f"{base}/video/{vid}?api={API_KEY}"

    LOGGER.info(f"Starting video API download for {vid} with URL: {poll_url}")
    return await _poll_download_api(
        poll_url=poll_url,
        vid=vid,
        default_ext="mp4",
        is_video=True,
    )


def get_final_path_from_info(info: Dict) -> Optional[str]:
    vid = info.get("id")
    if not vid:
        return None
    ext = info.get("ext")
    if ext:
        p = f"{DOWNLOAD_DIR}/{vid}.{ext}"
        if os.path.exists(p):
            return p
    matches = sorted(
        glob.glob(f"{DOWNLOAD_DIR}/{vid}.*"),
        key=os.path.getmtime,
        reverse=True,
    )
    return matches[0] if matches else None


def download_with_ytdlp_sync(link: str, fmt: str) -> Optional[str]:
    try:
        opts = get_ytdlp_base_opts()
        opts["format"] = fmt
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(link, download=False)
            if path := get_final_path_from_info(info):
                return path
            ydl.download([link])
            return get_final_path_from_info(info)
    except Exception:
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


async def race_ytdlp_and_api(yt_task, api_task, title: str):
    done, pending = await asyncio.wait(
        {yt_task, api_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in done:
        result = task.result()
        if result and os.path.exists(result):
            source = "yt-dlp" if task is yt_task else "API"
            log_download_source(title, source)
            for p in pending:
                p.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await p
            return result
    for task in pending:
        try:
            result = await task
            if result and os.path.exists(result):
                source = "yt-dlp" if task is yt_task else "API"
                log_download_source(title, source)
                return result
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    return None


async def yt_dlp_download(link: str, type: str, title: str = "") -> Optional[str]:
    loop = asyncio.get_running_loop()
    vid = extract_video_id(link)
    if cached := find_cached_file(vid):
        if title:
            LOGGER.info(f"Track '{title}' - Served from cache")
        return cached

    if type == "audio":
        key = f"audio:{link}"

        async def run():
            ytdlp_task = asyncio.create_task(
                run_with_semaphore(
                    loop.run_in_executor(
                        None,
                        download_with_ytdlp_sync,
                        link,
                        "bestaudio[ext=webm][acodec=opus]",
                    )
                )
            )
            api_task = (
                asyncio.create_task(api_download_audio(link))
                if USE_AUDIO_API
                else None
            )
            if api_task:
                return await race_ytdlp_and_api(ytdlp_task, api_task, title or "Unknown")
            result = await ytdlp_task
            if result and title:
                log_download_source(title, "yt-dlp")
            return result

        return await deduplicate_download(key, run)

    elif type == "video":
        key = f"video:{link}"

        async def run():
            ytdlp_task = asyncio.create_task(
                run_with_semaphore(
                    loop.run_in_executor(
                        None,
                        download_with_ytdlp_sync,
                        link,
                        "(bestvideo[height<=?720][width<=?1280][ext=mp4])+(bestaudio)",
                    )
                )
            )
            api_task = (
                asyncio.create_task(api_download_video(link))
                if USE_VIDEO_API
                else None
            )
            if api_task:
                return await race_ytdlp_and_api(ytdlp_task, api_task, title or "Unknown")
            result = await ytdlp_task
            if result and title:
                log_download_source(title, "yt-dlp")
            return result

        return await deduplicate_download(key, run)

    return None
