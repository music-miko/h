# Authored By Certified Coders © 2025
import asyncio
import glob
import os
import re
import urllib.parse
import uuid
from pathlib import Path
from typing import Dict, Optional, Any

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

# Optional Telegram client for t.me links
try:
    from AnnieXMedia import app as TG_APP
except Exception:  # pragma: no cover
    TG_APP = None

try:
    from pyrogram import errors as tg_errors
except Exception:  # pragma: no cover
    tg_errors = None

_inflight: Dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()

YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")
YOUTUBE_ID_IN_URL_RE = re.compile(r"""(?x)(?:v=|\/)([A-Za-z0-9_-]{11})|youtu\.be\/([A-Za-z0-9_-]{11})""")
TG_LINK_RE = re.compile(r"https?://t\.me/(?:(c)/(\d+)|([^/]+)/(\d+))", re.IGNORECASE)

# Retries requested
V2_API_RETRIES = 10
FALLBACK_RETRIES = 1  # Fallen /track fallback only
JOB_POLL_ATTEMPTS = 10
JOB_POLL_INTERVAL = 2.0
JOB_POLL_BACKOFF = 1.2


# -----------------------
# small helpers
# -----------------------
def log_download_source(title: str, source: str) -> None:
    LOGGER.info(f"Track '{title}' - Downloaded by {source}")


def extract_video_id(link: str) -> str:
    if not link:
        return ""
    s = link.strip()
    if YOUTUBE_ID_RE.match(s):
        return s
    m = YOUTUBE_ID_IN_URL_RE.search(s)
    if m:
        return m.group(1) or m.group(2) or ""
    if "v=" in s:
        return s.split("v=")[-1].split("&")[0]
    last = s.split("/")[-1].split("?")[0]
    if YOUTUBE_ID_RE.match(last):
        return last
    return ""


def _as_download_dir(path: str) -> str:
    """
    Force pyrogram to treat this as a DIRECTORY.
    Prevents 'downloads.temp' errors when a file-ish path is passed.
    """
    p = str(Path(path).resolve())
    if not p.endswith(os.sep):
        p += os.sep
    return p


def _resolve_if_dir(download_result: str) -> Optional[str]:
    """
    If pyrogram returns a directory path, pick the newest file inside it.
    """
    if not download_result:
        return None
    p = Path(download_result)
    if p.exists() and p.is_file():
        return str(p)
    if p.exists() and p.is_dir():
        files = [x for x in p.iterdir() if x.is_file()]
        if not files:
            return None
        newest = max(files, key=lambda x: x.stat().st_mtime)
        return str(newest)
    return download_result


def get_cookie_file() -> Optional[str]:
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
    if not video_id:
        return None
    if media_type == "video":
        exts = ("mp4", "mkv", "webm")
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


# -----------------------
# V2 API (primary)
# -----------------------
def _v2_headers() -> Dict[str, str]:
    return {"X-API-Key": API_KEY or "", "Accept": "application/json"}


async def _v2_request_json(endpoint: str, params: Dict[str, Any]) -> Optional[Any]:
    if not API_URL or not API_KEY:
        LOGGER.warning("[V2] API_URL/API_KEY not configured.")
        return None

    base = API_URL.rstrip("/")
    url = f"{base}/{endpoint.lstrip('/')}"
    if API_KEY and "api_key" not in params:
        params["api_key"] = API_KEY

    for attempt in range(1, V2_API_RETRIES + 1):
        try:
            session = await get_http_session()
            LOGGER.info(f"[V2] REQUEST attempt={attempt}/{V2_API_RETRIES} url={url} params={params}")
            async with session.get(url, params=params, headers=_v2_headers()) as resp:
                text = await resp.text()
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    LOGGER.warning(f"[V2] Non-JSON response status={resp.status} body={text[:200]}")
                    return None

                LOGGER.info(f"[V2] RESPONSE status={resp.status} url={url}")
                if 200 <= resp.status < 300:
                    return data

                LOGGER.warning(f"[V2] API error status={resp.status} data={str(data)[:300]}")
                return data

        except aiohttp.ClientError as e:
            LOGGER.warning(f"[V2] NETWORK attempt={attempt}/{V2_API_RETRIES} err={e}")
        except asyncio.TimeoutError:
            LOGGER.warning(f"[V2] TIMEOUT attempt={attempt}/{V2_API_RETRIES}")
        except Exception as e:
            LOGGER.error(f"[V2] UNEXPECTED attempt={attempt}/{V2_API_RETRIES} err={e}", exc_info=True)

        await asyncio.sleep(1)

    LOGGER.warning("[V2] FAILED all retries exhausted.")
    return None


def _extract_candidate(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        return s if s else None
    if isinstance(obj, list) and obj:
        return _extract_candidate(obj[0])
    if isinstance(obj, dict):
        # job.result.public_url style
        if "job" in obj and isinstance(obj["job"], dict):
            job = obj["job"]
            res = job.get("result")
            if isinstance(res, dict):
                pub = res.get("public_url")
                if isinstance(pub, str) and pub.strip():
                    return pub.strip()
                for k in ("cdnurl", "download_url", "url", "file_path", "tg_link", "telegram_link"):
                    v = res.get(k)
                    if isinstance(v, str) and v.strip():
                        return v.strip()

        for k in ("public_url", "cdnurl", "download_url", "url", "file_path", "tg_link", "telegram_link", "message_link"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

        for wrap in ("result", "results", "data", "items", "payload", "message"):
            v = obj.get(wrap)
            if v:
                c = _extract_candidate(v)
                if c:
                    return c
    return None


def _looks_like_status_text(s: Optional[str]) -> bool:
    if not s:
        return False
    low = s.lower()
    return any(x in low for x in ("download started", "background", "jobstatus", "job_id", "processing", "queued"))


def _normalize_candidate_to_url(candidate: str) -> Optional[str]:
    if not candidate:
        return None
    c = candidate.strip()
    if c.startswith(("http://", "https://")):
        return c
    if c.startswith("/"):
        # ignore local fs paths
        if c.startswith("/root") or c.startswith("/home"):
            return None
        return f"{API_URL.rstrip('/')}{c}"
    return f"{API_URL.rstrip('/')}/{c.lstrip('/')}"


async def _download_from_cdn(cdn_url: str, out_path: str) -> Optional[str]:
    if not cdn_url:
        return None
    try:
        session = await get_http_session()
        LOGGER.info(f"[CDN] GET {cdn_url} -> {out_path}")
        async with session.get(cdn_url) as resp:
            if resp.status != 200:
                LOGGER.warning(f"[CDN] HTTP {resp.status} for {cdn_url}")
                return None
            async with aiofiles.open(out_path, "wb") as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    if not chunk:
                        break
                    await f.write(chunk)
        if os.path.exists(out_path):
            LOGGER.info(f"[CDN] Download complete: {out_path}")
            return out_path
        LOGGER.warning(f"[CDN] Finished but missing on disk: {out_path}")
        return None
    except Exception as e:
        LOGGER.error(f"[CDN] Exception downloading {cdn_url}: {e}", exc_info=True)
        return None


async def _download_from_telegram(tme_url: str) -> Optional[str]:
    if TG_APP is None or tg_errors is None:
        LOGGER.warning("[TG] Telegram client not available; cannot download t.me link.")
        return None

    m = TG_LINK_RE.match(tme_url)
    if not m:
        LOGGER.warning(f"[TG] Invalid Telegram URL: {tme_url}")
        return None

    if m.group(1):  # /c/<id>/<msg>
        channel_id = m.group(2)
        chat_id = int(f"-100{channel_id}")
        msg_id = int(tme_url.rstrip("/").split("/")[-1])
    else:
        chat_id = m.group(3)
        msg_id = int(m.group(4))

    try:
        dl_dir = _as_download_dir(DOWNLOAD_DIR)
        LOGGER.info(f"[TG] get_messages chat={chat_id} msg={msg_id}")
        msg = await TG_APP.get_messages(chat_id=chat_id, message_ids=msg_id)

        LOGGER.info(f"[TG] msg.download -> dir={dl_dir}")
        res = await msg.download(file_name=dl_dir)
        fixed = _resolve_if_dir(res)

        LOGGER.info(f"[TG] download result={res} fixed={fixed}")
        if fixed and Path(fixed).exists():
            return fixed
        return None

    except tg_errors.FloodWait as e:  # type: ignore[attr-defined]
        LOGGER.warning(f"[TG] FloodWait {e.value}s")
        await asyncio.sleep(e.value)
        return await _download_from_telegram(tme_url)
    except Exception as e:
        LOGGER.warning(f"[TG] Telegram download error: {e}")
        return None


async def v2_download(link: str, media_type: str) -> Optional[str]:
    """
    V2 primary:
      - youtube/v2/download (isVideo true/false)
      - poll youtube/jobStatus if job_id returned
      - download from CDN or Telegram if t.me
    """
    is_video = media_type == "video"
    vid = extract_video_id(link)

    # Normalize query param (use id if available)
    query = vid or link
    LOGGER.info(f"[V2] START media_type={media_type} query={query}")

    resp = await _v2_request_json("youtube/v2/download", {"query": query, "isVideo": str(is_video).lower()})
    if not resp:
        LOGGER.warning("[V2] v2 empty -> trying v1 compat")
        resp = await _v2_request_json("youtube/v1/download", {"query": query, "isVideo": str(is_video).lower()})
    if not resp:
        LOGGER.warning("[V2] No response from API")
        return None

    candidate = _extract_candidate(resp)
    LOGGER.info(f"[V2] candidate={candidate}")

    if candidate and _looks_like_status_text(candidate):
        candidate = None

    job_id = None
    if isinstance(resp, dict):
        job_id = resp.get("job_id") or resp.get("job")
        if isinstance(job_id, dict) and "id" in job_id:
            job_id = job_id.get("id")

    if job_id and not candidate:
        interval = JOB_POLL_INTERVAL
        LOGGER.info(f"[V2] job_id={job_id} polling jobStatus attempts={JOB_POLL_ATTEMPTS}")
        for i in range(1, JOB_POLL_ATTEMPTS + 1):
            await asyncio.sleep(interval)
            status = await _v2_request_json("youtube/jobStatus", {"job_id": str(job_id)})
            candidate = _extract_candidate(status) if status else None
            LOGGER.info(f"[V2] jobStatus poll {i}/{JOB_POLL_ATTEMPTS} candidate={candidate}")
            if candidate and _looks_like_status_text(candidate):
                candidate = None
            if candidate:
                break
            interval *= JOB_POLL_BACKOFF

    if not candidate:
        LOGGER.warning("[V2] No candidate URL after polling")
        return None

    # Telegram direct
    if candidate.startswith(("http://t.me", "https://t.me")):
        return await _download_from_telegram(candidate)

    normalized = _normalize_candidate_to_url(candidate)
    LOGGER.info(f"[V2] normalized={normalized}")
    if not normalized:
        return None

    # choose extension for naming
    ext = "mp4" if is_video else "webm"
    out_path = os.path.join(DOWNLOAD_DIR, f"{vid or uuid.uuid4().hex[:8]}.{ext}")
    return await _download_from_cdn(normalized, out_path)


# -----------------------
# Fallen /track (audio fallback)
# -----------------------
def _fallen_headers() -> Dict[str, str]:
    return {"X-API-Key": API_KEY2 or "", "Accept": "application/json"}


async def fallen_get_track(link: str) -> Optional[Dict]:
    if not API_URL2 or not API_KEY2:
        LOGGER.warning("[FALLBACK] API_URL2/API_KEY2 not configured.")
        return None

    base = API_URL2.rstrip("/")
    encoded = urllib.parse.quote(link, safe="")
    endpoint = f"{base}/track?url={encoded}"

    # exactly 1 attempt (as requested)
    for attempt in range(1, FALLBACK_RETRIES + 1):
        try:
            session = await get_http_session()
            LOGGER.info(f"[FALLBACK] REQUEST attempt={attempt}/1 endpoint={endpoint}")
            async with session.get(endpoint, headers=_fallen_headers()) as resp:
                data = await resp.json(content_type=None)
                LOGGER.info(f"[FALLBACK] RESPONSE status={resp.status} data={str(data)[:250]}")
                if resp.status == 200 and isinstance(data, dict):
                    return data
                return None
        except Exception as e:
            LOGGER.warning(f"[FALLBACK] track request error: {e}")
            return None
    return None


async def fallen_download_audio(link: str) -> Optional[str]:
    LOGGER.info(f"[FALLBACK] START download_audio url={link}")
    track = await fallen_get_track(link)
    if not track:
        LOGGER.warning("[FALLBACK] No track metadata.")
        return None

    cdn_url = track.get("cdnurl") or track.get("url") or track.get("download_url")
    if not cdn_url:
        LOGGER.warning("[FALLBACK] Missing cdnurl/url in response.")
        return None

    cdn_url = str(cdn_url).strip()
    LOGGER.info(f"[FALLBACK] resolved_download_url={cdn_url}")

    tg_match = re.match(r"https?://t\.me/([^/]+)/(\d+)", cdn_url)
    if tg_match:
        return await _download_from_telegram(cdn_url)

    out_path = os.path.join(DOWNLOAD_DIR, f"fallen_{uuid.uuid4().hex[:8]}.mp3")
    return await _download_from_cdn(cdn_url, out_path)


# -----------------------
# yt-dlp (final fallback)
# -----------------------
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
        return "https://" + s if not s.startswith(("http://", "https://")) else s
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


# -----------------------
# Main entry
# -----------------------
async def yt_dlp_download(link: str, type: str, title: str = "") -> Optional[str]:
    """
    Updated flow:

    - AUDIO:
        1) V2 API (10 retries)
        2) Fallen /track fallback (1 try)  [API_URL2/API_KEY2]
        3) yt-dlp

    - VIDEO:
        1) V2 API (10 retries)
        2) yt-dlp
    """
    loop = asyncio.get_running_loop()
    vid = extract_video_id(link)
    cache_media_type = "audio" if type == "audio" else "video"

    # cache
    if vid and (cached := find_cached_file(vid, cache_media_type)):
        if title:
            LOGGER.info(f"Track '{title}' - Served from cache ({cache_media_type})")
        return cached

    dedup_id = vid or normalize_ytdlp_link(link)
    key = f"{type}:{dedup_id}"

    if type == "audio":

        async def run():
            # 1) V2 primary
            v2_path = await v2_download(link, media_type="audio")
            if v2_path and os.path.exists(v2_path):
                log_download_source(title or "Unknown", "V2 API")
                return v2_path

            # 2) Fallen fallback (final try before yt-dlp)
            fb_path = await fallen_download_audio(link)
            if fb_path and os.path.exists(fb_path):
                log_download_source(title or "Unknown", "Fallen Fallback")
                return fb_path

            # 3) yt-dlp
            primary_fmt = "bestaudio[ext=webm][acodec=opus]"
            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(None, download_with_ytdlp_sync, link, primary_fmt)
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                log_download_source(title or "Unknown", f"yt-dlp ({primary_fmt})")
                return ytdlp_result

            fallback_fmt = "bestaudio/best"
            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(None, download_with_ytdlp_sync, link, fallback_fmt)
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                log_download_source(title or "Unknown", f"yt-dlp ({fallback_fmt})")
                return ytdlp_result

            return None

        return await deduplicate_download(key, run)

    if type == "video":

        async def run():
            # 1) V2 primary
            v2_path = await v2_download(link, media_type="video")
            if v2_path and os.path.exists(v2_path):
                log_download_source(title or "Unknown", "V2 API")
                return v2_path

            # 2) yt-dlp video fallback
            primary_fmt = "(bestvideo[height<=?720][width<=?1280][ext=mp4])+(bestaudio/best)"
            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(None, download_with_ytdlp_sync, link, primary_fmt)
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                log_download_source(title or "Unknown", f"yt-dlp ({primary_fmt})")
                return ytdlp_result

            fallback_fmt = "best[ext=mp4]/best"
            ytdlp_result = await run_with_semaphore(
                loop.run_in_executor(None, download_with_ytdlp_sync, link, fallback_fmt)
            )
            if ytdlp_result and os.path.exists(ytdlp_result):
                log_download_source(title or "Unknown", f"yt-dlp ({fallback_fmt})")
                return ytdlp_result

            return None

        return await deduplicate_download(key, run)

    return None
