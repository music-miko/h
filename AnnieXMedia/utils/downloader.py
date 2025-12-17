# Authored By Certified Coders © 2025
import asyncio
import os
import re
import urllib.parse
import uuid
from pathlib import Path
from typing import Dict, Optional, Any
from urllib.parse import urlparse

import aiofiles
import aiohttp
from aiohttp import TCPConnector

from AnnieXMedia.core.dir import CACHE_DIR, DOWNLOAD_DIR
from AnnieXMedia.utils.cookie_handler import COOKIE_PATH as _COOKIES_FILE
from AnnieXMedia.utils.tuning import CHUNK_SIZE
from config import API_KEY, API_URL
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

# ✅ V2 only
V2_API_RETRIES = 1
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
# V2 API (ONLY)
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
                    data = None

                LOGGER.info(f"[V2] RESPONSE status={resp.status} url={url}")
                if 200 <= resp.status < 300:
                    return data

                LOGGER.warning(f"[V2] API error status={resp.status} data={str(data)[:300]}")
                # do not keep retrying on hard API errors (like 401/403); but retry network/timeouts above
                return None

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
                for k in ("cdnurl", "download_url", "url", "file_path", "tg_link", "telegram_link", "message_link"):
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


def _guess_ext_from_url(u: str, is_video: bool) -> str:
    try:
        path = urlparse(u).path
        ext = os.path.splitext(path)[1].lstrip(".").lower()
        if ext and ext.isalnum() and len(ext) <= 5:
            return ext
    except Exception:
        pass
    return "mp4" if is_video else "m4a"


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
    """
    Download Telegram-hosted media via Pyrogram (TG_APP).
    """
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
    V2 ONLY:
      - call youtube/v2/download (isVideo true/false)
      - poll youtube/jobStatus if job_id returned
      - download from CDN or Telegram if t.me
    """
    is_video = media_type == "video"
    vid = extract_video_id(link)

    query = vid or link
    LOGGER.info(f"[V2] START media_type={media_type} query={query}")

    resp = await _v2_request_json("youtube/v2/download", {"query": query, "isVideo": str(is_video).lower()})
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

    ext = _guess_ext_from_url(normalized, is_video=is_video)

    # Save as stable name if we have youtube id, else random
    base_name = vid if vid else uuid.uuid4().hex[:10]
    out_path = os.path.join(DOWNLOAD_DIR, f"{base_name}.{ext}")

    # If exists already, return it (avoid re-download)
    if os.path.exists(out_path):
        LOGGER.info(f"[V2] Served from disk cache: {out_path}")
        return out_path

    return await _download_from_cdn(normalized, out_path)


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
# Main entry (V2 only)
# -----------------------
async def media_download(link: str, type: str, title: str = "") -> Optional[str]:
    """
    V2 ONLY (no Fallen, no yt-dlp, no conversions).

    - AUDIO: V2 download -> (CDN or Telegram)
    - VIDEO: V2 download -> (CDN or Telegram)
    """
    vid = extract_video_id(link)
    cache_media_type = "audio" if type == "audio" else "video"

    # local cache
    if vid and (cached := find_cached_file(vid, cache_media_type)):
        if title:
            LOGGER.info(f"Track '{title}' - Served from cache ({cache_media_type})")
        return cached

    dedup_id = vid or link.strip()
    key = f"{type}:{dedup_id}"

    async def run():
        if type == "audio":
            path = await v2_download(link, media_type="audio")
            if path and os.path.exists(path):
                log_download_source(title or "Unknown", "V2 API")
                return path
            return None

        if type == "video":
            path = await v2_download(link, media_type="video")
            if path and os.path.exists(path):
                log_download_source(title or "Unknown", "V2 API")
                return path
            return None

        return None

    return await deduplicate_download(key, run)
