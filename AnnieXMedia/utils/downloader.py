# Authored By Certified Coders © 2025
import asyncio
import os
import re
import uuid
from pathlib import Path
from typing import Dict, Optional, Any
from urllib.parse import urlparse

import aiofiles
import aiohttp
from aiohttp import TCPConnector

from motor.motor_asyncio import AsyncIOMotorClient

from AnnieXMedia.core.dir import DOWNLOAD_DIR
from AnnieXMedia.utils.tuning import CHUNK_SIZE
from config import API_KEY, API_URL, MEDIA_CHANNEL_ID, DB_URI
from AnnieXMedia.logging import LOGGER as _LOGGER

LOGGER = _LOGGER(__name__)

# Optional Telegram client for t.me links / Media DB channel downloads
try:
    from AnnieXMedia import app as TG_APP
except Exception:
    TG_APP = None

try:
    from pyrogram import errors as tg_errors
except Exception:
    tg_errors = None

# Optional DB name / collection overrides (safe defaults)
try:
    from config import MEDIA_DB_NAME
except Exception:
    MEDIA_DB_NAME = "arcapi"

try:
    from config import MEDIA_COLLECTION_NAME
except Exception:
    MEDIA_COLLECTION_NAME = "medias"

# Single shared mongo client
_MONGO_CLIENT: Optional[AsyncIOMotorClient] = None


def _get_media_collection():
    """
    Returns the motor collection used for media index.
    DB and collection can be overridden by config:
      - DB_URI (required)
      - MEDIA_DB_NAME (optional, default 'arcapi')
      - MEDIA_COLLECTION_NAME (optional, default 'medias')
    """
    global _MONGO_CLIENT
    if not DB_URI:
        return None
    if _MONGO_CLIENT is None:
        _MONGO_CLIENT = AsyncIOMotorClient(DB_URI)
    db = _MONGO_CLIENT[MEDIA_DB_NAME]
    return db[MEDIA_COLLECTION_NAME]


async def is_media(track_id: str, isVideo: bool = False) -> bool:
    col = _get_media_collection()
    if col is None:
        return False
    doc = await col.find_one({"track_id": track_id, "isVideo": isVideo}, {"_id": 1})
    return bool(doc)


async def get_media_id(track_id: str, isVideo: bool = False) -> Optional[int]:
    col = _get_media_collection()
    if col is None:
        return None
    doc = await col.find_one(
        {"track_id": track_id, "isVideo": isVideo},
        {"message_id": 1},
    )
    if not doc:
        return None
    mid = doc.get("message_id")
    return int(mid) if mid is not None else None


# -----------------------
# STATS
# -----------------------
DOWNLOAD_STATS: Dict[str, int] = {
    "total": 0,
    "success": 0,
    "failed": 0,
    "success_audio": 0,
    "success_video": 0,
    "failed_audio": 0,
    "failed_video": 0,
    "hard_fail_401": 0,
    "hard_fail_403": 0,
    "api_fail_other_4xx": 0,
    "api_fail_5xx": 0,
    "network_fail": 0,
    "timeout_fail": 0,
    "no_candidate": 0,
    "tg_fail": 0,
    "cdn_fail": 0,
    "hard_cycle_retries": 0,
    # Media DB stats
    "media_db_hit": 0,
    "media_db_miss": 0,
    "media_db_fail": 0,
}

def get_download_stats() -> Dict[str, int]:
    return dict(DOWNLOAD_STATS)

def reset_download_stats() -> None:
    for k in list(DOWNLOAD_STATS.keys()):
        DOWNLOAD_STATS[k] = 0

def _inc(key: str, n: int = 1) -> None:
    DOWNLOAD_STATS[key] = DOWNLOAD_STATS.get(key, 0) + n


# -----------------------
# V2 SETTINGS
# -----------------------
V2_HTTP_RETRIES = 5
V2_DOWNLOAD_CYCLES = 5
HARD_RETRY_WAIT = 3

JOB_POLL_ATTEMPTS = 10
JOB_POLL_INTERVAL = 2.0
JOB_POLL_BACKOFF = 1.2

NO_CANDIDATE_WAIT = 4

CDN_RETRIES = 5
CDN_RETRY_DELAY = 2

# -----------------------
# COMPLETE CYCLE TIMEOUT
# -----------------------
# Whole flow timeout: MediaDB attempt + V2 attempt combined
CYCLE_TIMEOUT_SEC = 120

# -----------------------
# Regex / helpers
# -----------------------
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")
YOUTUBE_ID_IN_URL_RE = re.compile(r"""(?x)(?:v=|\/)([A-Za-z0-9_-]{11})|youtu\.be\/([A-Za-z0-9_-]{11})""")
TG_LINK_RE = re.compile(r"https?://t\.me/(?:(c)/(\d+)/(\d+)|([^/]+)/(\d+))", re.IGNORECASE)

_inflight: Dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


class V2HardAPIError(Exception):
    def __init__(self, status: int, body_preview: str = ""):
        super().__init__(f"Hard API error status={status}")
        self.status = status
        self.body_preview = body_preview[:200]


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
    p = str(Path(path).resolve())
    if not p.endswith(os.sep):
        p += os.sep
    return p


def _resolve_if_dir(download_result: str) -> Optional[str]:
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


def _extract_candidate(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        return s if s else None
    if isinstance(obj, list) and obj:
        return _extract_candidate(obj[0])
    if isinstance(obj, dict):
        job = obj.get("job")
        if isinstance(job, dict):
            res = job.get("result")
            if isinstance(res, dict):
                pub = res.get("public_url")
                if isinstance(pub, str) and pub.strip():
                    return pub.strip()
                for k in ("cdnurl", "download_url", "url", "tg_link", "telegram_link", "message_link"):
                    v = res.get(k)
                    if isinstance(v, str) and v.strip():
                        return v.strip()

        for k in ("public_url", "cdnurl", "download_url", "url", "tg_link", "telegram_link", "message_link"):
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

    for attempt in range(1, CDN_RETRIES + 1):
        try:
            session = await get_http_session()
            async with session.get(cdn_url) as resp:
                if resp.status != 200:
                    if resp.status in (429, 500, 502, 503, 504) and attempt < CDN_RETRIES:
                        await asyncio.sleep(CDN_RETRY_DELAY)
                        continue
                    return None

                async with aiofiles.open(out_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        if not chunk:
                            break
                        await f.write(chunk)

            return out_path if os.path.exists(out_path) else None

        except asyncio.TimeoutError:
            _inc("timeout_fail")
            if attempt < CDN_RETRIES:
                await asyncio.sleep(CDN_RETRY_DELAY)
                continue
            return None
        except aiohttp.ClientError:
            _inc("network_fail")
            if attempt < CDN_RETRIES:
                await asyncio.sleep(CDN_RETRY_DELAY)
                continue
            return None
        except Exception:
            return None

    return None


async def _download_from_telegram(tme_url: str) -> Optional[str]:
    if TG_APP is None or tg_errors is None:
        return None

    m = TG_LINK_RE.match(tme_url)
    if not m:
        return None

    if m.group(1):  # /c/<id>/<msg>
        channel_id = m.group(2)
        chat_id = int(f"-100{channel_id}")
        msg_id = int(tme_url.rstrip("/").split("/")[-1])
    else:
        chat_id = m.group(4)
        msg_id = int(m.group(5))

    try:
        dl_dir = _as_download_dir(DOWNLOAD_DIR)
        msg = await TG_APP.get_messages(chat_id=chat_id, message_ids=msg_id)
        res = await msg.download(file_name=dl_dir)
        fixed = _resolve_if_dir(res)
        if fixed and Path(fixed).exists():
            return fixed
        return None
    except Exception:
        return None


# -----------------------
# MEDIA DB FETCH
# -----------------------
async def _download_from_media_db(track_id: str, is_video: bool) -> Optional[str]:
    """
    1) check is_media(track_id, isVideo)
    2) get_media_id(...)
    3) download that message from MEDIA_CHANNEL_ID
    """
    if not track_id:
        return None
    if TG_APP is None:
        return None
    if not MEDIA_CHANNEL_ID:
        return None
    if not DB_URI:
        # DB not configured
        return None

    ext = "mp4" if is_video else "mp3"

    # Support multiple key styles (some DBs store track_id with ext)
    keys_to_try = [
        f"{track_id}.{ext}",
        track_id,
        f"{track_id}_{'v' if is_video else 'a'}",
        f"{track_id}_{'v' if is_video else 'a'}.{ext}",
    ]

    try:
        msg_id: Optional[int] = None
        for k in keys_to_try:
            if await is_media(k, isVideo=is_video):
                msg_id = await get_media_id(k, isVideo=is_video)
                break

        if not msg_id:
            _inc("media_db_miss")
            return None

        _inc("media_db_hit")

        dl_dir = _as_download_dir(DOWNLOAD_DIR)
        out_path = os.path.join(dl_dir, f"{track_id}.{ext}")

        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            return out_path

        msg = await TG_APP.get_messages(chat_id=int(MEDIA_CHANNEL_ID), message_ids=int(msg_id))
        if not msg:
            _inc("media_db_fail")
            return None

        res = await msg.download(file_name=dl_dir)
        fixed = _resolve_if_dir(res)
        if not fixed or not os.path.exists(fixed) or os.path.getsize(fixed) <= 0:
            _inc("media_db_fail")
            return None

        # Normalize to {track_id}.{ext}
        try:
            if fixed != out_path:
                os.replace(fixed, out_path)
                fixed = out_path
        except Exception:
            pass

        if os.path.exists(fixed) and os.path.getsize(fixed) > 0:
            return fixed

        _inc("media_db_fail")
        return None

    except Exception:
        _inc("media_db_fail")
        return None


async def _v2_request_json(endpoint: str, params: Dict[str, Any]) -> Optional[Any]:
    if not API_URL or not API_KEY:
        return None

    base = API_URL.rstrip("/")
    url = f"{base}/{endpoint.lstrip('/')}"
    if "api_key" not in params:
        params["api_key"] = API_KEY

    for attempt in range(1, V2_HTTP_RETRIES + 1):
        try:
            session = await get_http_session()
            async with session.get(
                url,
                params=params,
                headers={"X-API-Key": API_KEY, "Accept": "application/json"},
            ) as resp:
                text = await resp.text()
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = None

                if 200 <= resp.status < 300:
                    return data

                if resp.status in (401, 403):
                    if resp.status == 401:
                        _inc("hard_fail_401")
                    else:
                        _inc("hard_fail_403")
                    raise V2HardAPIError(resp.status, text)

                if 500 <= resp.status <= 599:
                    _inc("api_fail_5xx")
                else:
                    _inc("api_fail_other_4xx")
                    return None

        except V2HardAPIError:
            raise
        except asyncio.TimeoutError:
            _inc("timeout_fail")
        except aiohttp.ClientError:
            _inc("network_fail")
        except Exception:
            _inc("network_fail")

        if attempt < V2_HTTP_RETRIES:
            await asyncio.sleep(1)

    return None


async def v2_download(link: str, media_type: str) -> Optional[str]:
    is_video = media_type == "video"
    vid = extract_video_id(link)
    query = vid or link

    for cycle in range(1, V2_DOWNLOAD_CYCLES + 1):
        try:
            resp = await _v2_request_json(
                "youtube/v2/download",
                {"query": query, "isVideo": str(is_video).lower()},
            )
        except V2HardAPIError:
            _inc("hard_cycle_retries")
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(HARD_RETRY_WAIT)
                continue
            return None

        if not resp:
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(1)
                continue
            return None

        candidate = _extract_candidate(resp)
        if candidate and _looks_like_status_text(candidate):
            candidate = None

        job_id = None
        if isinstance(resp, dict):
            job_id = resp.get("job_id") or resp.get("job")
            if isinstance(job_id, dict) and "id" in job_id:
                job_id = job_id.get("id")

        if job_id and not candidate:
            interval = JOB_POLL_INTERVAL
            for _ in range(1, JOB_POLL_ATTEMPTS + 1):
                await asyncio.sleep(interval)
                try:
                    status = await _v2_request_json("youtube/jobStatus", {"job_id": str(job_id)})
                except V2HardAPIError:
                    _inc("hard_cycle_retries")
                    candidate = None
                    break

                candidate = _extract_candidate(status) if status else None
                if candidate and _looks_like_status_text(candidate):
                    candidate = None
                if candidate:
                    break
                interval *= JOB_POLL_BACKOFF

        if not candidate:
            _inc("no_candidate")
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(NO_CANDIDATE_WAIT)
                continue
            return None

        if candidate.startswith(("http://t.me", "https://t.me")):
            path = await _download_from_telegram(candidate)
            if not path:
                _inc("tg_fail")
                if cycle < V2_DOWNLOAD_CYCLES:
                    await asyncio.sleep(2)
                    continue
            return path

        normalized = _normalize_candidate_to_url(candidate)
        if not normalized:
            _inc("no_candidate")
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(NO_CANDIDATE_WAIT)
                continue
            return None

        ext = _guess_ext_from_url(normalized, is_video=is_video)
        base_name = vid if vid else uuid.uuid4().hex[:10]
        out_path = os.path.join(DOWNLOAD_DIR, f"{base_name}.{ext}")

        if os.path.exists(out_path):
            return out_path

        path = await _download_from_cdn(normalized, out_path)
        if not path:
            _inc("cdn_fail")
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(2)
                continue
        return path

    return None


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


async def media_download(link: str, type: str, title: str = "") -> Optional[str]:
    """
    FINAL FLOW (as requested):
      1) Try Media DB first (Mongo via DB_URI + TG Media Channel)
      2) If not found -> try V2
      3) Whole cycle timeout = 120s
      4) If timeout -> return None
    """
    _inc("total")

    vid = extract_video_id(link)
    dedup_id = vid or link.strip()
    key = f"{type}:{dedup_id}"

    async def _cycle():
        is_video = (type == "video")
        is_audio = (type == "audio")

        # 1) Media DB first (only if we can extract a track id)
        if vid:
            db_path = await _download_from_media_db(vid, is_video=is_video)
            if db_path and os.path.exists(db_path):
                _inc("success")
                if is_audio:
                    _inc("success_audio")
                else:
                    _inc("success_video")
                LOGGER.info(f"MEDIA_DB_DOWNLOAD_SUCCESS type={type} title='{title or 'Unknown'}' path='{db_path}'")
                return db_path

        # 2) V2 fallback
        v2_path = await v2_download(link, media_type=("video" if is_video else "audio"))
        if v2_path and os.path.exists(v2_path):
            _inc("success")
            if is_audio:
                _inc("success_audio")
            else:
                _inc("success_video")
            LOGGER.info(f"V2_DOWNLOAD_SUCCESS type={type} title='{title or 'Unknown'}' path='{v2_path}'")
            return v2_path

        _inc("failed")
        if is_audio:
            _inc("failed_audio")
        else:
            _inc("failed_video")
        LOGGER.warning(f"DOWNLOAD_FAILED type={type} title='{title or 'Unknown'}' link='{link}' reason='not_found'")
        return None

    async def run():
        try:
            return await asyncio.wait_for(_cycle(), timeout=CYCLE_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            # As you requested: timeout => return None
            _inc("timeout_fail")
            _inc("failed")
            if type == "audio":
                _inc("failed_audio")
            elif type == "video":
                _inc("failed_video")
            LOGGER.warning(
                f"DOWNLOAD_TIMEOUT type={type} title='{title or 'Unknown'}' link='{link}' timeout={CYCLE_TIMEOUT_SEC}s"
            )
            return None

    return await deduplicate_download(key, run)
