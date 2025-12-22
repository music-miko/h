# AnnieXMedia/utils/downloader.py
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
from AnnieXMedia.logging import LOGGER as _LOGGER
from config import API_KEY, API_URL

LOGGER = _LOGGER(__name__)

# -----------------------
# Telegram (Pyrogram bot client)
# -----------------------
try:
    from AnnieXMedia import app as TG_APP
except Exception:
    TG_APP = None

# -----------------------
# Media DB config (required for Media DB fetch)
# -----------------------
try:
    from config import MEDIA_CHANNEL_ID  # should be int like -100123...
except Exception:
    MEDIA_CHANNEL_ID = None

# user said: will provide media fetching DB string in config -> use DB_URI
try:
    from config import DB_URI
except Exception:
    DB_URI = None

# Optional overrides if your db/collection names differ
try:
    from config import MEDIA_DB_NAME
except Exception:
    MEDIA_DB_NAME = "arcapi"

try:
    from config import MEDIA_COLLECTION_NAME
except Exception:
    MEDIA_COLLECTION_NAME = "medias"

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

# Whole flow timeout: MediaDB attempt + V2 attempt
CYCLE_TIMEOUT_SEC = 120


# -----------------------
# Regex / helpers
# -----------------------
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")
YOUTUBE_ID_IN_URL_RE = re.compile(r"""(?x)(?:v=|\/)([A-Za-z0-9_-]{11})|youtu\.be\/([A-Za-z0-9_-]{11})""")

_inflight: Dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()

_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()

_MONGO_CLIENT: Optional[AsyncIOMotorClient] = None


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


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


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


def _get_media_collection():
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
    doc = await col.find_one({"track_id": track_id, "isVideo": isVideo}, {"message_id": 1})
    if not doc:
        return None
    mid = doc.get("message_id")
    if mid is None:
        return None
    try:
        return int(mid)
    except Exception:
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
        job = obj.get("job")
        if isinstance(job, dict):
            res = job.get("result")
            if isinstance(res, dict):
                for k in ("public_url", "cdnurl", "download_url", "url", "tg_link", "telegram_link", "message_link"):
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
        # ignore local paths
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

                _ensure_dir(str(Path(out_path).parent))
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
            _inc("network_fail")
            return None

    return None


# -----------------------
# MEDIA DB FETCH (FIXED + VERBOSE PRINTS)
# -----------------------
async def _download_from_media_db(track_id: str, is_video: bool) -> Optional[str]:
    """
    Uses:
      - MongoDB via DB_URI (collection medias)
      - Telegram channel MEDIA_CHANNEL_ID
    Downloads file to DOWNLOAD_DIR
    """
    if not track_id:
        print("[MEDIA_DB] ❌ track_id empty")
        return None

    if TG_APP is None:
        print("[MEDIA_DB] ❌ TG_APP not available (pyrogram app import failed)")
        return None

    if not DB_URI:
        print("[MEDIA_DB] ❌ DB_URI missing in config")
        return None

    if not MEDIA_CHANNEL_ID:
        print("[MEDIA_DB] ❌ MEDIA_CHANNEL_ID missing in config")
        return None

    try:
        ch_id = int(MEDIA_CHANNEL_ID)
    except Exception:
        print(f"[MEDIA_DB] ❌ MEDIA_CHANNEL_ID not int: {MEDIA_CHANNEL_ID!r}")
        return None

    ext = "mp4" if is_video else "mp3"

    # try several keys (your DB might store with ext)
    keys_to_try = [
        f"{track_id}.{ext}",
        track_id,
        f"{track_id}_{'v' if is_video else 'a'}",
        f"{track_id}_{'v' if is_video else 'a'}.{ext}",
    ]

    msg_id: Optional[int] = None
    used_key: Optional[str] = None

    try:
        for k in keys_to_try:
            if await is_media(k, isVideo=is_video):
                msg_id = await get_media_id(k, isVideo=is_video)
                used_key = k
                break

        if not msg_id:
            _inc("media_db_miss")
            print(f"[MEDIA_DB] ❌ MISS type={'video' if is_video else 'audio'} track_id={track_id}")
            return None

        _inc("media_db_hit")
        print(f"[MEDIA_DB] ✅ HIT type={'video' if is_video else 'audio'} track_id={track_id} key={used_key} msg_id={msg_id}")

        out_dir = str(Path(DOWNLOAD_DIR))
        _ensure_dir(out_dir)

        final_path = os.path.join(out_dir, f"{track_id}.{ext}")
        tmp_path = final_path + ".temp"

        # already present
        if os.path.exists(final_path) and os.path.getsize(final_path) > 0:
            print(f"[MEDIA_DB] ✅ ALREADY_ON_DISK -> {final_path}")
            return final_path

        # fetch message
        msg = await TG_APP.get_messages(ch_id, msg_id)
        if not msg:
            _inc("media_db_fail")
            print(f"[MEDIA_DB] ❌ FAIL: message not found in channel {ch_id} for msg_id={msg_id}")
            return None

        # download using app.download_media (more reliable)
        dl_res = await TG_APP.download_media(msg, file_name=tmp_path)
        fixed = _resolve_if_dir(dl_res) if isinstance(dl_res, str) else None

        if not fixed or not os.path.exists(fixed) or os.path.getsize(fixed) <= 0:
            _inc("media_db_fail")
            print(f"[MEDIA_DB] ❌ FAIL: download_media returned bad file: {fixed}")
            with contextlib.suppress(Exception):
                if tmp_path and os.path.exists(tmp_path):
                    os.remove(tmp_path)
            return None

        # normalize to final filename
        try:
            if fixed != final_path:
                os.replace(fixed, final_path)
        except Exception:
            # if rename fails, still use fixed
            final_path = fixed

        if os.path.exists(final_path) and os.path.getsize(final_path) > 0:
            print(f"[MEDIA_DB] ✅ DOWNLOADED -> {final_path}")
            return final_path

        _inc("media_db_fail")
        print(f"[MEDIA_DB] ❌ FAIL: file empty after download: {final_path}")
        return None

    except Exception as e:
        _inc("media_db_fail")
        print(f"[MEDIA_DB] ❌ EXCEPTION: {type(e).__name__}: {e}")
        return None


# -----------------------
# V2 API
# -----------------------
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
            async with session.get(url, params=params, headers={"X-API-Key": API_KEY, "Accept": "application/json"}) as resp:
                text = await resp.text()
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = None

                if 200 <= resp.status < 300:
                    return data

                if resp.status in (401, 403):
                    _inc("hard_fail_401" if resp.status == 401 else "hard_fail_403")
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
    is_video = (media_type == "video")
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

        normalized = _normalize_candidate_to_url(candidate)
        if not normalized:
            _inc("no_candidate")
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(NO_CANDIDATE_WAIT)
                continue
            return None

        ext = _guess_ext_from_url(normalized, is_video=is_video)
        base_name = vid if vid else uuid.uuid4().hex[:10]
        out_path = os.path.join(str(Path(DOWNLOAD_DIR)), f"{base_name}.{ext}")

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


# -----------------------
# Deduplicate in-flight requests
# -----------------------
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
# Public function
# -----------------------
async def media_download(link: str, type: str, title: str = "") -> Optional[str]:
    """
    FLOW:
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

        # 1) Media DB first
        if vid:
            db_path = await _download_from_media_db(vid, is_video=is_video)
            if db_path and os.path.exists(db_path):
                _inc("success")
                if is_audio:
                    _inc("success_audio")
                else:
                    _inc("success_video")
                LOGGER.info(f"MEDIA_DB_DOWNLOAD_SUCCESS type={type} title='{title or 'Unknown'}' path='{db_path}'")
                print(f"[DOWNLOADER] ✅ MEDIA_DB SUCCESS {type} -> {db_path}")
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
            print(f"[DOWNLOADER] ✅ V2 SUCCESS {type} -> {v2_path}")
            return v2_path

        _inc("failed")
        if is_audio:
            _inc("failed_audio")
        else:
            _inc("failed_video")
        LOGGER.warning(f"DOWNLOAD_FAILED type={type} title='{title or 'Unknown'}' link='{link}' reason='not_found'")
        print(f"[DOWNLOADER] ❌ FAILED {type} -> {link}")
        return None

    async def run():
        try:
            return await asyncio.wait_for(_cycle(), timeout=CYCLE_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            _inc("timeout_fail")
            _inc("failed")
            if type == "audio":
                _inc("failed_audio")
            elif type == "video":
                _inc("failed_video")
            LOGGER.warning(
                f"DOWNLOAD_TIMEOUT type={type} title='{title or 'Unknown'}' link='{link}' timeout={CYCLE_TIMEOUT_SEC}s"
            )
            print(f"[DOWNLOADER] ⏳ TIMEOUT {type} ({CYCLE_TIMEOUT_SEC}s) -> {link}")
            return None

    return await deduplicate_download(key, run)
