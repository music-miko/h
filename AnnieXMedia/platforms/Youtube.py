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
CACHE_FILE = "youtube_cache.json"

# === Concurrency Control (Fix for Too Many Open Files) ===
# Limits concurrent subprocesses and HTTP requests to prevent FD exhaustion.
_API_SEMAPHORE = asyncio.Semaphore(6)


# === Helpers ===
def _cookiefile_path() -> Optional[str]:
    """
    Return COOKIE_PATH only if it exists, non-empty and looks like Netscape format.
    Avoids yt-dlp complaining about invalid cookies.txt.
    """
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
    # Force no cache directory for all subprocess calls to save Disk I/O
    cmd_args = list(args)
    if "yt-dlp" in cmd_args[0]:
        cmd_args.insert(1, "--no-cache-dir")

    # FIX: Protect subprocess creation with semaphore
    async with _API_SEMAPHORE:
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
    """Python port of the Go 'dig' function."""
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


def _parse_duration(s: str) -> int:
    """Python port of the Go 'parseDuration' function."""
    if not s: return 0
    parts = s.split(":")
    total = 0
    mul = 1
    for part in reversed(parts):
        try:
            val = int("".join(filter(str.isdigit, part)))
            total += val * mul
            mul *= 60
        except ValueError:
            pass
    return total


@capture_internal_err
async def cached_youtube_search(query: str) -> List[Dict]:
    """
    Stateless wrapper. No longer uses local RAM cache.
    Always performs a fresh VideosSearch.
    """
    try:
        # FIX: Protect search request
        async with _API_SEMAPHORE:
            data = await VideosSearch(query, limit=1).next()
        result = data.get("result", [])
        return result
    except Exception:
        return []


# === Main Class ===
class YouTubeAPI:
    def __init__(self) -> None:
        self.base_url = "https://www.youtube.com/watch?v="
        self.playlist_url = "https://youtube.com/playlist?list="
        self._url_pattern = re.compile(r"(?:youtube\.com|youtu\.be)")
        
        # Initialize Persistent JSON Cache
        self.cache_path = CACHE_FILE
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        """Loads the JSON cache from disk into memory."""
        if not os.path.exists(self.cache_path):
            return {}
        try:
            with open(self.cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}

    def _save_to_cache(self, key: str, data: Dict) -> None:
        """Updates memory cache and saves to disk."""
        self.cache[key] = data
        try:
            # We write synchronously here to ensure persistence
            with open(self.cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _prepare_link(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        if isinstance(videoid, str) and videoid.strip():
            link = self.base_url + videoid.strip()

        link = link.strip()

        if "youtu.be" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]
        elif "youtube.com/shorts/" in link or "youtube.com/live/" in link:
            link = self.base_url + link.split("/")[-1].split("?")[0]

        return link.split("&")[0]

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

    async def _ensure_watch_url(self, maybe_query_or_url: str) -> Optional[str]:
        prepared = self._prepare_link(maybe_query_or_url)
        if prepared.startswith("http"):
            return prepared
        info = await self._fetch_video_info(prepared)
        if info and info.get("id"):
             return self.base_url + info["id"]
        return None

    # === Internal Search Methods ===
    def _recursive_parse(self, node: Any, limit: int = 1) -> List[Dict]:
        """Recursive parser ported from Go's 'parseResults'."""
        tracks = []
        
        # If list, iterate items
        if isinstance(node, list):
            for item in node:
                tracks.extend(self._recursive_parse(item, limit))
                if len(tracks) >= limit:
                    break
            return tracks

        # If dict, check for videoRenderer
        if isinstance(node, dict):
            vr = _dig(node, "videoRenderer")
            if vr:
                # Check for LIVE badge
                is_live = False
                badges = vr.get("badges", [])
                for badge in badges:
                    style = _dig(badge, "metadataBadgeRenderer", "style")
                    if style == "BADGE_STYLE_TYPE_LIVE_NOW":
                        is_live = True
                        break
                
                # Extract Data
                vid_id = vr.get("videoId")
                title = _dig(vr, "title", "runs", 0, "text")
                duration_text = _dig(vr, "lengthText", "simpleText")

                if not is_live and vid_id and title and duration_text:
                    thumb = _dig(vr, "thumbnail", "thumbnails", 0, "url")
                    
                    return [{
                        "id": vid_id,
                        "title": title,
                        "duration": duration_text,
                        "thumbnail": thumb,
                        "thumbnails": [{"url": thumb}],
                        "link": f"https://www.youtube.com/watch?v={vid_id}"
                    }]

            # Recurse into values if not a videoRenderer
            for value in node.values():
                tracks.extend(self._recursive_parse(value, limit))
                if len(tracks) >= limit:
                    break
                    
        return tracks

    async def _raw_youtube_search(self, query: str) -> Optional[Dict]:
        """Manual implementation of InnerTube search (Go port)."""
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
            # FIX: Protect HTTP request
            async with _API_SEMAPHORE:
                async with aiohttp.ClientSession() as session:
                    async with session.post(endpoint, json=payload) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json()
        except Exception:
            return None

        # Start parsing from the specific root mentioned in Go code
        root = _dig(
            data,
            "contents",
            "twoColumnSearchResultsRenderer",
            "primaryContents",
            "sectionListRenderer",
            "contents",
        )
        
        results = self._recursive_parse(root, limit=1)
        return results[0] if results else None

    # === Metadata Fetching ===
    @capture_internal_err
    async def _fetch_video_info(self, query: str, *, use_cache: bool = True) -> Optional[Dict]:
        """
        Robust fetcher with Caching and Prioritization.
        Priority:
        1. JSON Cache (Disk/Memory)
        2. Raw InnerTube Search (Fastest) - For both URL and Query
        3. yt-dlp (Fallback)
        """
        q = self._prepare_link(query)
        is_link = q.startswith("http")

        # 1. Check Cache First (if enabled)
        if use_cache and q in self.cache:
            return self.cache[q]

        # 2. Try Raw Search (Prioritized for speed on BOTH types)
        info = await self._raw_youtube_search(q)
        
        # 3. Fallback: yt-dlp
        if not info:
            try:
                search_query = q if is_link else f"ytsearch1:{q}"
                # _exec_proc is protected by semaphore
                stdout, _ = await _exec_proc(
                    "yt-dlp",
                    *(_cookies_args()),
                    "--dump-json",
                    "--no-warnings",
                    "--ignore-errors",
                    search_query
                )

                if stdout:
                    raw_info = json.loads(stdout.decode())
                    if "entries" in raw_info:
                        raw_info = raw_info["entries"][0] if raw_info["entries"] else {}

                    if raw_info and raw_info.get("id"):
                        info = {
                            "id": raw_info.get("id"),
                            "title": raw_info.get("title"),
                            "duration": raw_info.get("duration_string") or str(raw_info.get("duration", 0)),
                            "thumbnails": [{"url": raw_info.get("thumbnail", "")}],
                            "thumbnail": raw_info.get("thumbnail", ""),
                            "link": raw_info.get("webpage_url", q)
                        }
            except Exception:
                pass

        # 4. Save to Cache if successful
        if info:
            self._save_to_cache(q, info)
            
        return info

    @capture_internal_err
    async def is_live(self, link: str) -> bool:
        """Checks if a video is live. Uses yt-dlp for reliable detection."""
        prepared = self._prepare_link(link)
        # _exec_proc is protected by semaphore
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
        
        # Attempt fetch
        info = await self._fetch_video_info(prepared_link)

        # Fallback to defaults only if EVERYTHING failed
        if not info:
            vid_id = ""
            if "watch?v=" in prepared_link:
                try:
                    vid_id = prepared_link.split("v=")[-1].split("&")[0]
                except Exception:
                    pass
            elif "youtu.be/" in prepared_link:
                 try:
                    vid_id = prepared_link.split("youtu.be/")[-1].split("?")[0]
                 except Exception:
                    pass

            if vid_id or prepared_link.startswith("http"):
                 return "Unknown", "00:00", 0, "", vid_id or ""
            
            raise ValueError(f"Video not found for: {prepared_link}")

        # === Safe Data Extraction ===
        title = info.get("title") or "Unknown"
        dt = info.get("duration") or "00:00"
        
        try:
            ds = int(time_to_seconds(dt)) if dt else 0
        except Exception:
            ds = 0

        thumbs = info.get("thumbnails", [{}])
        thumb_url = info.get("thumbnail") or (thumbs[-1].get("url") if thumbs else "") or ""
        thumb = thumb_url.split("?")[0]

        return title, dt, ds, thumb, info.get("id", "")

    @capture_internal_err
    async def title(self, link: str, videoid: Union[str, bool, None] = None) -> str:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return info.get("title", "") if info else ""

    @capture_internal_err
    async def duration(self, link: str, videoid: Union[str, bool, None] = None) -> Optional[str]:
        info = await self._fetch_video_info(self._prepare_link(link, videoid))
        return info.get("duration") if info else None

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
        info = await self._fetch_video_info(prepared_link)
        
        if not info:
            vid_id = ""
            if "watch?v=" in prepared_link:
                try:
                    vid_id = prepared_link.split("v=")[-1].split("&")[0]
                except Exception:
                    pass
            
            if vid_id or prepared_link.startswith("http"):
                info = {
                    "id": vid_id,
                    "title": "Unknown",
                    "duration": "00:00",
                    "thumbnail": "",
                    "link": prepared_link
                }
            else:
                 raise ValueError(f"Could not fetch info for '{prepared_link}' via any method.")

        thumbs = info.get("thumbnails", [{}])
        thumb_url = info.get("thumbnail") or (thumbs[-1].get("url") if thumbs else "") or ""
        thumb = thumb_url.split("?")[0]

        details = {
            "title": info.get("title") or "Unknown",
            "link": info.get("link") or prepared_link,
            "vidid": info.get("id", ""),
            "duration_min": info.get("duration") or "00:00",
            "thumb": thumb,
        }
        return details, info.get("id", "")

    # === Media & Formats ===
    @capture_internal_err
    async def video(self, link: str, videoid: Union[str, bool, None] = None) -> Tuple[int, str]:
        link = self._prepare_link(link, videoid)
        # _exec_proc is protected
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
            # FIX: Protect Playlist fetch
            async with _API_SEMAPHORE:
                plist = await Playlist.get(link)
            items = [video.get("id") for video in plist.get("videos", [])[:limit] if video.get("id")]
            if items:
                return items
        except Exception:
            pass

        # Fallback uses protected _exec_proc
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
    async def formats(
        self, link: str, videoid: Union[str, bool, None] = None
    ) -> Tuple[List[Dict], str]:
        link = self._prepare_link(link, videoid)
        opts = {
            "quiet": True,
            "no_cache_dir": True, 
        }
        if cf := _cookiefile_path():
            opts["cookiefile"] = cf

        out: List[Dict] = []
        try:
            # FIX: Protect blocking call with semaphore and executor
            async with _API_SEMAPHORE:
                loop = asyncio.get_running_loop()
                info = await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(opts).extract_info(link, download=False))
            
            for fmt in info.get("formats", []):
                if "dash" in str(fmt.get("format", "")).lower():
                    continue
                if not any(k in fmt for k in ("filesize", "filesize_approx")):
                    continue
                if not all(k in fmt for k in ("format", "format_id", "ext", "format_note")):
                    continue
                size = fmt.get("filesize") or fmt.get("filesize_approx")
                if not size:
                    continue
                out.append(
                    {
                        "format": fmt["format"],
                        "filesize": size,
                        "format_id": fmt["format_id"],
                        "ext": fmt["ext"],
                        "format_note": fmt["format_note"],
                        "yturl": link,
                    }
                )
        except Exception:
            pass

        return out, link

    @capture_internal_err
    async def slider(
        self, link: str, query_type: int, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], str, str]:
        try:
            # FIX: Protect VideosSearch
            async with _API_SEMAPHORE:
                data = await VideosSearch(self._prepare_link(link, videoid), limit=10).next()
            results = data.get("result", [])
        except Exception:
            query = self._prepare_link(link, videoid)
            # Fallback uses protected _exec_proc
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
            raise IndexError(
                f"Query type index {query_type} out of range (found {len(results)} results)"
            )
        r = results[query_type]
        return (
            r.get("title", ""),
            r.get("duration"),
            r.get("thumbnails", [{}])[-1].get("url", "").split("?")[0],
            r.get("id", ""),
        )

    # === Download Manager ===
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
