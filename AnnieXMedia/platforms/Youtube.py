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

import config
from AnnieXMedia.utils.cookie_handler import COOKIE_PATH
from AnnieXMedia.utils.downloader import media_download
from AnnieXMedia.utils.errors import capture_internal_err
from AnnieXMedia.utils.formatters import time_to_seconds
from AnnieXMedia.utils.tuning import YTDLP_TIMEOUT
from AnnieXMedia import LOGGER

# === Constants ===
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")
# CACHE_FILE removed

# === Concurrency Control ===
_API_SEMAPHORE = asyncio.Semaphore(25) 
_API_TIMEOUT = 10
VIDEO_PLAY_ALLOWED = False


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
    cmd_args = list(args)
    if "yt-dlp" in cmd_args[0]:
        cmd_args.insert(1, "--no-cache-dir")

    async with _API_SEMAPHORE:
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            return await asyncio.wait_for(proc.communicate(), timeout=YTDLP_TIMEOUT)
        except (asyncio.TimeoutError, Exception):
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


@capture_internal_err
async def cached_youtube_search(query: str) -> List[Dict]:
    # Cache logic removed
    try:
        async with _API_SEMAPHORE:
            data = await asyncio.wait_for(
                VideosSearch(query, limit=1).next(), 
                timeout=_API_TIMEOUT
            )
        result = data.get("result", [])
        return result
    except Exception as e:
        LOGGER(__name__).error(f"Search failed for query '{query}': {e}")
        return []


# === Main Class ===
class YouTubeAPI:
    def __init__(self) -> None:
        self.base_url = "https://www.youtube.com/watch?v="
        self.playlist_url = "https://youtube.com/playlist?list="
        self._url_pattern = re.compile(r"(?:youtube\.com|youtu\.be)")
        # Cache initialization removed

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

    # === Internal Search Methods (Optimized) ===
    def _recursive_parse(self, node: Any, limit: int = 1) -> List[Dict]:
        tracks = []
        if isinstance(node, list):
            for item in node:
                tracks.extend(self._recursive_parse(item, limit))
                if len(tracks) >= limit: break
            return tracks

        if isinstance(node, dict):
            vr = _dig(node, "videoRenderer")
            if vr:
                is_live = False
                badges = vr.get("badges", [])
                for badge in badges:
                    if _dig(badge, "metadataBadgeRenderer", "style") == "BADGE_STYLE_TYPE_LIVE_NOW":
                        is_live = True
                        break
                
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

            for value in node.values():
                tracks.extend(self._recursive_parse(value, limit))
                if len(tracks) >= limit: break
                    
        return tracks

    async def _raw_youtube_search(self, query: str) -> Optional[Dict]:
        endpoint = "https://www.youtube.com/youtubei/v1/search?key=AIzaSyCDCG4LCrByczUR8oYZKj-43dW-JqVIPHk"
        payload = {
            "context": {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": "2.20250101.01.00", 
                    "hl": "en", "gl": "IN",
                },
            },
            "query": query,
        }

        try:
            async with _API_SEMAPHORE:
                timeout = aiohttp.ClientTimeout(total=_API_TIMEOUT)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(endpoint, json=payload) as resp:
                        if resp.status != 200:
                            return None
                        data = await resp.json()
        except Exception as e:
            LOGGER(__name__).error(f"Raw search failed: {e}")
            return None

        root = _dig(data, "contents", "twoColumnSearchResultsRenderer", "primaryContents", "sectionListRenderer", "contents")
        results = self._recursive_parse(root, limit=1)
        return results[0] if results else None

    # === Metadata Fetching ===
    @capture_internal_err
    async def _fetch_video_info(self, query: str) -> Optional[Dict]:
        q = self._prepare_link(query)
        is_link = q.startswith("http")

        # Removed cache check

        info = await self._raw_youtube_search(q)
        
        if not info:
            try:
                search_query = q if is_link else f"ytsearch1:{q}"
                stdout, _ = await _exec_proc(
                    "yt-dlp", *(_cookies_args()), "--dump-json", "--no-warnings", "--ignore-errors", search_query
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
            except Exception as e:
                LOGGER(__name__).error(f"yt-dlp fetch failed: {e}")
                pass

        # Removed cache save
            
        return info

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
        info = await self._fetch_video_info(prepared_link)

        if not info:
            vid_id = ""
            if "watch?v=" in prepared_link:
                try: vid_id = prepared_link.split("v=")[-1].split("&")[0]
                except: pass
            elif "youtu.be/" in prepared_link:
                 try: vid_id = prepared_link.split("youtu.be/")[-1].split("?")[0]
                 except: pass

            if vid_id or prepared_link.startswith("http"):
                 return "Unknown", "00:00", 0, "", vid_id or ""
            
            # Log technical error, display user-friendly message
            LOGGER(__name__).error(f"Video details failed for: {prepared_link}")
            raise ValueError(
                "❌ **Failed to Process Query**\n\n"
                "I was unable to retrieve information for this request.\n"
                "Please try searching via **@MultiSourceDLBot** in Inline Mode for song search and download."
            )

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
                try: vid_id = prepared_link.split("v=")[-1].split("&")[0]
                except: pass
            
            if vid_id or prepared_link.startswith("http"):
                info = {
                    "id": vid_id, "title": "Unknown", "duration": "00:00",
                    "thumbnail": "", "link": prepared_link
                }
            else:
                 # Log technical error, display user-friendly message
                 LOGGER(__name__).error(f"Track info failed for '{prepared_link}'")
                 raise ValueError(
                    "❌ **Failed to Process Query**\n\n"
                    "I was unable to retrieve information for this request.\n"
                    "Please try searching via **@MultiSourceDLBot** in Inline Mode for song search and download."
                 )

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
            # FIX: Protect Playlist with Semaphore & Timeout
            async with _API_SEMAPHORE:
                plist = await asyncio.wait_for(
                    Playlist.get(link), timeout=_API_TIMEOUT
                )
            items = [video.get("id") for video in plist.get("videos", [])[:limit] if video.get("id")]
            if items: return items
        except Exception as e:
            LOGGER(__name__).error(f"Playlist fetch failed: {e}")
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
        opts = { "quiet": True, "no_cache_dir": True }
        if cf := _cookiefile_path(): opts["cookiefile"] = cf

        out: List[Dict] = []
        try:
            # FIX: Protect blocking call with semaphore and executor & Timeout
            async with _API_SEMAPHORE:
                loop = asyncio.get_running_loop()
                info = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(opts).extract_info(link, download=False)),
                    timeout=_API_TIMEOUT
                )
            
            for fmt in info.get("formats", []):
                if "dash" in str(fmt.get("format", "")).lower(): continue
                if not any(k in fmt for k in ("filesize", "filesize_approx")): continue
                
                size = fmt.get("filesize") or fmt.get("filesize_approx")
                if not size: continue
                out.append({
                    "format": fmt["format"], "filesize": size, "format_id": fmt["format_id"],
                    "ext": fmt["ext"], "format_note": fmt.get("format_note", ""), "yturl": link,
                })
        except Exception as e:
            LOGGER(__name__).error(f"Format extraction failed: {e}")
            pass
        return out, link

    @capture_internal_err
    async def slider(
        self, link: str, query_type: int, videoid: Union[str, bool, None] = None
    ) -> Tuple[str, Optional[str], str, str]:
        try:
            # FIX: Protect VideosSearch
            async with _API_SEMAPHORE:
                data = await asyncio.wait_for(
                    VideosSearch(self._prepare_link(link, videoid), limit=10).next(),
                    timeout=_API_TIMEOUT
                )
            results = data.get("result", [])
        except Exception as e:
            LOGGER(__name__).error(f"Slider VideosSearch failed: {e}")
            query = self._prepare_link(link, videoid)
            stdout, _ = await _exec_proc(
                "yt-dlp", *(_cookies_args()), "--dump-json", "--default-search",
                "ytsearch10", "--no-playlist", query
            )
            results = []
            if stdout:
                for line in stdout.decode().split("\n"):
                    if not line: continue
                    try:
                        v = json.loads(line)
                        results.append({
                            "title": v.get("title"), "duration": v.get("duration_string"),
                            "thumbnails": [{"url": v.get("thumbnail")}], "id": v.get("id")
                        })
                    except: pass
        
        if not results or query_type >= len(results):
            # Log technical error, display user-friendly message
            LOGGER(__name__).error(f"Slider query index {query_type} out of range or no results")
            raise IndexError(
                "❌ **Failed to Process Query**\n\n"
                "I was unable to retrieve information for this request.\n"
                "Please try searching via **@MultiSourceDLBot** in Inline Mode for song search and download."
            )
        r = results[query_type]
        return (
            r.get("title", ""), r.get("duration"),
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

        # 🛑 Check config before downloading video
        if video and not VIDEO_PLAY_ALLOWED:
            return None, None

        if video:
            if await self.is_live(link):
                status, stream_url = await self.video(link)
                if status == 1:
                    return stream_url, None
                return None, None

            title = await self.title(link)
            # FIX: Pass extracted 'videoid' directly to reduce load
            p = await media_download(link, "video", title, videoid)
            return (p, True) if p else (None, None)

        title = await self.title(link)
        # FIX: Pass extracted 'videoid' directly to reduce load
        p = await media_download(link, "audio", title, videoid)
        return (p, True) if p else (None, None)
