# Authored By Certified Coders © 2025
import re
from os import getenv
from dotenv import load_dotenv
from pyrogram import filters

# Load environment variables from .env file
load_dotenv()

# ── Core bot config ────────────────────────────────────────────────────────────
API_ID = int(getenv("API_ID", 27798659))
API_HASH = getenv("API_HASH", "26100c77cee02e5e34b2bbee58440f86")
BOT_TOKEN = getenv("BOT_TOKEN")

OWNER_ID = int(getenv("OWNER_ID", 6848223695))
OWNER_USERNAME = getenv("OWNER_USERNAME", "its_damiann")
BOT_USERNAME = getenv("BOT_USERNAME", "Maddy_MusicBot")
BOT_NAME = getenv("BOT_NAME", "˹ ᴍᴀᴅᴅʏ ✘ 𝙼ᴜsɪᴄ˼ ♪")
ASSUSERNAME = getenv("ASSUSERNAME", "HDjskjdjdhs")

# ── Database & logging ─────────────────────────────────────────────────────────
MONGO_DB_URI = getenv("MONGO_DB_URI")
LOGGER_ID = int(getenv("LOGGER_ID", -1002014167331))

# ── Limits (durations in min/sec; sizes in bytes) ──────────────────────────────
DURATION_LIMIT_MIN = int(getenv("DURATION_LIMIT", 300))
SONG_DOWNLOAD_DURATION = int(getenv("SONG_DOWNLOAD_DURATION", "1200"))
SONG_DOWNLOAD_DURATION_LIMIT = int(getenv("SONG_DOWNLOAD_DURATION_LIMIT", "1800"))
TG_AUDIO_FILESIZE_LIMIT = int(getenv("TG_AUDIO_FILESIZE_LIMIT", "157286400"))
TG_VIDEO_FILESIZE_LIMIT = int(getenv("TG_VIDEO_FILESIZE_LIMIT", "1288490189"))
PLAYLIST_FETCH_LIMIT = int(getenv("PLAYLIST_FETCH_LIMIT", "30"))

# ── External APIs ──────────────────────────────────────────────────────────────
COOKIE_URL = getenv("COOKIE_URL")  # required (paste link)
API_URL = getenv("API_URL", 'https://api.thequickearn.xyz') #youtube song url
VIDEO_API_URL = getenv("VIDEO_API_URL", 'https://api.video.thequickearn.xyz')
API_KEY = getenv("API_KEY")
DEEP_API = getenv("DEEP_API")      # optional

# ── Hosting / deployment ───────────────────────────────────────────────────────
HEROKU_APP_NAME = getenv("HEROKU_APP_NAME")
HEROKU_API_KEY = getenv("HEROKU_API_KEY")

# ── Git / updates ──────────────────────────────────────────────────────────────
UPSTREAM_REPO = getenv("UPSTREAM_REPO", "https://github.com/music-miko/anon")
UPSTREAM_BRANCH = getenv("UPSTREAM_BRANCH", "Master")
GIT_TOKEN = getenv("GIT_TOKEN")  # needed if repo is private

# ── Support links ──────────────────────────────────────────────────────────────
SUPPORT_CHANNEL = getenv("SUPPORT_CHANNEL", "https://t.me/ArcUpdates")
SUPPORT_CHAT = getenv("SUPPORT_CHAT", "https://t.me/ArcChatz")

# ── Assistant auto-leave ───────────────────────────────────────────────────────
AUTO_LEAVING_ASSISTANT = False
AUTO_LEAVE_ASSISTANT_TIME = int(getenv("ASSISTANT_LEAVE_TIME", "3600"))

# ── Debug ──────────────────────────────────────────────────────────────────────
DEBUG_IGNORE_LOG = True

# ── Spotify (optional) ─────────────────────────────────────────────────────────
SPOTIFY_CLIENT_ID = getenv("SPOTIFY_CLIENT_ID", "22b6125bfe224587b722d6815002db2b")
SPOTIFY_CLIENT_SECRET = getenv("SPOTIFY_CLIENT_SECRET", "c9c63c6fbf2f467c8bc68624851e9773")

# ── Session strings (optional) ─────────────────────────────────────────────────
STRING1 = getenv("STRING_SESSION")
STRING2 = getenv("STRING_SESSION2")
STRING3 = getenv("STRING_SESSION3")
STRING4 = getenv("STRING_SESSION4")
STRING5 = getenv("STRING_SESSION5")

# ── Media assets ───────────────────────────────────────────────────────────────
START_IMG_URL = getenv(
    "START_IMG_URL", "https://files.catbox.moe/yjl5om.jpg"
)
STICKERS = [
    "CAACAgUAAx0Cd6nKUAACASBl_rnalOle6g7qS-ry-aZ1ZpVEnwACgg8AAizLEFfI5wfykoCR4h4E",
    "CAACAgUAAx0Cd6nKUAACATJl_rsEJOsaaPSYGhU7bo7iEwL8AAPMDgACu2PYV8Vb8aT4_HUPHgQ",
]
HELP_IMG_URL = "https://files.catbox.moe/yjl5om.jpg"
PING_IMG_URL = "https://files.catbox.moe/lq63qh.jpg"
PLAYLIST_IMG_URL = "https://files.catbox.moe/tny9ug.jpg"
STATS_IMG_URL = "https://files.catbox.moe/uih0vr.jpg"
TELEGRAM_AUDIO_URL = "https://files.catbox.moe/nknnw1.jpg"
TELEGRAM_VIDEO_URL = "https://files.catbox.moe/1xn73k.jpg"
STREAM_IMG_URL = "https://files.catbox.moe/tny9ug.jpg"
SOUNCLOUD_IMG_URL = "https://files.catbox.moe/1xn73k.jpg"
YOUTUBE_IMG_URL = "https://files.catbox.moe/ibrn13.jpg"
SPOTIFY_ARTIST_IMG_URL = SPOTIFY_ALBUM_IMG_URL = SPOTIFY_PLAYLIST_IMG_URL = YOUTUBE_IMG_URL

# ── Helpers ────────────────────────────────────────────────────────────────────
def time_to_seconds(time: str) -> int:
    return sum(int(x) * 60**i for i, x in enumerate(reversed(time.split(":"))))

DURATION_LIMIT = time_to_seconds(f"{DURATION_LIMIT_MIN}:00")

# ───── Bot Introduction Messages ───── #
AYU = ["💞", "🦋", "🔍", "🧪", "⚡️", "🔥", "🎩", "🌈", "🍷", "🥂", "🥃", "🕊️", "🪄", "💌", "🧨"]
WELCOME_TEXT = (
"<b>𝖧𝖾𝗒𝗒</b> {0}, 🌷\n\n๏ 𝖸𝗈𝗎’𝗋𝖾 𝗍𝖺𝗅𝗄𝗂𝗇𝗀 𝗍𝗈 {1}!\n\n➻ 𝖠 𝗉𝗋𝖾𝗆𝗂𝗎𝗆, 𝗉𝗈𝗐𝖾𝗋𝗉𝖺𝖼𝗄𝖾𝖽 𝖬𝗎𝗌𝗂𝖼 𝖡𝗈𝗍 𝗍𝗈 𝗅𝗂𝗀𝗁𝗍 𝗎𝗉 𝗒𝗈𝗎𝗋 𝗀𝗋𝗈𝗎𝗉 𝗏𝗂𝖻𝖾𝗌 🎶\n───────────────────────────\n<b>๏ 𝖳𝗋𝗒 <code>/help</code> 𝗍𝗈 𝖽𝗂𝗌𝖼𝗈𝗏𝖾𝗋 𝖺𝗅𝗅 𝗍𝗁𝖾 𝖿𝗎𝗇 𝖼𝗈𝗆𝗆𝖺𝗇𝖽𝗌 𝖺𝗇𝖽 𝖿𝖾𝖺𝗍𝗎𝗋𝖾𝗌!</b>"
)

# ── Runtime structures ─────────────────────────────────────────────────────────
BANNED_USERS = filters.user()
adminlist, lyrical, autoclean, confirmer = {}, {}, [], {}

# ── Minimal validation ─────────────────────────────────────────────────────────
if SUPPORT_CHANNEL and not re.match(r"^https?://", SUPPORT_CHANNEL):
    raise SystemExit("[ERROR] - Invalid SUPPORT_CHANNEL URL. Must start with https://")

if SUPPORT_CHAT and not re.match(r"^https?://", SUPPORT_CHAT):
    raise SystemExit("[ERROR] - Invalid SUPPORT_CHAT URL. Must start with https://")
