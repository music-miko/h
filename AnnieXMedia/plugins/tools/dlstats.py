# AnnieXMedia/plugins/tools/dlstats.py
# Commands:
#   /dlstats   -> show V2 download stats counters
#   /dlreset   -> reset counters (optional sudo-only)

from pyrogram import filters
from AnnieXMedia import app

# If your downloader module path differs, change this import accordingly.
from AnnieXMedia.utils.downloader import get_download_stats, reset_download_stats
from AnnieXMedia.misc import SUDOERS

def _fmt_stats(stats: dict) -> str:
    total = stats.get("total", 0)
    success = stats.get("success", 0)
    failed = stats.get("failed", 0)

    s_audio = stats.get("success_audio", 0)
    s_video = stats.get("success_video", 0)
    f_audio = stats.get("failed_audio", 0)
    f_video = stats.get("failed_video", 0)

    hard401 = stats.get("hard_fail_401", 0)
    hard403 = stats.get("hard_fail_403", 0)
    api4xx = stats.get("api_fail_other_4xx", 0)
    api5xx = stats.get("api_fail_5xx", 0)
    net = stats.get("network_fail", 0)
    tout = stats.get("timeout_fail", 0)
    nocand = stats.get("no_candidate", 0)
    tgfail = stats.get("tg_fail", 0)
    cdnfail = stats.get("cdn_fail", 0)

    rate = (success * 100 / total) if total else 0.0

    return (
        "📊 **V2 Download Stats**\n"
        f"• Total: `{total}`\n"
        f"• Success: `{success}` | Failed: `{failed}`\n"
        f"• Success Rate: `{rate:.2f}%`\n\n"
        "🎵 **Audio**\n"
        f"• Success: `{s_audio}` | Failed: `{f_audio}`\n\n"
        "🎬 **Video**\n"
        f"• Success: `{s_video}` | Failed: `{f_video}`\n\n"
        "🧾 **Failure Breakdown**\n"
        f"• Hard 401: `{hard401}` | Hard 403: `{hard403}`\n"
        f"• API 4xx: `{api4xx}` | API 5xx: `{api5xx}`\n"
        f"• Network: `{net}` | Timeout: `{tout}`\n"
        f"• No Candidate: `{nocand}`\n"
        f"• Telegram Fail: `{tgfail}` | CDN Fail: `{cdnfail}`"
    )


@app.on_message(filters.command(["dlstats", "yt"]) & SUDOERS)
async def dlstats_cmd(_, message):
    stats = get_download_stats()
    await message.reply_text(_fmt_stats(stats), disable_web_page_preview=True)
