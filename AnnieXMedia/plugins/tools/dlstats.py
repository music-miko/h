# AnnieXMedia/plugins/tools/dlstats.py
# Commands:
#   /dlstats   -> show download stats counters (MediaDB + V2)
#   /dlreset   -> reset counters (sudo-only)

from pyrogram import filters
from AnnieXMedia import app
from AnnieXMedia.misc import SUDOERS

# If your downloader module path differs, change this import accordingly.
from AnnieXMedia.utils.downloader import get_download_stats, reset_download_stats


def _fmt_stats(stats: dict) -> str:
    total = stats.get("total", 0)
    success = stats.get("success", 0)
    failed = stats.get("failed", 0)

    s_audio = stats.get("success_audio", 0)
    s_video = stats.get("success_video", 0)
    f_audio = stats.get("failed_audio", 0)
    f_video = stats.get("failed_video", 0)

    # V2/API breakdown
    hard401 = stats.get("hard_fail_401", 0)
    hard403 = stats.get("hard_fail_403", 0)
    api4xx = stats.get("api_fail_other_4xx", 0)
    api5xx = stats.get("api_fail_5xx", 0)
    net = stats.get("network_fail", 0)
    tout = stats.get("timeout_fail", 0)
    nocand = stats.get("no_candidate", 0)
    tgfail = stats.get("tg_fail", 0)
    cdnfail = stats.get("cdn_fail", 0)
    hard_cycles = stats.get("hard_cycle_retries", 0)

    # NEW: Media DB counters
    db_hit = stats.get("media_db_hit", 0)
    db_miss = stats.get("media_db_miss", 0)
    db_fail = stats.get("media_db_fail", 0)

    rate = (success * 100 / total) if total else 0.0

    return (
        "📊 **Download Stats (Media DB → V2)**\n"
        f"• Total: `{total}`\n"
        f"• Success: `{success}` | Failed: `{failed}`\n"
        f"• Success Rate: `{rate:.2f}%`\n\n"
        "🗄️ **Media DB**\n"
        f"• Hit: `{db_hit}` | Miss: `{db_miss}` | Fail: `{db_fail}`\n\n"
        "🎵 **Audio**\n"
        f"• Success: `{s_audio}` | Failed: `{f_audio}`\n\n"
        "🎬 **Video**\n"
        f"• Success: `{s_video}` | Failed: `{f_video}`\n\n"
        "🧾 **Failure Breakdown (Mostly V2)**\n"
        f"• Hard 401: `{hard401}` | Hard 403: `{hard403}`\n"
        f"• Hard Cycle Retries: `{hard_cycles}`\n"
        f"• API 4xx: `{api4xx}` | API 5xx: `{api5xx}`\n"
        f"• Network: `{net}` | Timeout: `{tout}`\n"
        f"• No Candidate: `{nocand}`\n"
        f"• Telegram Fail: `{tgfail}` | CDN Fail: `{cdnfail}`"
    )


@app.on_message(filters.command("yt") & SUDOERS)
async def dlstats_cmd(_, message):
    stats = get_download_stats()
    await message.reply_text(_fmt_stats(stats), disable_web_page_preview=True)


@app.on_message(filters.command("ryt") & SUDOERS)
async def dlreset_cmd(_, message):
    reset_download_stats()
    await message.reply_text("✅ Download stats reset.", disable_web_page_preview=True)
