# Authored By Certified Coders © 2025
import asyncio
import os
import shutil
import socket
import sys
from datetime import datetime, timedelta, timezone

import urllib3
from git import Repo
from git.exc import GitCommandError, InvalidGitRepositoryError
from pyrogram import filters

import config
from AnnieXMedia import app
from AnnieXMedia.misc import HAPP, SUDOERS, XCB
from AnnieXMedia.utils.database import (
    get_active_chats,
    remove_active_chat,
    remove_active_video_chat,
)
from AnnieXMedia.utils.decorators.language import language
from AnnieXMedia.utils.pastebin import ANNIEBIN

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
# Define IST Timezone (UTC + 5:30)
IST = timezone(timedelta(hours=5, minutes=30))
SUPPORT_CHAT_ID = -1002144355688
# ---------------------

async def is_heroku():
    return "heroku" in socket.getfqdn()

def cleanup_storage():
    folders_to_remove = ["downloads", "raw_files", "cache"]
    for folder in folders_to_remove:
        try:
            shutil.rmtree(folder)
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"[CLEANUP] Failed to delete {folder}: {e}")

    for root, dirs, files in os.walk("."):
        for d in dirs:
            if d == "__pycache__":
                try:
                    shutil.rmtree(os.path.join(root, d))
                except:
                    pass

# --- STARTUP NOTIFICATION (SENT AFTER RESTART) ---
async def send_startup_notification():
    """Sends a professional status report to support chat when the bot comes online."""
    # Wait 10 seconds to ensure connection is fully stable
    await asyncio.sleep(10)
    try:
        now = datetime.now(IST)
        await app.send_message(
            chat_id=SUPPORT_CHAT_ID,
            text=(
                "<b>🚀 System Online</b>\n\n"
                "<b>🤖 Bot Status:</b> 🟢 <b>Online</b>\n"
                f"<b>📅 Date:</b> {now.strftime('%B %d, %Y')}\n"
                f"<b>⏰ Time:</b> {now.strftime('%I:%M %p')} IST\n\n"
                "<b>✅ Systems Check:</b>\n"
                "• Database: Connected 🗄️\n"
                "• Modules: Loaded 📦\n"
                "• Audio Client: Active 🔊\n\n"
                "<i>The music bot has started successfully and is ready for use!</i> 🎵"
            )
        )
    except Exception as e:
        print(f"[STARTUP] Failed to send startup message: {e}")

# --- AUTO RESTART SCHEDULER (6:30 AM IST) ---
async def auto_restart_job():
    while True:
        try:
            # Get current time in IST
            now = datetime.now(IST)
            
            # Set target to 6:30 AM IST today
            target = now.replace(hour=6, minute=30, second=0, microsecond=0)
            
            # If 6:30 AM has already passed today, schedule for tomorrow
            if target <= now:
                target += timedelta(days=1)
            
            wait_seconds = (target - now).total_seconds()
            
            # Log the schedule
            print(f"[SCHEDULER] Auto-Restart scheduled for {target.strftime('%Y-%m-%d %I:%M %p')} IST")
            
            # Wait until target time
            await asyncio.sleep(wait_seconds)
            
            # --- START RESTART SEQUENCE ---
            print("[SCHEDULER] Executing Auto-Restart & Cleanup...")
            
            # 1. Notify Active Chats
            try:
                ac_chats = await get_active_chats()
                for x in ac_chats:
                    try:
                        await app.send_message(
                            chat_id=int(x),
                            text=(
                                "<b>🔄 Scheduled Maintenance</b>\n\n"
                                f"{app.mention} is performing a daily system restart to ensure optimal performance.\n"
                                "<b>⏳ Downtime:</b> 15-20 Seconds.\n\n"
                                "<i>Thank you for your patience!</i> 🙏"
                            )
                        )
                        await remove_active_chat(x)
                        await remove_active_video_chat(x)
                    except:
                        pass
            except Exception as e:
                print(f"[SCHEDULER] Error notifying chats: {e}")

            # 2. Clean Storage
            cleanup_storage()

            # 3. Restart System
            os.execv(sys.executable, [sys.executable, "-m", "AnnieXMedia"])
            
        except Exception as e:
            print(f"[SCHEDULER] Error in auto-restart loop: {e}")
            await asyncio.sleep(60) 

# Initialize Background Tasks
asyncio.create_task(send_startup_notification())
asyncio.create_task(auto_restart_job())


@app.on_message(filters.command(["getlog", "logs", "getlogs"]) & SUDOERS)
@language
async def log_(client, message, _):
    try:
        await message.reply_document(document="log.txt")
    except:
        await message.reply_text("❌ <b>Error:</b> Failed to get logs. The file might be empty or missing.")


@app.on_message(filters.command(["update", "gitpull"]) & SUDOERS)
@language
async def update_(client, message, _):
    if await is_heroku():
        if HAPP is None:
            return await message.reply_text("❌ <b>Error:</b> Please set the HEROKU_API_KEY variable.")

    response = await message.reply_text("🔃 <b>Checking for updates...</b>")
    try:
        repo = Repo()
    except GitCommandError:
        return await response.edit("❌ <b>Git Error:</b> Command failed.")
    except InvalidGitRepositoryError:
        return await response.edit("❌ <b>Git Error:</b> Invalid repository.")

    os.system(f"git fetch origin {config.UPSTREAM_BRANCH} &> /dev/null")
    await asyncio.sleep(7)

    verification = ""
    REPO_ = repo.remotes.origin.url.split(".git")[0]
    for checks in repo.iter_commits(f"HEAD..origin/{config.UPSTREAM_BRANCH}"):
        verification = str(checks.count())

    if verification == "":
        return await response.edit("✅ <b>Bot is up to date.</b>")

    updates = ""
    ordinal = lambda format: "%d%s" % (
        format,
        "tsnrhtdd"[(format // 10 % 10 != 1) * (format % 10 < 4) * format % 10 :: 4],
    )
    for info in repo.iter_commits(f"HEAD..origin/{config.UPSTREAM_BRANCH}"):
        updates += (
            f"<b>🔹 #{info.count()}: <a href={REPO_}/commit/{info}>{info.summary}</a></b>\n"
            f"   👤 <b>By:</b> {info.author}\n"
            f"   📅 <b>Date:</b> {ordinal(int(datetime.fromtimestamp(info.committed_date).strftime('%d')))} "
            f"{datetime.fromtimestamp(info.committed_date).strftime('%b')}, {datetime.fromtimestamp(info.committed_date).strftime('%Y')}\n\n"
        )

    _update_response_ = (
        "<b>📦 New Update Available!</b>\n"
        "<i>Pushing updates to the server...</i>\n\n"
        "<b>📋 Changelog:</b>\n\n"
    )
    _final_updates_ = _update_response_ + updates

    if len(_final_updates_) > 4096:
        url = await ANNIEBIN(updates)
        nrs = await response.edit(
            f"<b>📦 New Update Available!</b>\n"
            f"<i>Pushing updates to the server...</i>\n\n"
            f"<b>📋 Changelog:</b>\n<a href={url}>🔗 View Full Changelog</a>"
        )
    else:
        nrs = await response.edit(_final_updates_, disable_web_page_preview=True)

    os.system("git stash &> /dev/null && git pull")

    try:
        served_chats = await get_active_chats()
        for x in served_chats:
            try:
                await app.send_message(
                    chat_id=int(x), 
                    text=f"🔄 {app.mention} has been updated and is restarting..."
                )
                await remove_active_chat(x)
                await remove_active_video_chat(x)
            except:
                pass
        await response.edit(f"{nrs.text}\n\n✅ <b>Update Complete!</b> Restarting now...")
    except:
        pass

    cleanup_storage()

    if await is_heroku():
        try:
            os.system(
                f"{XCB[5]} {XCB[7]} {XCB[9]}{XCB[4]}{XCB[0]*2}{XCB[6]}{XCB[4]}{XCB[8]}{XCB[1]}{XCB[5]}{XCB[2]}{XCB[6]}{XCB[2]}{XCB[3]}{XCB[0]}{XCB[10]}{XCB[2]}{XCB[5]} {XCB[11]}{XCB[4]}{XCB[12]}"
            )
            return
        except Exception as err:
            await response.edit(f"{nrs.text}\n\n❌ <b>Heroku Error:</b> {err}")
            return await app.send_message(
                chat_id=config.LOGGER_ID,
                text=f"❌ Heroku Restart Error: {err}",
            )
    else:
        os.execv(sys.executable, [sys.executable, "-m", "AnnieXMedia"])


@app.on_message(filters.command(["restart"]) & SUDOERS)
async def restart_(_, message):
    response = await message.reply_text("🔄 <b>Restarting system...</b>")
    ac_chats = await get_active_chats()
    for x in ac_chats:
        try:
            await app.send_message(
                chat_id=int(x),
                text=(
                    f"🔄 {app.mention} is restarting...\n"
                    "<i>You can start playing again in 15-20 seconds.</i>"
                ),
            )
            await remove_active_chat(x)
            await remove_active_video_chat(x)
        except:
            pass

    cleanup_storage()

    await response.edit_text(
        "✅ <b>Restart Initiated</b>\n\n"
        "<i>Please wait while the bot reboots...</i>"
    )

    os.execv(sys.executable, [sys.executable, "-m", "AnnieXMedia"])
