# Authored By Certified Coders © 2025
import asyncio
import platform
import socket
from sys import version as pyver

import psutil
from pyrogram import __version__ as pyrover
from pyrogram import filters
from pyrogram.errors import MessageIdInvalid
from pyrogram.types import InputMediaPhoto, Message
from pytgcalls.__version__ import __version__ as pytgver

import config
from AnnieXMedia import app
from AnnieXMedia.core.userbot import assistants
from AnnieXMedia.misc import SUDOERS, mongodb
from AnnieXMedia.plugins import ALL_MODULES
from AnnieXMedia.utils.database import get_served_chats, get_served_users, get_sudoers
from AnnieXMedia.utils.decorators.language import language, languageCB
from AnnieXMedia.utils.inline.stats import (
    build_stats_keyboard,
    build_back_keyboard,
    StatsCallbacks,
)
from config import BANNED_USERS


async def _edit_media_or_reply(cbq, caption: str, reply_markup):
    """
    Safely edits the media or replies if the message is too old/invalid.
    Uses InputMediaPhoto to ensure compatibility with IMG_URLs.
    """
    media = InputMediaPhoto(media=config.STATS_IMG_URL, caption=caption)
    try:
        await cbq.edit_message_media(media=media, reply_markup=reply_markup)
    except MessageIdInvalid:
        await cbq.message.reply_photo(
            photo=config.STATS_IMG_URL, caption=caption, reply_markup=reply_markup
        )
    except Exception as e:
        # Fallback for unexpected errors
        try:
            await cbq.edit_message_text(text=caption, reply_markup=reply_markup)
        except:
            pass


@app.on_message(filters.command(["stats", "gstats"]) & ~BANNED_USERS)
@language
async def open_stats(client, message: Message, _):
    is_sudo = message.from_user and (message.from_user.id in SUDOERS)
    keyboard = build_stats_keyboard(_, is_sudo)
    await message.reply_photo(
        photo=config.STATS_IMG_URL,
        caption=_["gstats_2"].format(app.mention),
        reply_markup=keyboard,
    )


@app.on_callback_query(filters.regex(f"^{StatsCallbacks.BACK}$") & ~BANNED_USERS)
@languageCB
async def handle_back_to_stats(client, callback_query, _):
    is_sudo = callback_query.from_user and (callback_query.from_user.id in SUDOERS)
    keyboard = build_stats_keyboard(_, is_sudo)
    try:
        await callback_query.edit_message_text(
            text=_["gstats_2"].format(app.mention), reply_markup=keyboard
        )
    except MessageIdInvalid:
        # If media type changed, we might need to send fresh
        await callback_query.message.reply_photo(
            photo=config.STATS_IMG_URL,
            caption=_["gstats_2"].format(app.mention),
            reply_markup=keyboard,
        )


@app.on_callback_query(filters.regex(f"^{StatsCallbacks.SHOW_OVERVIEW}$") & ~BANNED_USERS)
@languageCB
async def handle_show_overview(client, callback_query, _):
    await callback_query.answer("Collecting Overview...")
    
    try:
        back_keyboard = build_back_keyboard(_)
        
        # Database calls
        served_chats = len(await get_served_chats())
        served_users = len(await get_served_users())
        
        caption = _["gstats_3"].format(
            app.mention,
            len(assistants),
            len(BANNED_USERS),
            served_chats,
            served_users,
            len(ALL_MODULES),
            len(SUDOERS),
            "Enabled" if config.AUTO_LEAVING_ASSISTANT else "Disabled",
            config.DURATION_LIMIT_MIN,
        )
        await _edit_media_or_reply(callback_query, caption, back_keyboard)
        
    except Exception as e:
        await callback_query.answer(f"Error: {e}", show_alert=True)


@app.on_callback_query(filters.regex(f"^{StatsCallbacks.SHOW_BOT_STATS}$") & ~BANNED_USERS)
@languageCB
async def handle_show_bot_stats(client, callback_query, _):
    if callback_query.from_user.id not in SUDOERS:
        return await callback_query.answer(_["gstats_4"], show_alert=True)
    
    await callback_query.answer("Collecting System Stats...")
    
    # Switch to text to indicate loading
    try:
        await callback_query.edit_message_text(_["gstats_1"].format(app.mention))
    except:
        pass
    
    back_keyboard = build_back_keyboard(_)
    
    # Run blocking psutil calls in a separate thread to avoid freezing the bot
    loop = asyncio.get_running_loop()
    
    def get_system_stats():
        cpu_p = psutil.cpu_count(logical=False)
        cpu_t = psutil.cpu_count(logical=True)
        ram = f"{round(psutil.virtual_memory().total / (1024.0 ** 3))} ɢʙ"
        try:
            freq = psutil.cpu_freq().current
            if freq >= 1000:
                freq_str = f"{round(freq / 1000, 2)}ɢʜᴢ"
            else:
                freq_str = f"{round(freq, 2)}ᴍʜᴢ"
        except:
            freq_str = "N/A"
            
        disk = psutil.disk_usage("/")
        d_total = str(disk.total / (1024.0 ** 3))[:4]
        d_used = str(disk.used / (1024.0 ** 3))[:4]
        d_free = str(disk.free / (1024.0 ** 3))[:4]
        
        return cpu_p, cpu_t, ram, freq_str, d_total, d_used, d_free

    try:
        physical_cores, total_cores, ram_total_gb, cpu_freq, disk_total, disk_used, disk_free = await loop.run_in_executor(None, get_system_stats)
        
        # Async DB Stats
        db_stats = await mongodb.command("dbstats")
        data_size_kb = str(db_stats["dataSize"] / 1024)[:6]
        storage_kb = str(db_stats["storageSize"] / 1024)[:6]
        collections = db_stats["collections"]
        objects = db_stats["objects"]
        
        served_chats = len(await get_served_chats())
        served_users = len(await get_served_users())
        sudoers_count = len(await get_sudoers())

        caption = _["gstats_5"].format(
            app.mention,
            len(ALL_MODULES),
            platform.system(),
            ram_total_gb,
            physical_cores,
            total_cores,
            cpu_freq,
            pyver.split()[0],
            pyrover,
            pytgver,
            disk_total,
            disk_used,
            disk_free,
            served_chats,
            served_users,
            len(BANNED_USERS),
            sudoers_count,
            data_size_kb,
            storage_kb,
            collections,
            objects,
        )
        await _edit_media_or_reply(callback_query, caption, back_keyboard)
        
    except Exception as e:
        await callback_query.message.edit_text(f"❌ **Failed to fetch system stats:**\n`{e}`", reply_markup=back_keyboard)
