# Authored By Certified Coders © 2025
from pyrogram import filters, Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from AnnieXMedia import app
from AnnieXMedia.misc import SUDOERS
from AnnieXMedia.utils.database import (
    get_active_chats,
    get_active_video_chats,
)

@app.on_message(filters.command(["ac", "av"]) & SUDOERS)
async def active_count(client: Client, message: Message):
    audio_chats = await get_active_chats()
    video_chats = await get_active_video_chats()
    
    ac_audio = len(audio_chats)
    ac_video = len(video_chats)
    total = ac_audio + ac_video
    
    await message.reply_text(
        f"<b>▱▱▱ ᴀᴄᴛɪᴠᴇ sᴛᴀᴛs ▱▱▱</b>\n\n"
        f"<b>🔊 ᴀᴜᴅɪᴏ :</b> <code>{ac_audio}</code>\n"
        f"<b>📹 ᴠɪᴅᴇᴏ :</b> <code>{ac_video}</code>\n"
        f"<b>📈 ᴛᴏᴛᴀʟ :</b> <code>{total}</code>",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("✯ ᴄʟᴏsᴇ ✯", callback_data="close")]]
        )
    )
