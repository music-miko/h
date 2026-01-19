# Authored By Certified Coders © 2025
from pyrogram import filters
from pyrogram.types import Message

from AnnieXMedia import app
from AnnieXMedia.misc import SUDOERS
from AnnieXMedia.utils.database import autoend_off, autoend_on


@app.on_message(filters.command("autoend") & SUDOERS)
async def auto_end_stream(_, message: Message):
    usage = "**⚠️ Usage:**\n\n/autoend [enable | disable]"
    
    if len(message.command) != 2:
        return await message.reply_text(usage)
    
    state = message.text.split(None, 1)[1].strip().lower()
    
    if state == "enable":
        await autoend_on()
        await message.reply_text(
            "✅ **Auto End Stream Enabled**\n\n"
            "The assistant will automatically leave the voice chat after a few minutes of inactivity."
        )
    elif state == "disable":
        await autoend_off()
        await message.reply_text(
            "❌ **Auto End Stream Disabled**\n\n"
            "The assistant will stay in the voice chat indefinitely, even if no one is listening."
        )
    else:
        await message.reply_text(usage)
