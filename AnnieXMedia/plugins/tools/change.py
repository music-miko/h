from pyrogram import Client, filters
from config import OWNER_ID
from AnnieXMedia import app
from AnnieXMedia.utils.database import onoffdb

@app.on_message(filters.command("video") & filters.user(OWNER_ID))
async def enable_download_first(_, message):
    await onoffdb.update_one({"on_off": 1}, {"$set": {"on_off": 1}}, upsert=True)
    await message.reply("🔄 Video mode updated:\n📥 Download First → Stream fallback enabled!")
