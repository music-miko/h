# Authored By Certified Coders © 2025

from pyrogram import filters
from pyrogram.types import Message
from AnnieXMedia import app
import config

@app.on_message(filters.command("privacy"))
async def privacy_command(client, message: Message):
    """
    Displays the Privacy Policy (Data Collection & Usage) to the user with professional formatting.
    """
    privacy_text = (
        "<b>🔒 Privacy Policy & Data Handling</b>\n\n"
        
        "<b>📝 1. Information We Collect</b>\n"
        "To ensure the seamless operation of our services, we collect and store specific identifiers, including:\n"
        "🔹 <b>User IDs:</b> Used for unique identification and permission management.\n"
        "🔹 <b>Chat IDs:</b> Essential for establishing connections and managing sessions within Voice Chats.\n"
        "🔹 <b>Basic Metadata:</b> Includes public profile names for display purposes during playback.\n\n"
        
        "<b>⚙️ 2. How We Use Your Information</b>\n"
        "The data collected is processed strictly for operational purposes, specifically to:\n"
        "🔸 Facilitate high-quality audio and video streaming in Telegram Voice Chats.\n"
        "🔸 Manage playback queues and process media requests efficiently.\n"
        "🔸 Maintain service stability, security, and prevent platform abuse.\n\n"
        
        "<b>🛡️ 3. Data Protection & Sharing</b>\n"
        "We are committed to maintaining the confidentiality of your information.\n"
        "✅ We **do not** sell, trade, or transfer your personal identifiers to outside parties.\n"
        "✅ Data is retained solely for the duration necessary to provide the requested services."
    )
    
    await message.reply_text(
        text=privacy_text,
        disable_web_page_preview=True
    )


@app.on_message(filters.command(["music", "song"]))
async def music_download_redirect(client, message: Message):
    """
    Redirects users to the downloader bot with a professional message.
    """
    await message.reply_text(
        "<b>🎵 Music Download Portal</b>\n\n"
        "For high-quality music downloads and an enhanced experience, please utilize our dedicated bot.\n\n"
        "📥 <b>Download Here:</b> @MultiSourceDLBot\n"
        "💬 <b>Support Chat:</b> @ArcChatz",
        disable_web_page_preview=True
    )
