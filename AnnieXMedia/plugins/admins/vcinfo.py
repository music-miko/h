# Authored By Certified Coders © 2025
from pyrogram import filters
from pyrogram.enums import ChatType
from pyrogram.types import Message

from config import BANNED_USERS
from AnnieXMedia import app
from AnnieXMedia.core.call import StreamController
from AnnieXMedia.utils.database import group_assistant
from AnnieXMedia.utils.admin_filters import admin_filter


@app.on_message(filters.command(["vcinfo", "vcmembers"]) & filters.group & admin_filter & ~BANNED_USERS)
async def vc_info(client, message: Message):
    chat_id = message.chat.id
    status_msg = await message.reply_text("🔄 **Fetching Voice Chat statistics...**")

    try:
        # 1. Get the assistant client and participants
        assistant = await group_assistant(StreamController, chat_id)
        participants = await assistant.get_participants(chat_id)

        if not participants:
            return await status_msg.edit_text("❌ **Voice Chat is currently empty.**")

        # 2. Optimization: Fetch all user details in one batch request (Much faster)
        user_ids = [p.user_id for p in participants]
        try:
            users_batch = await app.get_users(user_ids)
            user_map = {user.id: user for user in users_batch}
        except Exception:
            # Fallback if batch fetch fails (e.g., due to privacy settings)
            user_map = {}

        # 3. Build the professional report
        msg_lines = [
            f"📊 **Voice Chat Report**",
            f"📍 **Chat:** {message.chat.title}",
            f"👥 **Participants:** {len(participants)}\n",
            "<b>📋 Member Status:</b>"
        ]

        for i, p in enumerate(participants, 1):
            # Resolve Name
            user = user_map.get(p.user_id)
            if user:
                user_link = user.mention
            else:
                user_link = f"<code>{p.user_id}</code>"

            # Resolve Status Icons
            is_muted = p.muted
            is_video = getattr(p, "video", False)
            is_screen = getattr(p, "screen_sharing", False)
            volume = getattr(p, "volume", 100)

            # Define Icon
            if is_video:
                status_icon = "📹"  # Video On
            elif is_screen:
                status_icon = "🖥️"  # Screen Share
            elif is_muted:
                status_icon = "🔇"  # Muted
            else:
                status_icon = "🗣️"  # Speaking/Unmuted

            # Format Line
            # Example: 1. 🗣️ User Name [Vol: 100%]
            line = f"{i}. {status_icon} <b>{user_link}</b> [Vol: {volume}%]"
            msg_lines.append(line)

        # 4. Final Formatting
        report_text = "\n".join(msg_lines)
        
        # Check Telegram message length limit (4096 chars)
        if len(report_text) > 4000:
            report_text = (
                f"📊 **Voice Chat Report**\n"
                f"📍 **Chat:** {message.chat.title}\n"
                f"👥 **Total Participants:** {len(participants)}\n\n"
                f"<i>⚠️ List is too long to display directly.</i>"
            )

        await status_msg.edit_text(report_text)

    except Exception as e:
        await status_msg.edit_text(f"❌ **Failed to fetch VC Info.**\n\n**Reason:** `{type(e).__name__}: {e}`")
