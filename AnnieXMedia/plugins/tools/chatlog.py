# Authored By Certified Coders © 2025
import asyncio
import urllib.parse
from typing import Optional

from pyrogram import filters, errors, types
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup

from config import LOGGER_ID
from AnnieXMedia import app

BOT_INFO: Optional[types.User] = None
BOT_ID: Optional[int] = None
LEFT_ID = -1003499984720


def _is_valid_url(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        parsed = urllib.parse.urlparse(url.strip())
        return parsed.scheme in ("http", "https", "tg") and (parsed.netloc or parsed.path)
    except Exception:
        return False


async def _ensure_bot_info() -> None:
    global BOT_INFO, BOT_ID
    if BOT_INFO is None:
        try:
            BOT_INFO = await app.get_me()
            BOT_ID = BOT_INFO.id
        except Exception as e:
            print(f"Failed to get bot info: {e}")


async def safe_send_message(chat_id, text, reply_markup=None, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            return await app.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup
            )
        except errors.FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Failed to send message after {max_retries} attempts: {e}")
                raise
            await asyncio.sleep(1)


@app.on_message(filters.new_chat_members)
async def join_watcher(_, message: Message):
    try:
        await _ensure_bot_info()
        if BOT_INFO is None or BOT_ID is None:
            return

        chat = message.chat
        try:
            invite_link = await app.export_chat_invite_link(chat.id)
        except Exception:
            invite_link = None

        for member in message.new_chat_members:
            # Only log when THIS bot is added
            if member.id != BOT_ID:
                continue

            # Members count
            member_count = "?"
            try:
                member_count = await app.get_chat_members_count(chat.id)
            except errors.FloodWait as fw:
                await asyncio.sleep(fw.value + 1)
                try:
                    member_count = await app.get_chat_members_count(chat.id)
                except Exception:
                    pass
            except Exception:
                pass

            adder = message.from_user.mention if message.from_user else "Unknown"

            text = (
                "📝 **ɴᴇᴡ ɢʀᴏᴜᴘ ʟᴏɢ**\n\n"
                "**ʙᴏᴛ ᴀᴅᴅᴇᴅ ɪɴ ᴀ ɴᴇᴡ ɢʀᴏᴜᴘ** ✅\n\n"
                f"📌 **ᴄʜᴀᴛ ɴᴀᴍᴇ:** `{chat.title}`\n"
                f"🆔 **ᴄʜᴀᴛ ɪᴅ:** `{chat.id}`\n"
                f"🔐 **ᴄʜᴀᴛ ᴜsᴇʀɴᴀᴍᴇ:** @{chat.username if chat.username else 'Private'}\n"
                f"🛰 **ᴄʜᴀᴛ ʟɪɴᴋ:** {invite_link or '`No Invite Link`'}\n"
                f"📈 **ɢʀᴏᴜᴘ ᴍᴇᴍʙᴇʀs:** `{member_count}`\n"
                f"➕ **ᴀᴅᴅᴇᴅ ʙʏ:** {adder}"
            )

            reply_markup = None
            if _is_valid_url(invite_link):
                reply_markup = InlineKeyboardMarkup(
                    [[InlineKeyboardButton("sᴇᴇ ɢʀᴏᴜᴘ 👀", url=invite_link.strip())]]
                )

            await safe_send_message(
                LOGGER_ID,
                text=text,
                reply_markup=reply_markup
            )
    except Exception as e:
        print(f"Error in join_watcher: {e}")


@app.on_message(filters.left_chat_member)
async def on_left_chat_member(_, message: Message):
    try:
        await _ensure_bot_info()
        if BOT_INFO is None or BOT_ID is None:
            return

        # Only care when THIS bot leaves
        if not message.left_chat_member or message.left_chat_member.id != BOT_ID:
            return

        chat = message.chat

        # Who triggered the leave?
        actor = message.from_user  # user who removed or who left (if self)
        if actor is None:
            remover_mention = "**ᴜɴᴋɴᴏᴡɴ ᴜsᴇʀ**"
        else:
            remover_mention = actor.mention

        # Determine reason
        if actor and actor.id == BOT_ID:
            # Bot left by itself (for example via leave_chat or kicked by self in code)
            reason = "🤖 **ʀᴇᴀsᴏɴ:** ʙᴏᴛ ʟᴇғᴛ ᴛʜᴇ ᴄʜᴀᴛ ɪᴛsᴇʟғ (sᴇʟғ ʟᴇғᴛ)"
            remover_line = f"👤 **ᴀᴄᴛᴏʀ:** @{BOT_INFO.username}"
        else:
            # Someone removed the bot
            reason = "🚫 **ʀᴇᴀsᴏɴ:** ʙᴏᴛ ᴡᴀs ʀᴇᴍᴏᴠᴇᴅ/ᴋɪᴄᴋᴇᴅ ғʀᴏᴍ ᴛʜᴇ ɢʀᴏᴜᴘ"
            remover_line = f"👤 **ʀᴇᴍᴏᴠᴇᴅ ʙʏ:** {remover_mention}"

        text = (
            "✫ **<u>#ʟᴇғᴛ_ɢʀᴏᴜᴘ</u>** ✫\n\n"
            f"📌 **ᴄʜᴀᴛ ɴᴀᴍᴇ:** `{chat.title}`\n"
            f"🆔 **ᴄʜᴀᴛ ɪᴅ:** `{chat.id}`\n"
            f"{remover_line}\n"
            f"{reason}\n"
            f"🤖 **ʙᴏᴛ:** @{BOT_INFO.username}"
        )

        await safe_send_message(LEFT_ID, text)
    except Exception as e:
        print(f"Error in on_left_chat_member: {e}")
