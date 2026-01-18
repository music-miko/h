# Authored By Certified Coders © 2025
# Updated for Professional Standards

from datetime import datetime
from pyrogram import filters, enums
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from config import OWNER_ID
from AnnieXMedia import app

# Configuration
# Replace this with your specific Log Channel ID if different
LOG_CHANNEL_ID = -1003043405529

def extract_bug_content(msg: Message) -> str | None:
    """Extracts the text after the command."""
    if msg.text and len(msg.text.split()) > 1:
        return msg.text.split(None, 1)[1]
    return None

def escape_md(text: str) -> str:
    """Escapes Markdown special characters to prevent broken formatting."""
    special_chars = ['[', ']', '(', ')', '`', '*', '_']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

@app.on_message(filters.command("bug"))
async def report_bug(_, msg: Message):
    # 1. Validation: Group Chats Only
    if msg.chat.type == enums.ChatType.PRIVATE:
        return await msg.reply_text(
            "<b>⚠️ Action not allowed.</b>\nThis command is designed for group usage only.",
            parse_mode=enums.ParseMode.HTML
        )

    # 2. Validation: Content Existence
    bug_description = extract_bug_content(msg)
    if not bug_description:
        return await msg.reply_text(
            "<b>❌ Missing Description</b>\n\n"
            "Please provide a description of the bug.\n"
            "<i>Example: /bug The music player stops after 2 minutes.</i>",
            parse_mode=enums.ParseMode.HTML
        )

    # 3. Validation: Owner Bypass
    user_id = msg.from_user.id
    if user_id == OWNER_ID:
        return await msg.reply_text(
            "<b>Note:</b> You are identified as the Bot Owner. Please log this issue manually."
        )

    # 4. Data Preparation
    user_name = escape_md(msg.from_user.first_name)
    mention = f"[{user_name}](tg://user?id={user_id})"
    
    chat_title = msg.chat.title or "Unknown Chat"
    if msg.chat.username:
        chat_reference = f"[{chat_title}](https://t.me/{msg.chat.username})"
    else:
        chat_reference = f"{chat_title} (`{msg.chat.id}`)"

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M UTC")

    # 5. Build the Report (Log Channel)
    bug_report = (
        f"**#BugReport Filed**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"**👤 Reported By:** {mention}\n"
        f"**🆔 User ID:** `{user_id}`\n"
        f"**📍 Source:** {chat_reference}\n"
        f"**📅 Date:** `{current_date}`\n\n"
        f"**📝 Description:**\n"
        f"> {escape_md(bug_description)}"
    )

    # 6. Send Response to User
    await msg.reply_text(
        f"**✅ Report Sent Successfully**\n\n"
        f"Thank you, {user_name}. Your report has been forwarded to the support team for review.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Close Message", callback_data="close_user_msg")]]
        )
    )

    # 7. Send Report to Log Channel
    log_buttons = [
        [InlineKeyboardButton("🗑️ Dismiss Report", callback_data="close_log_report")]
    ]
    
    # Add "View Message" button if the source chat is public/accessible
    if msg.chat.username:
        msg_link = f"https://t.me/{msg.chat.username}/{msg.id}"
        log_buttons.insert(0, [InlineKeyboardButton("🔗 View Original Message", url=msg_link)])

    await app.send_message(
        LOG_CHANNEL_ID,
        bug_report,
        parse_mode=enums.ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(log_buttons),
        disable_web_page_preview=True
    )

# --- Callbacks ---

@app.on_callback_query(filters.regex("close_user_msg"))
async def close_user_message(_, query: CallbackQuery):
    """Allows the user to close their own success message."""
    await query.message.delete()

@app.on_callback_query(filters.regex("close_log_report"))
async def close_log_report(_, query: CallbackQuery):
    """Allows admins in the log channel to dismiss the report."""
    try:
        member = await app.get_chat_member(query.message.chat.id, query.from_user.id)
        
        # Check for admin privileges
        if not (member.status in [enums.ChatMemberStatus.OWNER, enums.ChatMemberStatus.ADMINISTRATOR]):
             return await query.answer("⚠️ You do not have permission to dismiss this report.", show_alert=True)
             
        # Check specifically for delete rights if necessary, or just generic admin
        if member.status == enums.ChatMemberStatus.ADMINISTRATOR and not member.privileges.can_delete_messages:
            return await query.answer("⚠️ You need 'Delete Messages' permission to dismiss this.", show_alert=True)

        await query.message.delete()
        
    except Exception as e:
        print(f"Error in close_log_report: {e}")
        return await query.answer("❌ Error: Could not verify permissions.", show_alert=True)
