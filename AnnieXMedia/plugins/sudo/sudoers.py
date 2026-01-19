# Authored By Certified Coders © 2025
from pyrogram import filters
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from config import BANNED_USERS, OWNER_ID, STATS_IMG_URL
from AnnieXMedia import app
from AnnieXMedia.misc import SUDOERS
from AnnieXMedia.utils.database import add_sudo, remove_sudo
from AnnieXMedia.utils.decorators.language import language
from AnnieXMedia.utils.extraction import extract_user

# ─── Add Sudo ─────────────────────────────────────────────

@app.on_message(filters.command(["addsudo"], prefixes=["/", "!", "."]) & filters.user(OWNER_ID))
@language
async def add_sudo_user(client, message: Message, _):
    if not message.reply_to_message and len(message.command) != 2:
        return await message.reply_text(_["general_1"])

    user = await extract_user(message)
    if not user:
        return await message.reply_text(_["general_1"])

    if user.id in SUDOERS:
        return await message.reply_text(_["sudo_1"].format(user.mention))

    if await add_sudo(user.id):
        SUDOERS.add(user.id)
        return await message.reply_text(_["sudo_2"].format(user.mention))

    await message.reply_text(_["sudo_8"])


# ─── Remove Sudo ───────────────────────────────────────────

@app.on_message(filters.command(["delsudo", "rmsudo"], prefixes=["/", "!", "."]) & filters.user(OWNER_ID))
@language
async def remove_sudo_user(client, message: Message, _):
    if not message.reply_to_message and len(message.command) != 2:
        return await message.reply_text(_["general_1"])

    user = await extract_user(message)
    if not user:
        return await message.reply_text(_["general_1"])

    if user.id not in SUDOERS:
        return await message.reply_text(_["sudo_3"].format(user.mention))

    if await remove_sudo(user.id):
        SUDOERS.discard(user.id)
        return await message.reply_text(_["sudo_4"].format(user.mention))

    await message.reply_text(_["sudo_8"])


# ─── Sudo List Entry ───────────────────────────────────────

@app.on_message(filters.command(["sudolist", "listsudo", "sudoers"], prefixes=["/", "!", "."]) & ~BANNED_USERS)
async def sudoers_list(client, message: Message):
    keyboard = [[InlineKeyboardButton("๏ ᴠɪᴇᴡ sᴜᴅᴏʟɪsᴛ ๏", callback_data="sudo_list_view")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Changed to reply_photo to support edit_caption in callback
    await message.reply_photo(
        photo=STATS_IMG_URL,
        caption=(
            "**🛠️ Sudo Users Management**\n\n"
            "Tap the button below to view the list of authorized sudo users.\n"
            "**🔒 Note:** Restricted to Owner & Sudoers only."
        ),
        reply_markup=reply_markup
    )


# ─── Callback: View Sudo List ──────────────────────────────

@app.on_callback_query(filters.regex("^sudo_list_view$"))
async def view_sudo_list_callback(client, callback_query: CallbackQuery):
    if callback_query.from_user.id not in SUDOERS:
        return await callback_query.answer("🔒 Access Denied: Sudoers Only.", show_alert=True)

    try:
        owner = await app.get_users(OWNER_ID)
        owner_mention = owner.mention if owner else f"Unknown ({OWNER_ID})"
    except:
        owner_mention = f"Unknown ({OWNER_ID})"

    caption = (
        "**👑 ʙᴏᴛ ᴀᴅᴍɪɴɪsᴛʀᴀᴛɪᴏɴ**\n\n"
        f"**🤴 Oᴡɴᴇʀ:** {owner_mention}\n\n"
        "**👮‍♂️ Sᴜᴅᴏ Usᴇʀs:**\n"
    )

    keyboard = [[InlineKeyboardButton("๏ ᴠɪᴇᴡ ᴏᴡɴᴇʀ ๏", url=f"tg://openmessage?user_id={OWNER_ID}")]]
    
    # Batch fetch for performance
    sudo_ids = [uid for uid in SUDOERS if uid != OWNER_ID]
    
    if sudo_ids:
        count = 0
        try:
            # Fetch all users at once to avoid API floods
            users = await app.get_users(sudo_ids)
            for user in users:
                count += 1
                caption += f"**{count}.** {user.mention}\n"
                keyboard.append([
                    InlineKeyboardButton(f"๏ ᴠɪᴇᴡ sᴜᴅᴏ {count} ๏", url=f"tg://openmessage?user_id={user.id}")
                ])
        except Exception:
            # Fallback for deleted accounts
            caption += "<i>Error fetching specific user details.</i>"
    else:
        caption += "<i>No additional sudo users.</i>"

    keyboard.append([InlineKeyboardButton("๏ ʙᴀᴄᴋ ๏", callback_data="sudo_list_back")])
    
    await callback_query.edit_message_caption(caption=caption, reply_markup=InlineKeyboardMarkup(keyboard))


# ─── Callback: Back to List Menu ────────────────────────────

@app.on_callback_query(filters.regex("^sudo_list_back$"))
async def back_to_sudo_list_menu(client, callback_query: CallbackQuery):
    keyboard = [[InlineKeyboardButton("๏ ᴠɪᴇᴡ sᴜᴅᴏʟɪsᴛ ๏", callback_data="sudo_list_view")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await callback_query.edit_message_caption(
        caption=(
            "**🛠️ Sudo Users Management**\n\n"
            "Tap the button below to view the list of authorized sudo users.\n"
            "**🔒 Note:** Restricted to Owner & Sudoers only."
        ),
        reply_markup=reply_markup
    )


# ─── Delete All Sudo ───────────────────────────────────────

@app.on_message(filters.command("delallsudo", prefixes=["/", "!", "%", ",", ".", "@", "#"]) & filters.user(OWNER_ID))
@language
async def remove_all_sudo_users(client, message: Message, _):
    removed_count = 0
    # Create a copy of the list to iterate safely while modifying
    for user_id in list(SUDOERS):
        if user_id != OWNER_ID:
            if await remove_sudo(user_id):
                SUDOERS.discard(user_id)
                removed_count += 1
    
    await message.reply_text(f"✅ **Success:** Removed {removed_count} users from the sudo list.")
