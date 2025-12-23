# Authored By Certified Coders © 2025
import re
from typing import Union

from pyrogram import Client, filters, types
from pyrogram.types import InlineKeyboardMarkup, Message

from AnnieXMedia import app
from AnnieXMedia.utils.database import get_lang
from AnnieXMedia.utils.decorators.language import LanguageStart, languageCB
from AnnieXMedia.utils.inline.help import (
    first_page,
    help_back_markup,
    private_help_panel,
)
from AnnieXMedia.utils.inline.start import private_panel
from config import BANNED_USERS, HELP_IMG_URL, SUPPORT_CHAT
from strings import get_string, helpers

# ────────────────────────────────────────────────  /help entrypoints ──

@app.on_message(filters.command(["help"]) & filters.private & ~BANNED_USERS)
@app.on_callback_query(filters.regex("open_help") & ~BANNED_USERS)
@LanguageStart
async def helper_private(client: Client, update: Union[Message, types.CallbackQuery], _):
    is_cb = isinstance(update, types.CallbackQuery)
    language = await get_lang(update.from_user.id)
    _ = get_string(language)

    keyboard = first_page(_)
    caption = _["help_1"].format(SUPPORT_CHAT)

    if is_cb:
        await update.answer()
        await update.message.edit_caption(caption, reply_markup=keyboard)
    else:
        await update.delete()
        await update.reply_photo(
            photo=HELP_IMG_URL,
            caption=caption,
            reply_markup=keyboard
        )

# ────────────────────────────────────────────────  group /help notice ─

@app.on_message(filters.command(["help"]) & filters.group & ~BANNED_USERS)
@LanguageStart
async def help_com_group(client: Client, message: Message, _):
    keyboard = private_help_panel(_)
    await message.reply_text(
        _["help_2"],
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True
    )

# ────────────────────────────────────────────────  main help buttons ──

@app.on_callback_query(filters.regex(r"help_callback hb(\d+)_p(\d+)") & ~BANNED_USERS)
@languageCB
async def helper_cb(client: Client, CallbackQuery: types.CallbackQuery, _):
    match = re.match(r"help_callback hb(\d+)_p(\d+)", CallbackQuery.data)
    if not match:
        return await CallbackQuery.answer("Invalid callback.", show_alert=True)

    number = int(match.group(1))

    # ── Load help text directly (no action sub-menu)
    help_text = getattr(helpers, f"HELP_{number}", None)
    if not help_text:
        return await CallbackQuery.answer("Invalid help topic.", show_alert=True)

    await CallbackQuery.edit_message_text(
        help_text,
        reply_markup=help_back_markup(_),
        disable_web_page_preview=True
    )


# ───────────────────────────────────────── Back callback (only first page)

@app.on_callback_query(filters.regex(r"help_back") & ~BANNED_USERS)
@languageCB
async def help_back_cb(client: Client, CallbackQuery: types.CallbackQuery, _):
    await CallbackQuery.edit_message_text(
        _["help_1"].format(SUPPORT_CHAT),
        reply_markup=first_page(_),
        disable_web_page_preview=True
    )

# ────────────────────────────────────────────────  back to start panel ─

@app.on_callback_query(filters.regex("back_to_main") & ~BANNED_USERS)
@languageCB
async def back_to_main_cb(client: Client, CallbackQuery: types.CallbackQuery, _):
    out = private_panel(_)
    await CallbackQuery.edit_message_caption(
        _["start_2"].format(
            CallbackQuery.from_user.mention, app.mention
        ),
        reply_markup=InlineKeyboardMarkup(out)
    )
