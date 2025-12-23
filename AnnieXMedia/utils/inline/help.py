# Authored By Certified Coders © 2025

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from AnnieXMedia import app


def generate_help_buttons(_, start: int, end: int):
    """Create a grid of three buttons per row (single page only)."""
    buttons, per_row = [], 2

    for idx, i in enumerate(range(start, end + 1)):
        if idx % per_row == 0:
            buttons.append([])

        buttons[-1].append(
            InlineKeyboardButton(
                text=_[f"H_B_{i}"],
                callback_data=f"help_callback hb{i}_p1"
            )
        )

    return buttons


def first_page(_):
    buttons = generate_help_buttons(_, 1, 4)

    buttons.append(
        [
            InlineKeyboardButton(text="Back", callback_data="back_to_main"),
            InlineKeyboardButton(text=_["CLOSE_BUTTON"], callback_data="close"),
        ]
    )

    return InlineKeyboardMarkup(buttons)


def help_back_markup(_):
    """Back always returns to first page."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    text=_["BACK_BUTTON"],
                    callback_data="help_back"
                ),
                InlineKeyboardButton(
                    text=_["CLOSE_BUTTON"],
                    callback_data="close"
                ),
            ]
        ]
    )


def private_help_panel(_):
    return [
        [
            InlineKeyboardButton(
                text=_["S_B_3"],
                url=f"https://t.me/{app.username}?start=help"
            ),
        ],
    ]
