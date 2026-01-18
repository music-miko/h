import time
import logging
import asyncio
import json
import os
import datetime

from pyrogram import filters
from pyrogram.enums import ChatMembersFilter
from pyrogram.errors import FloodWait, RPCError, MessageNotModified
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

from AnnieXMedia import app
from AnnieXMedia.misc import SUDOERS
from AnnieXMedia.utils.database import (
    get_active_chats,
    get_authuser_names,
    get_served_chats,
    get_served_users,
)
from AnnieXMedia.utils.formatters import alpha_to_int
from config import adminlist

# Logger config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Broadcast")

SEMAPHORE = asyncio.Semaphore(25) # Slightly reduced for stability
BROADCAST_FILE = "broadcast_state.json"
BROADCAST_LOCK = asyncio.Lock() # Prevents multiple concurrent runs
CANCEL_BROADCAST = False # Flag to stop execution

# ---------------------------------------------------------------------------------
# Utils: Time Formatting
# ---------------------------------------------------------------------------------

def format_time(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))

def get_readable_time(seconds: int) -> str:
    count = 0
    ping_time = ""
    time_list = []
    time_suffix_list = ["s", "m", "h", "days"]

    while count < 4:
        count += 1
        remainder, result = divmod(seconds, 60) if count < 3 else divmod(seconds, 24)
        if seconds == 0 and remainder == 0:
            break
        time_list.append(int(result))
        seconds = int(remainder)

    for x in range(len(time_list)):
        time_list[x] = str(time_list[x]) + time_suffix_list[x]
    if len(time_list) == 4:
        ping_time += time_list.pop() + ", "

    time_list.reverse()
    ping_time += ":".join(time_list)
    return ping_time

# ---------------------------------------------------------------------------------
# State Management
# ---------------------------------------------------------------------------------

def save_checkpoint(data):
    with open(BROADCAST_FILE, "w") as f:
        json.dump(data, f, indent=4)

def load_checkpoint():
    if os.path.exists(BROADCAST_FILE):
        try:
            with open(BROADCAST_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    return None

def clear_checkpoint():
    if os.path.exists(BROADCAST_FILE):
        os.remove(BROADCAST_FILE)

# ---------------------------------------------------------------------------------
# Core Broadcast Logic
# ---------------------------------------------------------------------------------

async def run_broadcast(data, status_message=None):
    global CANCEL_BROADCAST
    
    # Acquire lock to ensure only one runs at a time
    if BROADCAST_LOCK.locked():
        logger.warning("Broadcast triggered but lock is active.")
        return

    async with BROADCAST_LOCK:
        CANCEL_BROADCAST = False
        
        targets = data["targets"]
        mode = data["mode"]
        content_chat_id = data["content_chat"]
        content_msg_id = data["content_msg"]
        stats = data["stats"]
        initiator_id = data.get("initiator")
        start_time = data.get("start_time", time.time()) # Use saved time if resuming
        
        # Validate content
        try:
            content = await app.get_messages(content_chat_id, content_msg_id)
            if not content:
                raise ValueError("Message not found")
        except Exception as e:
            err = f"❌ <b>Error:</b> Content message not found. Broadcast cancelled.\nReason: {e}"
            if status_message: await status_message.edit_text(err)
            elif initiator_id: await app.send_message(initiator_id, err)
            clear_checkpoint()
            return

        sent_users = stats.get("sent_users", 0)
        sent_chats = stats.get("sent_chats", 0)
        failed = stats.get("failed", 0)
        total_targets_initial = len(targets)
        processed_count = 0
        
        # For Speed Calculation
        loop_start_time = time.time()
        
        async def deliver(chat_id, is_user, retries=3):
            nonlocal sent_users, sent_chats, failed
            async with SEMAPHORE:
                try:
                    if mode == "forward":
                        await content.forward(chat_id)
                    else:
                        await content.copy(chat_id)
                    
                    if is_user: sent_users += 1
                    else: sent_chats += 1
                    return True
                except FloodWait as e:
                    await asyncio.sleep(min(e.value, 60))
                    if retries > 0:
                        return await deliver(chat_id, is_user, retries - 1)
                    failed += 1
                except Exception:
                    failed += 1
                return False

        # --- Batch Loop ---
        for i in range(0, len(targets), 100):
            # Check Cancellation
            if CANCEL_BROADCAST:
                if status_message: await status_message.edit_text("🛑 <b>Broadcast Cancelled by Admin.</b>")
                clear_checkpoint()
                return

            batch = targets[i:i + 100]
            tasks = [deliver(t, t > 0) for t in batch]
            await asyncio.gather(*tasks)
            
            processed_count += len(batch)

            # Update State
            data["stats"] = {"sent_users": sent_users, "sent_chats": sent_chats, "failed": failed}
            data["targets"] = targets[processed_count:]
            save_checkpoint(data)

            # Update Status & Calculate Time
            if status_message:
                elapsed = time.time() - start_time
                # Speed = processed / elapsed
                # Note: This average speed includes previous sessions if resuming, 
                # but calculating current session speed is often more accurate for ETC.
                # Here we use total elapsed for simplicity.
                speed = (sent_users + sent_chats + failed) / elapsed if elapsed > 0 else 1
                remaining = len(data["targets"])
                etc_seconds = remaining / speed if speed > 0 else 0
                etc_str = get_readable_time(etc_seconds)
                
                try:
                    await status_message.edit_text(
                        f"📢 <b>Broadcast In Progress...</b>\n\n"
                        f"➤ <b>Mode:</b> `{mode}`\n"
                        f"✅ <b>Success:</b> `{sent_users + sent_chats}`\n"
                        f"❌ <b>Failed:</b> `{failed}`\n"
                        f"⏳ <b>Remaining:</b> `{remaining}`\n\n"
                        f"🚀 <b>Speed:</b> `{round(speed, 1)} msg/s`\n"
                        f"⏱ <b>ETC:</b> `{etc_str}`"
                    )
                except (MessageNotModified, RPCError):
                    pass
                except Exception:
                    status_message = None

            await asyncio.sleep(2) # Breath

        # --- Finish ---
        clear_checkpoint()
        total_time = get_readable_time(time.time() - start_time)
        
        final_text = (
            f"✅ <b>Broadcast Completed</b>\n\n"
            f"➤ <b>Mode:</b> `{mode}`\n"
            f"👤 <b>Users:</b> `{sent_users}`\n"
            f"👥 <b>Chats:</b> `{sent_chats}`\n"
            f"❌ <b>Failed:</b> `{failed}`\n\n"
            f"⏱ <b>Total Time:</b> `{total_time}`"
        )
        
        if status_message:
            try:
                await status_message.edit_text(final_text)
            except:
                pass
        elif initiator_id:
            try:
                await app.send_message(initiator_id, final_text)
            except:
                pass

# ---------------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------------

@app.on_message(filters.command("broadcast") & SUDOERS)
async def broadcast_command(client, message: Message):
    # Lock Check
    if BROADCAST_LOCK.locked():
        return await message.reply_text("⚠ <b>A broadcast is already running!</b>\nWait for it to finish or use /cancelbroadcast.")
    
    existing_state = load_checkpoint()
    if existing_state:
        return await message.reply_text(
            "⚠ <b>Unfinished Broadcast Found!</b>\n\n"
            "Use /cancelbroadcast to delete it or check your saved messages to resume."
        )

    command = message.text.lower()
    mode = "forward" if "-forward" in command else "copy"
    target_ids = []

    # Target selection
    if "-all" in command:
        users = await get_served_users()
        chats = await get_served_chats()
        target_ids = [u["user_id"] for u in users] + [c["chat_id"] for c in chats]
    elif "-users" in command:
        users = await get_served_users()
        target_ids = [u["user_id"] for u in users]
    elif "-chats" in command:
        chats = await get_served_chats()
        target_ids = [c["chat_id"] for c in chats]
    else:
        return await message.reply_text("❗ Usage: /broadcast -all/-users/-chats [-forward]")

    if not target_ids:
        return await message.reply_text("⚠ No recipients found.")

    if message.reply_to_message:
        content_msg = message.reply_to_message
    else:
        return await message.reply_text("📝 Reply to a message to broadcast.")

    # Save State
    state_data = {
        "targets": target_ids,
        "mode": mode,
        "content_chat": content_msg.chat.id,
        "content_msg": content_msg.id,
        "stats": {"sent_users": 0, "sent_chats": 0, "failed": 0},
        "initiator": message.from_user.id,
        "start_time": time.time()
    }
    save_checkpoint(state_data)

    status_msg = await message.reply_text(f"📢 <b>Broadcast Started</b>\nTargets: {len(target_ids)}")
    await run_broadcast(state_data, status_msg)

@app.on_message(filters.command("cancelbroadcast") & SUDOERS)
async def cancel_broadcast_cmd(client, message: Message):
    global CANCEL_BROADCAST
    
    if os.path.exists(BROADCAST_FILE):
        CANCEL_BROADCAST = True
        clear_checkpoint()
        await message.reply_text("🛑 <b>Broadcast Cancelled.</b>\nState file deleted and process stopping.")
    else:
        await message.reply_text("✅ No active broadcast found to cancel.")

# ---------------------------------------------------------------------------------
# Auto-Resume Logic & Callback Handling
# ---------------------------------------------------------------------------------

@app.on_callback_query(filters.regex(r"^(resume_broadcast|cancel_broadcast)$") & SUDOERS)
async def broadcast_callback(client, query: CallbackQuery):
    data = query.data
    
    if data == "cancel_broadcast":
        clear_checkpoint()
        await query.message.edit_text("🛑 Broadcast cancelled and queue deleted.")
    
    elif data == "resume_broadcast":
        state = load_checkpoint()
        if not state:
            return await query.answer("Broadcast finished or file invalid.", show_alert=True)
        
        await query.answer("Resuming...")
        await query.message.edit_text("♻ <b>Resuming Broadcast...</b>")
        
        # Start running
        await run_broadcast(state, query.message)

async def auto_resume_check():
    """Run on startup to check for crashes."""
    await asyncio.sleep(10) # Wait for connection
    
    state = load_checkpoint()
    if state:
        logger.info("Found incomplete broadcast. Sending alert to owner...")
        
        # Calculate quick stats for the message
        total_done = state["stats"]["sent_users"] + state["stats"]["sent_chats"]
        remaining = len(state["targets"])
        
        text = (
            "⚠ <b>Broadcast Interrupted!</b>\n"
            "The bot restarted while a broadcast was running.\n\n"
            f"✅ <b>Completed:</b> {total_done}\n"
            f"⏳ <b>Remaining:</b> {remaining}\n\n"
            "What would you like to do?"
        )
        
        buttons = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("▶ Resume", callback_data="resume_broadcast"),
                InlineKeyboardButton("🛑 Cancel", callback_data="cancel_broadcast")
            ]
        ])
        
        # Hardcoded ID as requested
        TARGET_ID = 89891145
        
        try:
            await app.send_message(TARGET_ID, text, reply_markup=buttons)
        except Exception as e:
            logger.error(f"Failed to send auto-resume message to {TARGET_ID}: {e}")

# Adminlist Auto-cleaner (Keep existing)
async def auto_clean():
    while True:
        await asyncio.sleep(10)
        try:
            chats = await get_active_chats()
            for chat_id in chats:
                if chat_id not in adminlist:
                    adminlist[chat_id] = []
                async for member in app.get_chat_members(chat_id, filter=ChatMembersFilter.ADMINISTRATORS):
                    if getattr(member, "privileges", None) and member.privileges.can_manage_video_chats:
                        adminlist[chat_id].append(member.user.id)
                for username in await get_authuser_names(chat_id):
                    user_id = await alpha_to_int(username)
                    adminlist[chat_id].append(user_id)
        except Exception as e:
            logger.warning(f"AutoClean error: {e}")

# Start Background Tasks
asyncio.create_task(auto_resume_check())
