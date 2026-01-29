import time
import logging
import asyncio
import json
import os
import datetime
from functools import partial

from pyrogram import filters
from pyrogram.enums import ChatMembersFilter
from pyrogram.errors import (
    FloodWait, 
    RPCError, 
    InputUserDeactivated, 
    UserIsBlocked, 
    PeerIdInvalid
)
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

# ---------------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------------

# CONCURRENCY: 15 is a "Safe" sweet spot. 
# It is fast enough but less likely to hit FloodWait than 20.
SEMAPHORE = asyncio.Semaphore(15) 

# BATCH: Save progress every 200 messages. 
# Since saving is now "Light" (just a number), we can save more often for safety.
BATCH_SIZE = 200

# FILES
STATE_FILE = "broadcast_state.json"      # Stores just the Progress Number (Tiny)
TARGETS_FILE = "broadcast_targets.json"  # Stores the ID List (Saved Once)
FAILED_FILE = "broadcast_failed.json"    # Stores Blocked IDs

BROADCAST_LOCK = asyncio.Lock()
CANCEL_BROADCAST = False 
FAILED_IDS = set()

# ---------------------------------------------------------------------------------
# File I/O Helpers (Async & Light)
# ---------------------------------------------------------------------------------

def get_readable_time(seconds: int) -> str:
    count = 0
    time_list = []
    time_suffix_list = ["s", "m", "h", "days"]
    while count < 4:
        count += 1
        remainder, result = divmod(seconds, 60) if count < 3 else divmod(seconds, 24)
        if seconds == 0 and remainder == 0: break
        time_list.append(int(result))
        seconds = int(remainder)
    for x in range(len(time_list)):
        time_list[x] = str(time_list[x]) + time_suffix_list[x]
    if len(time_list) == 4: time_list.pop()
    time_list.reverse()
    return ":".join(time_list)

async def write_json_async(filename, data):
    """Runs file write in a separate thread to avoid blocking the bot."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, partial(json_dump_sync, filename, data))

def json_dump_sync(filename, data):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

def load_json_sync(filename):
    if os.path.exists(filename):
        try:
            with open(filename, "r") as f:
                return json.load(f)
        except: return None
    return None

# --- FAILED LIST MANAGEMENT ---

def load_failed_list():
    global FAILED_IDS
    data = load_json_sync(FAILED_FILE)
    if data: FAILED_IDS = set(data)

async def save_failed_list():
    await write_json_async(FAILED_FILE, list(FAILED_IDS))

load_failed_list()

# ---------------------------------------------------------------------------------
# Core Broadcast Logic
# ---------------------------------------------------------------------------------

async def run_broadcast(state, targets, status_message=None):
    global CANCEL_BROADCAST
    
    if BROADCAST_LOCK.locked():
        return

    async with BROADCAST_LOCK:
        CANCEL_BROADCAST = False
        
        # Load details from state
        mode = state["mode"]
        content_chat_id = state["content_chat"]
        content_msg_id = state["content_msg"]
        initiator_id = state.get("initiator")
        start_index = state.get("current_index", 0)
        start_time = state.get("start_time", time.time())
        
        # Stats counters
        stats = state.get("stats", {"sent": 0, "failed": 0, "skipped": 0})
        sent_count = stats["sent"]
        failed_count = stats["failed"]
        skipped_count = stats["skipped"]

        # Validate Content
        try:
            content = await app.get_messages(content_chat_id, content_msg_id)
            if not content: raise ValueError
        except:
            if status_message: await status_message.edit_text("❌ <b>Error:</b> Content message deleted.")
            return

        # Prepare List Slice
        # We only process from the current index onwards
        remaining_targets = targets[start_index:]
        total_targets = len(targets)

        async def deliver(chat_id):
            nonlocal sent_count, failed_count, skipped_count
            
            # Fast Skip
            if chat_id in FAILED_IDS:
                skipped_count += 1
                return

            try:
                async with SEMAPHORE:
                    if mode == "forward":
                        await content.forward(chat_id)
                    else:
                        await content.copy(chat_id)
                    sent_count += 1

            except FloodWait as e:
                # If floodwait is HUGE (blocked), fail. If small, wait.
                if e.value > 60:
                    failed_count += 1
                else:
                    await asyncio.sleep(e.value)
                    # Retry once after sleep
                    try:
                        if mode == "forward": await content.forward(chat_id)
                        else: await content.copy(chat_id)
                        sent_count += 1
                    except:
                        failed_count += 1

            except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
                FAILED_IDS.add(chat_id) 
                failed_count += 1
            except Exception:
                failed_count += 1

        # --- The Loop ---
        # Process in batches of BATCH_SIZE
        i = 0
        while i < len(remaining_targets):
            if CANCEL_BROADCAST:
                if status_message: await status_message.edit_text("🛑 <b>Cancelled.</b>")
                # Remove state but KEEP targets (optional, or remove both)
                if os.path.exists(STATE_FILE): os.remove(STATE_FILE)
                return

            batch = remaining_targets[i : i + BATCH_SIZE]
            
            # Create tasks
            tasks = [deliver(chat_id) for chat_id in batch]
            
            # Run batch
            await asyncio.gather(*tasks)

            # Update Index
            current_real_index = start_index + i + len(batch)
            
            # Update State Data
            new_state = {
                "mode": mode,
                "content_chat": content_chat_id,
                "content_msg": content_msg_id,
                "initiator": initiator_id,
                "start_time": start_time,
                "current_index": current_real_index,
                "stats": {
                    "sent": sent_count,
                    "failed": failed_count,
                    "skipped": skipped_count
                }
            }

            # FAST SAVE: We only save the small state dict, not the huge list
            await write_json_async(STATE_FILE, new_state)
            await save_failed_list()

            # UI Update
            if status_message:
                elapsed = time.time() - start_time
                if elapsed == 0: elapsed = 1
                total_done = sent_count + failed_count + skipped_count
                speed = (total_done - stats.get("skipped", 0)) / elapsed # exclude skips from speed calc mostly
                if speed <= 0: speed = 0.1
                
                remaining = total_targets - current_real_index
                eta = get_readable_time(remaining / speed)

                try:
                    await status_message.edit_text(
                        f"📢 <b>Broadcast Running...</b>\n\n"
                        f"✅ <b>Sent:</b> `{sent_count}`\n"
                        f"❌ <b>Failed:</b> `{failed_count}`\n"
                        f"⏩ <b>Skipped:</b> `{skipped_count}`\n"
                        f"📊 <b>Progress:</b> `{current_real_index}/{total_targets}`\n\n"
                        f"🚀 <b>Speed:</b> `{round(speed, 1)} msg/s`\n"
                        f"⏱ <b>ETA:</b> `{eta}`"
                    )
                except: pass
            
            # Move index
            i += BATCH_SIZE

        # --- Completed ---
        if os.path.exists(STATE_FILE): os.remove(STATE_FILE)
        if os.path.exists(TARGETS_FILE): os.remove(TARGETS_FILE)
        
        final_text = (
            f"✅ <b>Broadcast Finished!</b>\n\n"
            f"👥 <b>Total Users:</b> `{total_targets}`\n"
            f"✅ <b>Successful:</b> `{sent_count}`\n"
            f"❌ <b>Failed:</b> `{failed_count}`\n"
            f"⏩ <b>Skipped (Blocked):</b> `{skipped_count}`\n"
            f"⏱ <b>Time Taken:</b> `{get_readable_time(time.time() - start_time)}`"
        )
        
        if status_message: await status_message.edit_text(final_text)
        elif initiator_id: 
            try: await app.send_message(initiator_id, final_text)
            except: pass

# ---------------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------------

@app.on_message(filters.command("broadcast") & SUDOERS)
async def broadcast_command(client, message: Message):
    if BROADCAST_LOCK.locked():
        return await message.reply_text("⚠ <b>Broadcast is already running!</b>")

    # Check for existing session
    if os.path.exists(STATE_FILE) and os.path.exists(TARGETS_FILE):
        if "-new" not in message.text.lower():
            return await message.reply_text("⚠ <b>Found unfinished broadcast!</b>\nUse `/resume_broadcast` to continue or `/broadcast -new` to restart.")

    if not message.reply_to_message:
        return await message.reply_text("❗ Please reply to the message you want to broadcast.")

    query = message.text.lower()
    mode = "forward" if "-forward" in query else "copy"

    msg = await message.reply_text("⏳ <b>Fetching users...</b>")

    # Fetch Data
    users_list = []
    chats_list = []
    
    if "-all" in query:
        users_list = await get_served_users()
        chats_list = await get_served_chats()
    elif "-chats" in query:
        chats_list = await get_served_chats()
    else:
        # Default to users only if not specified
        users_list = await get_served_users()

    # Extract IDs safely
    # Assumes get_served_users returns dicts like {'user_id': 123}
    # Adjust key names if your database functions differ
    raw_targets = []
    for u in users_list:
        raw_targets.append(u.get("user_id") if isinstance(u, dict) else u)
    for c in chats_list:
        raw_targets.append(c.get("chat_id") if isinstance(c, dict) else c)

    # Unique & Clean
    targets = list(set(raw_targets))
    
    if not targets:
        return await msg.edit_text("❌ <b>No targets found in database.</b>")

    # Save Static List ONCE
    await write_json_async(TARGETS_FILE, targets)

    # Init State
    state = {
        "mode": mode,
        "content_chat": message.reply_to_message.chat.id,
        "content_msg": message.reply_to_message.id,
        "initiator": message.from_user.id,
        "start_time": time.time(),
        "current_index": 0,
        "stats": {"sent": 0, "failed": 0, "skipped": 0}
    }
    await write_json_async(STATE_FILE, state)

    await run_broadcast(state, targets, msg)

@app.on_message(filters.command("resume_broadcast") & SUDOERS)
async def resume_broadcast(client, message: Message):
    if not (os.path.exists(STATE_FILE) and os.path.exists(TARGETS_FILE)):
        return await message.reply_text("❌ <b>No broadcast to resume.</b>")
    
    msg = await message.reply_text("♻ <b>Resuming Broadcast...</b>")
    
    # Load data
    state = load_json_sync(STATE_FILE)
    targets = load_json_sync(TARGETS_FILE)
    
    if not state or not targets:
        return await msg.edit_text("❌ <b>Save files are corrupted.</b> Start a new broadcast.")
        
    await run_broadcast(state, targets, msg)

@app.on_message(filters.command("cancelbroadcast") & SUDOERS)
async def cancel_broadcast_cmd(client, message: Message):
    global CANCEL_BROADCAST
    CANCEL_BROADCAST = True
    await message.reply_text("🛑 <b>Stopping broadcast...</b>")

@app.on_message(filters.command("clearfailed") & SUDOERS)
async def clear_failed(client, message: Message):
    global FAILED_IDS
    FAILED_IDS.clear()
    if os.path.exists(FAILED_FILE): os.remove(FAILED_FILE)
    await message.reply_text("✅ Failed cache cleared.")

# ---------------------------------------------------------------------------------
# Auto-Recovery on Restart
# ---------------------------------------------------------------------------------

async def auto_resume_check():
    await asyncio.sleep(5)
    if os.path.exists(STATE_FILE) and os.path.exists(TARGETS_FILE):
        try:
            state = load_json_sync(STATE_FILE)
            targets = load_json_sync(TARGETS_FILE)
            total = len(targets)
            current = state.get("current_index", 0)
            
            text = (
                "⚠ <b>Broadcast Interrupted!</b>\n"
                f"📊 Progress: {current}/{total}\n"
                "Do you want to resume?"
            )
            buttons = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Resume", callback_data="resume_broadcast"),
                InlineKeyboardButton("❌ Abort", callback_data="cancel_broadcast")
            ]])
            # Change this ID to your ID or dynamic owner ID
            await app.send_message(89891145, text, reply_markup=buttons)
        except: pass

@app.on_callback_query(filters.regex(r"^(resume_broadcast|cancel_broadcast)$") & SUDOERS)
async def broadcast_callback(client, query: CallbackQuery):
    global CANCEL_BROADCAST
    if query.data == "cancel_broadcast":
        CANCEL_BROADCAST = True
        if os.path.exists(STATE_FILE): os.remove(STATE_FILE)
        if os.path.exists(TARGETS_FILE): os.remove(TARGETS_FILE)
        await query.message.edit_text("🛑 <b>Broadcast Cancelled.</b>")
    else:
        state = load_json_sync(STATE_FILE)
        targets = load_json_sync(TARGETS_FILE)
        if state and targets:
            await query.message.edit_text("♻ <b>Resuming...</b>")
            await run_broadcast(state, targets, query.message)
        else:
            await query.message.edit_text("❌ Data expired.")


async def auto_clean():
    while True:
        await asyncio.sleep(10)
        try:
            chats = await get_active_chats()
            for chat_id in chats:
                if chat_id not in adminlist: adminlist[chat_id] = []
                async for member in app.get_chat_members(chat_id, filter=ChatMembersFilter.ADMINISTRATORS):
                    if getattr(member, "privileges", None) and member.privileges.can_manage_video_chats:
                        adminlist[chat_id].append(member.user.id)
                for username in await get_authuser_names(chat_id):
                    user_id = await alpha_to_int(username)
                    adminlist[chat_id].append(user_id)
        except: pass


asyncio.create_task(auto_resume_check())
asyncio.create_task(auto_clean())
