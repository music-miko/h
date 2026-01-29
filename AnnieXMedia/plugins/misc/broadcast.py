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

# SPEED: Sends to 20 users simultaneously (Very Fast)
SEMAPHORE = asyncio.Semaphore(20) 

# PERFORMANCE: Only save to file every 500 messages (Prevents Lag)
BATCH_SIZE = 500

BROADCAST_FILE = "broadcast_state.json"
FAILED_FILE = "broadcast_failed.json"
BROADCAST_LOCK = asyncio.Lock()
CANCEL_BROADCAST = False 

# Cache for permanent failures (Blocked/Deleted users)
FAILED_IDS = set()

# ---------------------------------------------------------------------------------
# Utils
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

# NON-BLOCKING SAVE: Run file I/O in a separate thread to prevent bot lag
async def save_checkpoint_async(data):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, partial(save_sync, data))

def save_sync(data):
    with open(BROADCAST_FILE, "w") as f:
        json.dump(data, f, indent=4)

def load_checkpoint():
    if os.path.exists(BROADCAST_FILE):
        try:
            with open(BROADCAST_FILE, "r") as f:
                return json.load(f)
        except: return None
    return None

def clear_checkpoint():
    if os.path.exists(BROADCAST_FILE):
        os.remove(BROADCAST_FILE)

# --- FAILED LIST MANAGEMENT ---

def load_failed_list():
    """Load blocked/deleted users into memory to skip them later."""
    global FAILED_IDS
    if os.path.exists(FAILED_FILE):
        try:
            with open(FAILED_FILE, "r") as f:
                FAILED_IDS = set(json.load(f))
        except: pass

async def save_failed_list_async():
    """Save failed list in background thread."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, save_failed_sync)

def save_failed_sync():
    with open(FAILED_FILE, "w") as f:
        json.dump(list(FAILED_IDS), f)

# Load immediately on startup
load_failed_list()

# ---------------------------------------------------------------------------------
# Core Broadcast Logic
# ---------------------------------------------------------------------------------

async def run_broadcast(data, status_message=None):
    global CANCEL_BROADCAST
    
    # Ensure exclusive access
    if BROADCAST_LOCK.locked():
        return

    async with BROADCAST_LOCK:
        CANCEL_BROADCAST = False
        
        targets = data["targets"]
        mode = data["mode"]
        content_chat_id = data["content_chat"]
        content_msg_id = data["content_msg"]
        stats = data["stats"]
        initiator_id = data.get("initiator")
        start_time = data.get("start_time", time.time())
        
        # Validate Content
        try:
            content = await app.get_messages(content_chat_id, content_msg_id)
            if not content: raise ValueError
        except:
            err = "❌ <b>Error:</b> Content message not found (maybe deleted?)."
            if status_message: await status_message.edit_text(err)
            clear_checkpoint()
            return

        sent_users = stats.get("sent_users", 0)
        sent_chats = stats.get("sent_chats", 0)
        failed = stats.get("failed", 0)
        skipped = stats.get("skipped", 0)
        
        async def deliver(chat_id, is_user, retries=0):
            nonlocal sent_users, sent_chats, failed, skipped
            
            # 1. OPTIMIZATION: Skip known dead IDs immediately
            if chat_id in FAILED_IDS:
                skipped += 1
                return False

            async with SEMAPHORE:
                try:
                    # 2. FIX: Strict 10s timeout. If it takes longer, kill it.
                    if mode == "forward":
                        await asyncio.wait_for(content.forward(chat_id), timeout=10)
                    else:
                        await asyncio.wait_for(content.copy(chat_id), timeout=10)
                    
                    if is_user: sent_users += 1
                    else: sent_chats += 1
                    return True

                except FloodWait as e:
                    # Only wait if short, otherwise fail to keep queue moving
                    if e.value < 45:
                        await asyncio.sleep(e.value)
                        if retries > 0:
                            return await deliver(chat_id, is_user, retries - 1)
                    failed += 1

                # 3. FIX: Catch Permanent Errors -> Add to Blacklist
                except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
                    FAILED_IDS.add(chat_id) 
                    failed += 1
                
                except Exception:
                    failed += 1
                
                return False

        # --- Iteration Loop ---
        while targets:
            if CANCEL_BROADCAST:
                if status_message: await status_message.edit_text("🛑 <b>Cancelled.</b>")
                clear_checkpoint()
                return

            # Take the next batch (Slicing is safe here)
            batch = targets[:BATCH_SIZE]
            # Remove them from the main list immediately (in memory)
            targets = targets[BATCH_SIZE:]

            # Launch 500 tasks (But Semaphore limits execution to 20 at a time)
            tasks = [deliver(t, t > 0) for t in batch]
            await asyncio.gather(*tasks)
            
            # Update Stats in Data
            data["stats"] = {
                "sent_users": sent_users, 
                "sent_chats": sent_chats, 
                "failed": failed,
                "skipped": skipped
            }
            # Update Remaining Targets in Data
            data["targets"] = targets
            
            # Async Save (Prevents Lag)
            await save_checkpoint_async(data)
            await save_failed_list_async()

            # UI Update (Throttle edits to avoid rate limits)
            if status_message:
                elapsed = time.time() - start_time
                if elapsed == 0: elapsed = 1
                
                total_processed = sent_users + sent_chats + failed + skipped
                speed = total_processed / elapsed
                remaining = len(targets)
                
                if speed == 0: speed = 0.1
                etc_str = get_readable_time(remaining / speed)
                
                try:
                    await status_message.edit_text(
                        f"📢 <b>Broadcast In Progress...</b>\n\n"
                        f"➤ <b>Mode:</b> `{mode}`\n"
                        f"✅ <b>Success:</b> `{sent_users + sent_chats}`\n"
                        f"❌ <b>Failed:</b> `{failed}`\n"
                        f"🗑 <b>Skipped:</b> `{skipped}`\n"
                        f"⏳ <b>Remaining:</b> `{remaining}`\n\n"
                        f"🚀 <b>Speed:</b> `{round(speed, 1)} msg/s`\n"
                        f"⏱ <b>ETC:</b> `{etc_str}`"
                    )
                except: pass

        # --- Finish ---
        clear_checkpoint()
        await save_failed_list_async()
        total_time = get_readable_time(time.time() - start_time)
        
        final_text = (
            f"✅ <b>Broadcast Completed</b>\n\n"
            f"➤ <b>Mode:</b> `{mode}`\n"
            f"👤 <b>Users:</b> `{sent_users}`\n"
            f"👥 <b>Chats:</b> `{sent_chats}`\n"
            f"❌ <b>Failed:</b> `{failed}`\n"
            f"🗑 <b>Skipped (Dead):</b> `{skipped}`\n\n"
            f"⏱ <b>Total Time:</b> `{total_time}`"
        )
        
        if status_message:
            try: await status_message.edit_text(final_text)
            except: pass
        elif initiator_id:
            try: await app.send_message(initiator_id, final_text)
            except: pass

# ---------------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------------

@app.on_message(filters.command("broadcast") & SUDOERS)
async def broadcast_command(client, message: Message):
    if BROADCAST_LOCK.locked():
        return await message.reply_text("⚠ <b>Broadcast Running!</b> Use /cancelbroadcast to stop it.")
    
    existing_state = load_checkpoint()
    if existing_state and "-new" not in message.text.lower():
        return await message.reply_text("⚠ <b>Unfinished Broadcast Found!</b>\nUse `/resume_broadcast` or `/broadcast -new`.")

    command = message.text.lower()
    mode = "forward" if "-forward" in command else "copy"
    
    # Target Loading
    if "-all" in command:
        users = await get_served_users()
        chats = await get_served_chats()
        raw_targets = [u["user_id"] for u in users] + [c["chat_id"] for c in chats]
    elif "-users" in command:
        users = await get_served_users()
        raw_targets = [u["user_id"] for u in users]
    elif "-chats" in command:
        chats = await get_served_chats()
        raw_targets = [c["chat_id"] for c in chats]
    else:
        return await message.reply_text("❗ Usage: /broadcast -all/-users/-chats [-forward]")

    if not raw_targets:
        return await message.reply_text("⚠ No recipients found.")

    # FIX: DEDUPLICATION
    # Convert to set and back to list to ensure NO DUPLICATES
    unique_targets = list(set(raw_targets))

    # PRE-FILTERING: Calculate skipped users BEFORE starting
    valid_targets = []
    skipped_count = 0
    for t in unique_targets:
        if t in FAILED_IDS:
            skipped_count += 1
        else:
            valid_targets.append(t)

    if not valid_targets:
        return await message.reply_text(f"✅ All {skipped_count} targets are known dead/blocked. Nothing to send.")

    if message.reply_to_message:
        content_msg = message.reply_to_message
    else:
        return await message.reply_text("📝 Reply to a message.")

    # Initialize State
    state_data = {
        "targets": valid_targets,
        "mode": mode,
        "content_chat": content_msg.chat.id,
        "content_msg": content_msg.id,
        "stats": {"sent_users": 0, "sent_chats": 0, "failed": 0, "skipped": skipped_count},
        "initiator": message.from_user.id,
        "start_time": time.time()
    }
    
    # Use Async Save
    await save_checkpoint_async(state_data)

    status_msg = await message.reply_text(f"📢 <b>Broadcast Started</b>\nTargets: {len(valid_targets)}\nSkipped (Dead): {skipped_count}")
    await run_broadcast(state_data, status_msg)

@app.on_message(filters.command("cancelbroadcast") & SUDOERS)
async def cancel_broadcast_cmd(client, message: Message):
    global CANCEL_BROADCAST
    if os.path.exists(BROADCAST_FILE):
        CANCEL_BROADCAST = True
        clear_checkpoint()
        await message.reply_text("🛑 <b>Broadcast Cancelled.</b>")
    else:
        await message.reply_text("✅ No active broadcast.")

@app.on_message(filters.command("resume_broadcast") & SUDOERS)
async def manual_resume(client, message: Message):
    state = load_checkpoint()
    if not state: return await message.reply_text("✅ No pending broadcast.")
    msg = await message.reply_text("♻ <b>Resuming Broadcast...</b>")
    await run_broadcast(state, msg)

@app.on_message(filters.command("clearfailed") & SUDOERS)
async def clear_failed_cache(client, message: Message):
    global FAILED_IDS
    FAILED_IDS.clear()
    if os.path.exists(FAILED_FILE):
        os.remove(FAILED_FILE)
    await message.reply_text("✅ Failed cache cleared.")

# ---------------------------------------------------------------------------------
# Auto-Recovery
# ---------------------------------------------------------------------------------

@app.on_callback_query(filters.regex(r"^(resume_broadcast|cancel_broadcast)$") & SUDOERS)
async def broadcast_callback(client, query: CallbackQuery):
    if query.data == "cancel_broadcast":
        clear_checkpoint()
        await query.message.edit_text("🛑 Cancelled.")
    elif query.data == "resume_broadcast":
        state = load_checkpoint()
        if not state: return await query.answer("Expired.", show_alert=True)
        await query.answer("Resuming...")
        await query.message.edit_text("♻ <b>Resuming...</b>")
        await run_broadcast(state, query.message)

async def auto_resume_check():
    """Checks for crashes on restart."""
    await asyncio.sleep(10) # Wait for connection
    state = load_checkpoint()
    if state:
        logger.info("Found incomplete broadcast.")
        text = (
            "⚠ <b>Broadcast Interrupted!</b>\n\n"
            f"✅ Done: {state['stats']['sent_users'] + state['stats']['sent_chats']}\n"
            f"⏳ Left: {len(state['targets'])}\n\n"
            "Select action:"
        )
        buttons = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶ Resume", callback_data="resume_broadcast"),
            InlineKeyboardButton("🛑 Cancel", callback_data="cancel_broadcast")
        ]])
        
        # Notify the owner (Hardcoded ID)
        try: await app.send_message(89891145, text, reply_markup=buttons)
        except: pass

# Adminlist Auto-cleaner (Legacy)
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
