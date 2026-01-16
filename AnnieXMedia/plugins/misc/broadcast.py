import time
import logging
import asyncio
import json
import os

from pyrogram import filters
from pyrogram.enums import ChatMembersFilter
from pyrogram.errors import FloodWait, RPCError, MessageNotModified
from pyrogram.types import Message

from AnnieXMedia import app
from AnnieXMedia.misc import SUDOERS
from AnnieXMedia.utils.database import (
    get_active_chats,
    get_authuser_names,
    get_client,
    get_served_chats,
    get_served_users,
)
from AnnieXMedia.utils.decorators.language import language
from AnnieXMedia.utils.formatters import alpha_to_int
from config import adminlist

# Logger config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Broadcast")

SEMAPHORE = asyncio.Semaphore(30)
BROADCAST_FILE = "broadcast_state.json"

# ---------------------------------------------------------------------------------
# State Management Functions (Save/Load Progress)
# ---------------------------------------------------------------------------------

def save_checkpoint(data):
    """Saves the current broadcast state to a local JSON file."""
    with open(BROADCAST_FILE, "w") as f:
        json.dump(data, f, indent=4)

def load_checkpoint():
    """Loads the broadcast state from JSON if it exists."""
    if os.path.exists(BROADCAST_FILE):
        try:
            with open(BROADCAST_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    return None

def clear_checkpoint():
    """Deletes the state file after completion."""
    if os.path.exists(BROADCAST_FILE):
        os.remove(BROADCAST_FILE)

# ---------------------------------------------------------------------------------
# Core Broadcast Logic
# ---------------------------------------------------------------------------------

async def run_broadcast(data, status_message=None):
    """
    Main broadcast loop.
    data: dict containing 'targets', 'mode', 'content_chat', 'content_msg', 'stats', 'initiator'
    status_message: Message object to update progress (optional)
    """
    targets = data["targets"]
    mode = data["mode"]
    content_chat_id = data["content_chat"]
    content_msg_id = data["content_msg"]
    stats = data["stats"]
    initiator_id = data.get("initiator")
    
    # 1. Fetch the content message again
    try:
        content = await app.get_messages(content_chat_id, content_msg_id)
        if not content:
            raise ValueError("Message not found")
    except Exception as e:
        logger.error(f"Could not fetch content message: {e}")
        error_text = f"❌ <b>Error:</b> Could not fetch original message for broadcast.\nReason: {e}"
        if status_message:
            await status_message.edit_text(error_text)
        elif initiator_id:
            try:
                await app.send_message(initiator_id, error_text)
            except:
                pass
        clear_checkpoint() # Cannot proceed without content
        return

    sent_users = stats.get("sent_users", 0)
    sent_chats = stats.get("sent_chats", 0)
    failed = stats.get("failed", 0)
    
    # Delivery Helper Function
    async def deliver(chat_id, is_user, retries=3):
        nonlocal sent_users, sent_chats, failed
        async with SEMAPHORE:
            try:
                if mode == "forward":
                    await content.forward(chat_id)
                else:
                    await content.copy(chat_id)
                
                if is_user:
                    sent_users += 1
                else:
                    sent_chats += 1
                return True

            except FloodWait as e:
                wait_time = min(e.value, 120)
                await asyncio.sleep(wait_time)
                if retries > 0:
                    return await deliver(chat_id, is_user, retries - 1)
                failed += 1
            except Exception:
                failed += 1
            return False

    # 2. Process in Batches
    total_targets_initial = len(targets) # Remaining targets
    processed_count = 0
    last_update_time = time.time()

    # Iterate through targets in chunks of 100
    for i in range(0, len(targets), 100):
        batch = targets[i:i + 100]
        tasks = []
        
        for t in batch:
            # Simple heuristic: positive ID usually user, negative usually chat
            # If your DB returns ID only, this guess is standard. 
            is_user = t > 0 
            tasks.append(deliver(t, is_user))

        await asyncio.gather(*tasks)
        
        processed_count += len(batch)

        # -- SAVE CHECKPOINT --
        # Update in-memory stats
        data["stats"] = {
            "sent_users": sent_users,
            "sent_chats": sent_chats,
            "failed": failed
        }
        # Save remaining targets only (slicing off the ones we just did)
        # Note: We must be careful not to slice 'targets' variable directly inside loop if we use range on it
        # Strategy: update the data['targets'] to be the remainder
        remaining_targets = targets[processed_count:]
        data["targets"] = remaining_targets
        
        save_checkpoint(data)
        
        # Update Status Message (Every 8-10 seconds to avoid FloodWait)
        if status_message and (time.time() - last_update_time) > 8:
            try:
                await status_message.edit_text(
                    f"📢 <b>Broadcast In Progress...</b>\n\n"
                    f"➤ Mode: <code>{mode}</code>\n"
                    f"✅ Sent: <code>{sent_users + sent_chats}</code>\n"
                    f"❌ Failed: <code>{failed}</code>\n"
                    f"⏳ Remaining: <code>{len(remaining_targets)}</code>"
                )
                last_update_time = time.time()
            except (MessageNotModified, RPCError):
                pass
            except Exception:
                status_message = None # Stop trying if message deleted

        await asyncio.sleep(2.5) # Throttle

    # 3. Finish
    clear_checkpoint()
    
    final_text = (
        f"✅ <b>Broadcast Completed</b>\n\n"
        f"➤ Mode: <code>{mode}</code>\n"
        f"👤 Users Sent: <code>{sent_users}</code>\n"
        f"👥 Chats Sent: <code>{sent_chats}</code>\n"
        f"📦 Total Delivered: <code>{sent_users + sent_chats}</code>\n"
        f"❌ Failed: <code>{failed}</code>"
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
            
    logger.info(f"Broadcast finished. Success: {sent_users + sent_chats}, Failed: {failed}")


# ---------------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------------

@app.on_message(filters.command("broadcast") & SUDOERS)
async def broadcast_command(client, message: Message):
    # Check for unfinished broadcast
    existing_state = load_checkpoint()
    command = message.text.lower()
    
    if existing_state and "-new" not in command:
        return await message.reply_text(
            "⚠ <b>Unfinished Broadcast Found!</b>\n\n"
            "Bot found a broadcast that stopped unexpectedly.\n"
            "➤ <code>/resume_broadcast</code> to continue.\n"
            "➤ <code>/broadcast -new ...</code> to start fresh."
        )

    # Parse Arguments
    mode = "forward" if "-forward" in command else "copy"
    target_ids = []

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
        return await message.reply_text(
            "❗ Usage:\n"
            "/broadcast -all/-users/-chats [-forward]\n"
            "To ignore old broadcast: /broadcast -new -all ..."
        )

    if not target_ids:
        return await message.reply_text("⚠ No recipients found.")

    # Get Content
    if message.reply_to_message:
        content_msg = message.reply_to_message
        content_chat_id = content_msg.chat.id
        content_msg_id = content_msg.id
    else:
        return await message.reply_text("📝 Reply to a message to broadcast.")

    # Initial Save
    state_data = {
        "targets": target_ids,
        "mode": mode,
        "content_chat": content_chat_id,
        "content_msg": content_msg_id,
        "stats": {"sent_users": 0, "sent_chats": 0, "failed": 0},
        "initiator": message.from_user.id
    }
    
    save_checkpoint(state_data)

    status_msg = await message.reply_text(f"📢 <b>Broadcast Started</b>\nTargets: {len(target_ids)}")
    await run_broadcast(state_data, status_msg)


@app.on_message(filters.command("resume_broadcast") & SUDOERS)
async def manual_resume(client, message: Message):
    state = load_checkpoint()
    if not state:
        return await message.reply_text("✅ No pending broadcast found.")
    
    msg = await message.reply_text(
        f"♻ <b>Resuming Broadcast...</b>\n"
        f"Remaining Targets: {len(state['targets'])}"
    )
    await run_broadcast(state, msg)


# ---------------------------------------------------------------------------------
# Auto-Resume & Auto-Clean Tasks
# ---------------------------------------------------------------------------------

async def auto_resume_check():
    """Checks for pending broadcast on startup."""
    await asyncio.sleep(20) # Wait for bot to connect fully
    state = load_checkpoint()
    if state:
        logger.info("Found unfinished broadcast. Resuming automatically...")
        initiator = state.get("initiator")
        status_msg = None
        
        if initiator:
            try:
                status_msg = await app.send_message(
                    initiator, 
                    "⚠ <b>Bot Restarted.</b> Resuming incomplete broadcast..."
                )
            except:
                pass
        
        await run_broadcast(state, status_msg)

# Adminlist Auto-cleaner
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

# Start background tasks
asyncio.create_task(auto_resume_check())
# asyncio.create_task(auto_clean()) # Uncomment if you want this running
