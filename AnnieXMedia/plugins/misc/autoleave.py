
# Authored By Certified Coders © 2025
import pytz
import logging
import asyncio
from datetime import datetime, timedelta

from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait

import config
from AnnieXMedia import app
from AnnieXMedia.core.call import StreamController, autoend
from AnnieXMedia.utils.database import get_client, is_active_chat, is_autoend


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("AutoLeave")

# Constants
EXCLUDED_CHAT_IDS = {config.LOGGER_ID}
MAX_LEAVES_PER_RUN = 100
TIMEZONE = "Asia/Kolkata"
TARGET_HOUR = 4
TARGET_MINUTE = 40


def seconds_until_target_time(hour: int = TARGET_HOUR, minute: int = TARGET_MINUTE) -> float:
    """Calculate seconds remaining until the next target time (default: 4:40 AM IST)."""
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def leave_inactive_chats(client, client_num: int):
    """Make the client leave inactive chats with stability checks."""
    left_count = 0
    try:
        # Use a list to avoid modifying the iterator if dialogs change dynamically
        async for dialog in client.get_dialogs():
            chat = dialog.chat
            if chat.type in {ChatType.SUPERGROUP, ChatType.GROUP, ChatType.CHANNEL}:
                if chat.id in EXCLUDED_CHAT_IDS:
                    continue
                
                if left_count >= MAX_LEAVES_PER_RUN:
                    break
                
                if not await is_active_chat(chat.id):
                    try:
                        await client.leave_chat(chat.id)
                        logger.info(f"{client.me.first_name} left inactive chat: {chat.title} ({chat.id})")
                        left_count += 1
                        # Sleep to prevent hitting rate limits
                        await asyncio.sleep(1.5)
                    except FloodWait as e:
                        logger.warning(f"FloodWait hit. Sleeping for {e.value} seconds.")
                        await asyncio.sleep(e.value + 5)
                    except Exception as e:
                        logger.warning(f"Failed to leave chat {chat.title} ({chat.id}): {e}")
                        
    except Exception as e:
        logger.error(f"Assistant {client_num} failed to fetch dialogs: {e}")


async def auto_leave():
    """Run auto leave job daily at 4:40 AM IST."""
    if not config.AUTO_LEAVING_ASSISTANT:
        logger.info("AUTO_LEAVING_ASSISTANT is disabled. Exiting auto_leave task.")
        return

    logger.info("AutoLeave task started and running in background.")
    from AnnieXMedia.core.userbot import assistants

    while True:
        try:
            seconds_to_sleep = seconds_until_target_time()
            hrs, mins = divmod(seconds_to_sleep // 60, 60)
            logger.info(f"Sleeping for {int(hrs)}h {int(mins)}m until {TARGET_HOUR}:{TARGET_MINUTE} AM IST.")
            
            await asyncio.sleep(seconds_to_sleep)

            logger.info("Running cleanup of inactive chats...")
            
            # REPAIR: Added separate try-except for each assistant
            # This ensures if Assistant 1 crashes, Assistant 2 still cleans up.
            for num in assistants:
                try:
                    client = await get_client(num)
                    if client:
                        await leave_inactive_chats(client, num)
                except Exception as ex:
                    logger.error(f"Failed to run auto-leave for Assistant {num}: {ex}")
            
            logger.info("Cleanup complete. Sleeping again until next schedule.")
            
        except Exception as e:
            logger.error(f"Error in auto_leave loop: {e}")
            await asyncio.sleep(60) # Wait a bit before retrying if a crash occurs


async def auto_end():
    """Automatically ends streams if no one is listening, checking every 5 minutes."""
    while True:
        # Check every 10 seconds
        await asyncio.sleep(10)
        
        try:
            ender = await is_autoend()
            if not ender:
                continue

            # Iterate over list(autoend) to avoid RuntimeError
            for chat_id in list(autoend):
                timer = autoend.get(chat_id)
                if not timer:
                    # REPAIR: Clean up empty entries
                    autoend.pop(chat_id, None)
                    continue
                
                if datetime.now() > timer:
                    if not await is_active_chat(chat_id):
                        # REPAIR: Use .pop() instead of setting to {}
                        autoend.pop(chat_id, None)
                        continue
                    
                    # REPAIR: Use .pop() to correctly remove from tracking
                    autoend.pop(chat_id, None)
                    
                    try:
                        await StreamController.stop_stream(chat_id)
                    except Exception:
                        continue
                        
                    try:
                        # Updated message with emojis and /play suggestion
                        await app.send_message(
                            chat_id,
                            "👋 **Bot left the voice chat due to inactivity.**\n\n"
                            "❌ No one was listening for **120 Seconds**.\n"
                            "🎶 To play again: `/play [song name]`",
                        )
                    except Exception:
                        continue
        except Exception as e:
            logger.error(f"Error in auto_end task: {e}")


# Start the background tasks
asyncio.create_task(auto_leave())
asyncio.create_task(auto_end())
