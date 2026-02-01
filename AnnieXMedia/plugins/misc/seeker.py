# Authored By Certified Coders © 2025
import asyncio
from AnnieXMedia.misc import db
from AnnieXMedia.utils.database import get_active_chats, is_music_playing

async def timer():
    while True:
        # FIX: Sleep 5 seconds instead of 1 to save CPU
        await asyncio.sleep(5)
        try:
            active_chats = await get_active_chats()
            for chat_id in active_chats:
                if not await is_music_playing(chat_id):
                    continue
                playing = db.get(chat_id)
                if not playing:
                    continue
                
                # Safe key access
                if "seconds" not in playing[0]:
                    continue
                    
                duration = int(playing[0]["seconds"])
                if duration == 0:
                    continue
                
                # Add 5 seconds since we sleep for 5
                db[chat_id][0]["played"] += 5
                
                # Safety check if we exceeded duration
                if db[chat_id][0]["played"] >= duration:
                    db[chat_id][0]["played"] = duration
                    continue
        except Exception:
            pass

asyncio.create_task(timer())
