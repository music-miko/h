# Authored By Certified Coders © 2025
import asyncio
from pyrogram import Client

import config

from ..logging import LOGGER

assistants = []
assistantids = []

GROUPS_TO_JOIN = [
    "ArcChatz",
    "ArcBotz",
    "ArcUpdates",
]


# Initialize userbots
class Userbot:
    def __init__(self):
        self.one = Client(
            "AnnieAssis1",
            config.API_ID,
            config.API_HASH,
            session_string=str(config.STRING1),
            no_updates=True,
        )
        self.two = Client(
            "AnnieAssis2",
            config.API_ID,
            config.API_HASH,
            session_string=str(config.STRING2),
            no_updates=True,
        )
        self.three = Client(
            "AnnieAssis3",
            config.API_ID,
            config.API_HASH,
            session_string=str(config.STRING3),
            no_updates=True,
        )
        self.four = Client(
            "AnnieAssis4",
            config.API_ID,
            config.API_HASH,
            session_string=str(config.STRING4),
            no_updates=True,
        )
        self.five = Client(
            "AnnieAssis5",
            config.API_ID,
            config.API_HASH,
            session_string=str(config.STRING5),
            no_updates=True,
        )

    async def start_assistant(self, client: Client, index: int):
        string_attr = [
            config.STRING1,
            config.STRING2,
            config.STRING3,
            config.STRING4,
            config.STRING5,
        ][index - 1]
        if not string_attr:
            return

        try:
            await client.start()
            
            # FIX: Grab ID immediately so we know it's alive
            me = await client.get_me()
            client.id, client.name, client.username = me.id, me.first_name, me.username
            assistants.append(index)
            assistantids.append(me.id)

            for group in GROUPS_TO_JOIN:
                try:
                    await client.join_chat(group)
                except Exception:
                    pass

            try:
                await client.send_message(
                    config.LOGGER_ID, f"Assistant {index} Started"
                )
            except Exception:
                LOGGER(__name__).warning(
                    f"Assistant {index} started but can't access log group. Check perms!"
                )
                # FIX: REMOVED exit() so bot keeps running even if log message fails

            LOGGER(__name__).info(f"Assistant {index} Started as {client.name}")

        except Exception as e:
            LOGGER(__name__).error(f"Failed to start Assistant {index}: {e}")

    async def start(self):
        LOGGER(__name__).info("Starting Assistants...")
        # FIX: Parallel startup using asyncio.gather
        await asyncio.gather(
            self.start_assistant(self.one, 1),
            self.start_assistant(self.two, 2),
            self.start_assistant(self.three, 3),
            self.start_assistant(self.four, 4),
            self.start_assistant(self.five, 5),
        )

    async def stop(self):
        LOGGER(__name__).info("Stopping Assistants...")
        try:
            if config.STRING1: await self.one.stop()
            if config.STRING2: await self.two.stop()
            if config.STRING3: await self.three.stop()
            if config.STRING4: await self.four.stop()
            if config.STRING5: await self.five.stop()
        except Exception as e:
            LOGGER(__name__).error(f"Error while stopping assistants: {e}")
