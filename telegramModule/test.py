import asyncio

import asyncpg
from telethon import TelegramClient

from dbs.db_operations import pg_db_url
from vet_tg_channel import vetChannel, api_id, api_hash

with open('channels.txt') as file:
    lines = file.readlines()

telegram_channels = [line.strip() for line in lines]
print(telegram_channels)


async def vetChannelLimited(semaphore, channel, tg_client, pool=None):
    try:
        async with semaphore:
            return await vetChannel(channel=channel, tg_client=tg_client, pool=pool)
    except Exception as e:
        print(f"Exception in vetChannelLimited {e} while vetting channel  {channel}")


async def main_func():
    semaphore = asyncio.Semaphore(1)  # Limits the number of concurrent tasks to
    client = TelegramClient('anon', api_id, api_hash)
    pool = await asyncpg.create_pool(dsn=pg_db_url)

    tasks = [vetChannelLimited(semaphore, channel, client, pool=pool) for channel in telegram_channels]
    results = await asyncio.gather(*tasks)

    await client.disconnect()

    return results

if __name__ == "__main__":
    asyncio.run(main_func())
