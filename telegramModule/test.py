import asyncio
import random

import asyncpg


from dbs.db_operations import pg_db_url
from listeners.telegram_pools.tg_client_pooling import TelegramClientPool
from vet_tg_channel import vetChannel, api_hash, api_id

with open('channels.txt') as file:
    lines = file.readlines()

telegram_channels = [line.strip() for line in lines]
print(telegram_channels)


async def vetChannelLimited(semaphore, channel, tg_pool, pool=None):
    try:
        async with semaphore:
            return await vetChannel(channel=channel, tg_pool=tg_pool, pool=pool)
    except Exception as e:
        print(f"Exception in vetChannelLimited {e} while vetting channel  {channel}")

d = +2348162921144


async def main_func():
    semaphore = asyncio.Semaphore(10)  # Limits the number of concurrent tasks to
    tg_pool = TelegramClientPool(api_hash='841396171d9b111fa191dcdce768d223', api_id=21348081)
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=50, max_size=450, command_timeout=360)

    tasks = [vetChannelLimited(semaphore, channel, tg_pool=tg_pool, pool=pool) for channel in telegram_channels]
    results = await asyncio.gather(*tasks)

    return results

if __name__ == "__main__":
    asyncio.run(main_func())
