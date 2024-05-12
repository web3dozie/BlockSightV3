import asyncio, asyncpg
import time

from dbs.db_operations import pg_db_url
from listeners.smart_wallet_listener import maintain_txs


async def main():
    start = time.time()

    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=250, max_size=500,
                                     max_inactive_connection_lifetime=500, command_timeout=60)
    print(f'This took {time.time() - start:.2f} secs to create')
    print('POOL CREATED')

    maintain_txs_task = asyncio.create_task(maintain_txs(pool))

    await asyncio.gather(maintain_txs_task)


asyncio.run(main())


