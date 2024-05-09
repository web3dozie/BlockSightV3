import asyncio, asyncpg

from dbs.db_operations import pg_db_url
from listeners.smart_wallet_listener import maintain_txs


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=250, max_size=500, max_inactive_connection_lifetime=500)

    maintain_txs_task = asyncio.create_task(maintain_txs(pool))

    await asyncio.gather(maintain_txs_task)


asyncio.run(main())


