import asyncio, asyncpg
import time

from dbs.db_operations import pg_db_url
from listeners.smart_wallet_listener import maintain_txs


async def main():

    maintain_txs_task = asyncio.create_task(maintain_txs())

    await asyncio.gather(maintain_txs_task)


asyncio.run(main())


