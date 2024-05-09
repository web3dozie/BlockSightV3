import asyncio, asyncpg
import time

from pprint import pprint
from dbs.db_operations import useful_wallets, pg_db_url
from metadataAndSecurityModule.metadataUtils import get_metadata, get_data_from_helius


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=50, max_size=500, max_inactive_connection_lifetime=1000)

    wallets = await useful_wallets(pool)

    values = ','.join(f"'{wallet}'" for wallet in wallets)
    query = f"SELECT out_mint FROM TXS WHERE wallet IN ({values})"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
        data = set([row['out_mint'] for row in rows])

    async with asyncio.Semaphore(2):
        tasks = [asyncio.create_task(get_metadata(mint, pool=pool)) for mint in data]
        await asyncio.gather(*tasks)


asyncio.run(main())
