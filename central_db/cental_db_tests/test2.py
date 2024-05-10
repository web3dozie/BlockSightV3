import asyncio, asyncpg
import time

from pprint import pprint
from dbs.db_operations import useful_wallets, pg_db_url
from metadataAndSecurityModule.metadataUtils import get_metadata, get_data_from_helius


async def main():
    start = time.time()
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=50, max_size=250, max_inactive_connection_lifetime=200)
    print(f'This pool took {time.time() - start:.2f} secs to create')

    wallets = await useful_wallets(pool)

    values = ','.join(f"'{wallet}'" for wallet in wallets)
    query = f"SELECT out_mint FROM TXS WHERE wallet IN ({values})"

    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
        data = set([row['out_mint'] for row in rows])

    sem = asyncio.Semaphore(5)

    async def run_with_sem(mint):
        async with sem:
            await get_metadata(mint, pool=pool, regular_use=False)

    async def run_all():
        tasks = [run_with_sem(mint) for mint in data]
        await asyncio.gather(*tasks)

    await run_all()


asyncio.run(main())
