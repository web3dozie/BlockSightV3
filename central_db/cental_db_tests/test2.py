import asyncio, asyncpg, random, threading

from dbs.db_operations import useful_wallets, pg_db_url
from metadataAndSecurityModule.metadataUtils import get_metadata


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=100, max_size=500, command_timeout=360)

    wallets = await useful_wallets(pool=pool)

    values = ','.join(f"'{wallet}'" for wallet in wallets)
    query = f"SELECT out_mint FROM TXS WHERE wallet IN ({values}) AND out_mint NOT IN (SELECT token_mint FROM metadata)"

    conn = await pool.acquire()
    rows = await conn.fetch(query)
    data = list(set([row['out_mint'] for row in rows]))
    print(f'There are {len(data)} tokens to process')
    random.shuffle(data)
    await conn.close()

    sem = asyncio.Semaphore(5)

    async def run_with_sem(mint):
        async with sem:
            try:
                await get_metadata(mint, regular_use=False, pool=pool)
            except Exception as e:
                print(f"An error occurred when updating metadata for {mint}: {e}")

    async def run_all():
        tasks = [run_with_sem(mint) for mint in data]
        await asyncio.gather(*tasks)

    await run_all()

    if pool:
        await pool.close()


loop = asyncio.get_event_loop()
loop.set_debug(True)  # Enable debug mode
try:
    loop.run_until_complete(main())
finally:
    loop.close()
