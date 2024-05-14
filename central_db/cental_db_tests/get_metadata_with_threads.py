import asyncio, asyncpg, random, threading
import time

from dbs.db_operations import useful_wallets, pg_db_url
from metadataAndSecurityModule.metadataUtils import get_metadata

num_tokens = 10


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

    # sem = asyncio.Semaphore(5)

    data = data[0:num_tokens]
    print(f'There are {len(data)} tokens to process')

    async def run_with_sem(mint):
        try:
            await get_metadata(mint, regular_use=False, pool=pool)
        except Exception as e:
            print(f"An error occurred when updating metadata for {mint}: {e}")

    async def run_all():
        tasks = [threading.Thread(target=run_with_sem, args=(mint,)) for mint in data]

        for task in tasks:
            task.start()
        for task in tasks:
            task.join()

    await run_all()

    if pool:
        await pool.close()


start = time.time()
asyncio.run(main())
print(f'It took {time.time() - start:.2f} seconds to process {num_tokens} tokens.')
