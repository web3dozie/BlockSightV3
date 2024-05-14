import asyncio, asyncpg, random
import time

from dbs.db_operations import useful_wallets, pg_db_url
from metadataAndSecurityModule.metadataUtils import get_metadata

num_tokens = 30


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=100, max_size=500, command_timeout=360)

    wallets = await useful_wallets(pool=pool)
    values = ','.join(f"'{wallet}'" for wallet in wallets)
    query = f"SELECT out_mint FROM TXS WHERE wallet IN ({values}) AND out_mint NOT IN (SELECT token_mint FROM metadata)"

    conn = await pool.acquire()
    rows = await conn.fetch(query)
    data = list(set([row['out_mint'] for row in rows]))
    await conn.close()
    print(f'There are {len(data)} tokens to process')

    random.shuffle(data)
    data = data[0:num_tokens]

    sem = asyncio.Semaphore(num_tokens)

    async def run_with_sem(mint):
        async with sem:
            try:
                await get_metadata(mint, regular_use=False, pool=pool)
            except Exception as e:
                print(f"An error occurred when updating metadata for {mint}: {e}")

    async def run_all():
        tasks = [asyncio.create_task(run_with_sem(mint)) for mint in data]
        await asyncio.gather(*tasks)

    await run_all()

    if pool:
        await pool.close()


async def main_wrapper():
    for _ in range(10):
        start = time.time()
        await main()
        text = f'It took {time.time() - start:.2f} seconds to process {num_tokens} tokens at once.\n'

        with open('logs.txt', 'a') as log:
            log.write(text)

    print('ALL DONE')

asyncio.run(main_wrapper())
