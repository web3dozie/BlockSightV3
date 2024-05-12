import asyncio, asyncpg
import time

from pprint import pprint
from dbs.db_operations import useful_wallets, pg_db_url


async def main():
    start = time.time()
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=50, max_size=150, max_inactive_connection_lifetime=1000)

    print(f'This pool took {time.time() - start:.2f} secs to create')

    start = time.time()
    wallets = await useful_wallets(pool=pool)
    end = time.time()

    pprint(f'This took {end-start:.2f} secs to run')

    pprint(len(wallets))


asyncio.run(main())
