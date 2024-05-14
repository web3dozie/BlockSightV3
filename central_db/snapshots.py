import asyncio
import time
from pprint import pprint

import asyncpg

from central_db.snapshot_utils import take_snapshot
from dbs.db_operations import pg_db_url


async def snapshot_wrapper(mint, pool, session=None):
    start = time.time()
    await take_snapshot(mint, pool=pool, session=session)
    print(f'This snapshot took {time.time()-start:.2f} seconds to take\n\n')
    # await asyncio.sleep(55)


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=300, max_size=500, command_timeout=360)

    while True:
        x = await snapshot_wrapper('EZUFNJMZTBpungQX2czEb9ZyCMjtdzsDGMK4UywDUa1F', pool=pool)
        pprint(x)

asyncio.run(main())



