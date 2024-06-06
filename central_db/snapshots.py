import asyncio
import time
from pprint import pprint

import asyncpg
from solana.rpc.async_api import AsyncClient

from central_db.snapshot_utils import take_snapshot
from dbs.db_operations import pg_db_url, insert_snapshot_into_db
from metadataAndSecurityModule.metadataUtils import rpc_url


async def snapshot_wrapper(mint, pool, session=None, client=None):
    start = time.time()
    client = client or AsyncClient(rpc_url)
    x = await take_snapshot(mint, pool=pool, session=session, client=client)
    await insert_snapshot_into_db(pool=pool, data=x)
    print(f'This snapshot took {time.time()-start:.2f} seconds to take\n\n')
    await asyncio.sleep(55)
    return x


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=30, max_size=50, command_timeout=360)
    client = AsyncClient(rpc_url)
    while True:
        x = await snapshot_wrapper('BKPp73tBDkanhLVtX3qsFV2pWT99GZeMTEA5c1xPjDG7', pool=pool, client=client)
        pprint(x)

asyncio.run(main())



