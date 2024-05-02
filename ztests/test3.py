import asyncio
import time
from pprint import pprint

import asyncpg

from dbs.db_operations import pg_db_url, get_tx_list
from metadataAndSecurityModule.metadataUtils import get_data_from_helius, retrieve_metadata, get_metadata


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    conn = await pool.acquire()

    start = float(time.time())
    x = await get_tx_list('J9QKW6w7ALr8pwhXayXJa1njC1AtJPpNGNMsb4HFXBdS', pool)
    end = float(time.time())

    pprint(x)
    print(end - start)

asyncio.run(main())