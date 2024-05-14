import asyncio, aiohttp, asyncpg
from pprint import pprint

from dbs.db_operations import pg_db_url
from metadataAndSecurityModule.metadataUtils import get_metadata, retrieve_metadata


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=100, max_size=500)
    session = None# aiohttp.ClientSession()
    x = await retrieve_metadata('5BKTP1cWao5dhr8tkKcfPW9mWkKtuheMEAU6nih2jSX', session=session)
    pprint(x)

    if session:
        await session.close()

    if pool:
        await pool.close()

asyncio.run(main())
