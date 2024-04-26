import asyncio
from pprint import pprint

import asyncpg

from dbs.db_operations import pg_db_url
from metadataAndSecurityModule.metadataUtils import get_data_from_helius, retrieve_metadata, get_metadata


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    x = await get_metadata('GzyVDkS4FWo45LH4nz4hjRJpQ9iaCj1JCWxskADgXbae', pool=pool)
    pprint(x)

asyncio.run(main())
