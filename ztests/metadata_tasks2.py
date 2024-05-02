import asyncio
import random

import aiohttp
import asyncpg
from metadataAndSecurityModule.metadataUtils import get_metadata

pg_db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight'


async def fetch_mints(db_url):
    conn = await asyncpg.connect(db_url)
    try:
        query = '''
        SELECT DISTINCT combined.mint
        FROM (
          SELECT in_mint AS mint FROM txs
          UNION
          SELECT out_mint AS mint FROM txs
        ) AS combined
        WHERE NOT EXISTS (
          SELECT 1
          FROM metadata
          WHERE metadata.token_mint = combined.mint
        )
        ORDER BY combined.mint DESC;
        '''
        return await conn.fetch(query)
    finally:
        await conn.close()


async def get_metadata_with_semaphore(sem, mint, pool, session=None):
    async with sem:
        metadata = await get_metadata(mint, regular_use=False, pool=pool, session=session)
        return metadata


async def main():
    sem = asyncio.Semaphore(50)
    mints = await fetch_mints(pg_db_url)

    # session = None

    # CREATE POOL
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    tasks = [asyncio.create_task(get_metadata_with_semaphore(sem, mint['mint'], pool))for mint in mints]

    results = await asyncio.gather(*tasks)

    # await session.close()


asyncio.run(main())