import asyncpg, asyncio
from solana.rpc.async_api import AsyncClient
from central_db.snapshot_utils import take_snapshot
from dbs.db_operations import pg_db_url, insert_snapshot_into_db
from metadataAndSecurityModule.metadataUtils import rpc_url


async def take_all_snapshots(db_url=pg_db_url, pool=None, session=None, client=None):
    client = client or AsyncClient(rpc_url)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    sem = asyncio.Semaphore(15)

    async def snapshot_with_semaphore(mint):
        async with sem:
            data = await take_snapshot(mint, pool=pool, session=session, client=client)
            await insert_snapshot_into_db(pool=pool, data=data)

    while True:
        query = 'SELECT token_mint FROM snapshot_queue;'
        rows = await conn.fetch(query)

        tokens_to_snap = [row['token_mint'] for row in rows]

        # Create a list of tasks
        tasks = [snapshot_with_semaphore(mint) for mint in tokens_to_snap]

        # Run the tasks concurrently with a limit of 15 at a time
        await asyncio.gather(*tasks)

        await asyncio.sleep(45)


async def main():
    pool = await asyncpg.create_pool(min_size=20, max_size=30, dsn=pg_db_url)
    await take_all_snapshots(pool=pool)


asyncio.run(main())
