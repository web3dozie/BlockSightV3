import asyncio
import asyncpg
import cProfile
import pstats
import io
from metadata_tasks import fetch_mints, get_metadata_with_semaphore, pg_db_url

async def main():
    sem = asyncio.Semaphore(20)
    mints = await fetch_mints(pg_db_url)
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    tasks = [asyncio.create_task(get_metadata_with_semaphore(sem, mint['mint'], pool)) for mint in mints]

    results = await asyncio.gather(*tasks)

def profile_main():
    pr = cProfile.Profile()
    pr.enable()
    asyncio.run(main())
    pr.disable()
    s = io.StringIO()
    sortby = pstats.SortKey.CUMULATIVE
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())

if __name__ == "__main__":
    profile_main()
