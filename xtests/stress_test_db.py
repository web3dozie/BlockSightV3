import asyncio
import asyncpg
import time


async def run_query(pool):
    # Acquire a connection from the pool and run a transaction
    async with pool.acquire() as connection:
        async with connection.transaction():
            # Simple query to test the connection
            await connection.execute('SELECT 1')


async def test_db_concurrency(connections_count, db_url):
    # Start the timer to measure query execution time
    start_time = time.time()

    # Ensure min_size does not exceed max_size
    min_size = 1
    max_size = connections_count

    # Create a pool of connections with appropriate settings
    pool = await asyncpg.create_pool(dsn=db_url, min_size=min_size, max_size=max_size)

    # Create and gather tasks to run queries concurrently
    tasks = [run_query(pool) for _ in range(connections_count)]
    await asyncio.gather(*tasks)

    # Close the pool after tasks are completed
    await pool.close()

    # Measure and print the duration taken for the concurrent operations
    duration = time.time() - start_time
    print(f"Tested with {connections_count} connections: Duration {duration:.2f} seconds")


async def main():
    db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight'
    # Incrementally test the database with increasing numbers of connections
    for i in range(50, 3000, 50):  # Adjust this range based on your needs
        print(f"\n\nTesting {i} connections...")
        await test_db_concurrency(i, db_url)

# Run the main function
if __name__ == '__main__':
    asyncio.run(main())
