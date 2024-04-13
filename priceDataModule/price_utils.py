# FILL IN THE DB
# Takes token mint, start timestamp and end timestamp
# Get Price Data For that Single Token between the time
# 1-Minute Timeframe
# It should check if that range is covered and if it is not
# Fetch prices for that range and add to db
from pprint import pprint

import aiohttp, asyncio, backoff, time, asyncpg, random

pg_db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight'


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def token_exists(token_mint, db_url=pg_db_url, conn=None):
    # Connect to the database using the provided URL
    using_solo_conn = False

    if conn is None:
        conn = await asyncpg.connect(dsn=db_url)
        using_solo_conn = True

    try:
        # Prepare and execute the query to check for the token's existence
        query = "SELECT EXISTS(SELECT 1 FROM token_prices WHERE token_mint = $1 LIMIT 1)"
        exists = await conn.fetchval(query, token_mint)
        return bool(exists)
    finally:
        if using_solo_conn:
            await conn.close()


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def update_price_data(token_mint, start_timestamp, end_timestamp, db_url=pg_db_url, conn=None):
    # Fetch and insert price data into the database

    url = (f"https://public-api.birdeye.so/defi/history_price?address={token_mint}&address_type"
           f"=token&type=1m&time_from={start_timestamp}&time_to={end_timestamp}")
    headers = {
        "x-chain": "solana",
        "X-API-KEY": "813c7191c5f24a519e2325ad1649823d"
    }
    max_retries = 3  # Number of retries
    retries = 0
    price_data = None

    while retries < max_retries:
        try:
            await asyncio.sleep(random.randint(2, 10)) # trying to avoid 429's
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        price_data = await response.json()
                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")
        except Exception as e:
            retries += 1
            print(
                f"Error: {e}, Price History | Mint: {token_mint} |"
                f" Start Time: {start_timestamp} | End Time: {end_timestamp}")
            await asyncio.sleep(1)
        else:
            # If successful, exit the loop
            break

    if price_data is None:
        return
    
    try:
        items = price_data.get("data", {}).get("items", [])

        records_to_insert = [(token_mint, item['value'], item['unixTime']) for item in items]

        using_conn = False

        if conn is None:
            conn = await asyncpg.connect(dsn=db_url)
            using_conn = True

        try:
            async with conn.transaction():
                await conn.execute('''
                    CREATE TEMP TABLE tmp_token_prices AS
                    SELECT * FROM token_prices WITH NO DATA;
                ''')

                await conn.copy_records_to_table(
                    'tmp_token_prices',
                    columns=['token_mint', 'price', 'timestamp'],
                    records=records_to_insert
                )

                await conn.execute('''
                    INSERT INTO token_prices (token_mint, price, timestamp)
                    SELECT token_mint, price, timestamp FROM tmp_token_prices
                    ON CONFLICT (token_mint, timestamp) DO NOTHING;
                ''')

                await conn.execute('DROP TABLE tmp_token_prices;')
        finally:
            if using_conn:
                await conn.close()
    except Exception as e:
        print(f"Unexpected error from update_price_data: {e}")
        # Optionally, re-raise the exception if you want the calling function to handle it
        raise e


async def token_prices_to_db(token_mint, start_timestamp, end_timestamp, pool=None, db_url=pg_db_url):
    # Define extreme values for the timestamps
    MIN_TIMESTAMP = int(time.time()) - (30 *24 * 60 * 60) - (60*60) # 29 days, 23 hours ago
    MAX_TIMESTAMP = int(time.time())  # right now

    async def wrapper(conn):
        # Check if the token exists and fetch min/max timestamps in a single query
            result = await conn.fetchrow(
                "SELECT MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp "
                "FROM token_prices WHERE token_mint = $1",
                token_mint
            )
            min_timestamp, max_timestamp = result if result else (None, None)

            if min_timestamp is None:  # Implies token does not exist
                await update_price_data(token_mint, start_timestamp, end_timestamp, conn=conn)
            else:
                # Initialize min/max_timestamp if they are None
                min_timestamp = min_timestamp or MAX_TIMESTAMP
                max_timestamp = max_timestamp or MIN_TIMESTAMP

                # Adjust the range for fetching data to avoid duplication
                if start_timestamp < min_timestamp:
                    await update_price_data(token_mint, start_timestamp, min_timestamp, conn=conn)
                if end_timestamp > (max_timestamp + (3 * 60 * 60)):  # Adding a buffer of 3 hours
                    await update_price_data(token_mint, max_timestamp, end_timestamp, conn=conn)

    if pool is None:
        try:
            conn = await asyncpg.connect(dsn=db_url)
            await wrapper(conn)
        except Exception as e:
            print(f"Error From token_prices_to_db: {e}")
        finally:
            conn.close()
    else:
        try:
            async with pool.acquire() as conn:
                await wrapper(conn)        
        except Exception as e:
            print(f"Error From token_prices_to_db: {e}")
# CHECK ATH FROM CALL
# Takes a timestamp, token mint, price
# If token in db
# Fetch prices from most recent time in db to right now
# Else fetch for the last 30 days
# Returns max price after start timestamp

async def max_timestamp(token_mint, db_url=pg_db_url):
    # Connect to the PostgreSQL database using asyncpg
    conn = await asyncpg.connect(dsn=db_url)
    try:
        # Execute the query to find the maximum timestamp for the given token_mint
        query = "SELECT MAX(timestamp) FROM token_prices WHERE token_mint = $1"
        result = await conn.fetchval(query, token_mint)

        # Check if result is not None
        if result is not None:
            return result
        else:
            # Return default value if no data is found
            return 0
    finally:
        # Ensure the connection is closed after operation
        await conn.close()


async def max_price_after(token_mint, timestamp, db_url=pg_db_url):
    # Get the maximum price of a given token_mint that appears after a specified timestamp.

    # Connect to the PostgreSQL database using asyncpg
    conn = await asyncpg.connect(dsn=db_url)
    try:
        # Prepare the SQL query to select the maximum price
        query = "SELECT MAX(price) FROM token_prices WHERE token_mint = $1 AND timestamp >= $2"
        # Execute the query with parameters
        max_price = await conn.fetchval(query, token_mint, timestamp)
        return max_price
    finally:
        # Ensure the connection is closed after operation
        await conn.close()


# CHECK W or L TRADE
# Takes a token mint and a timestamp
# if the token is in the db
# check if its most recent time is >= 1 days from timestamp or today
# note the price at the time closest to the timestamp
# check if there is a price within 3.5 days after the timestamp that is 3x or more the initial price
# if there is, return True
# else return false

@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def is_win_trade(token_mint, timestamp, pool=None, db_url=pg_db_url):
    # TODO figure out if this is necessary (prices were updated earlier during fetching)
    # Alternatively, use refactor token_prices_to_db to add stricter checks before trying to do anything (more I/O)
    # We could also load price data for each token we need beforehand and use whatever is in_memory instead

    three_point_five_days_in_seconds = 3.5 * 24 * 60 * 60
    # Find the closest price at or after the given timestamp
    query = """
    WITH initial_price AS (
        SELECT price, timestamp FROM token_prices
        WHERE token_mint = $1 AND timestamp >= $2
        ORDER BY timestamp ASC
        LIMIT 1
    )
    SELECT EXISTS(
        SELECT 1 FROM token_prices, initial_price
        WHERE token_prices.token_mint = $1 
        AND token_prices.timestamp BETWEEN initial_price.timestamp AND initial_price.timestamp + $3 
        AND token_prices.price >= initial_price.price * 2.5
    )
    """
    if pool:
        async with pool.acquire() as conn:
            try:
                result = await conn.fetchval(query, token_mint, timestamp, three_point_five_days_in_seconds)
                return bool(result)
            except Exception as e:
                print(f'Error occurred in is_win_trade: {e}')
    else:
        conn = await asyncpg.connect(db_url)
        try:
            result = await conn.fetchval(query, token_mint, timestamp, three_point_five_days_in_seconds)
            return bool(result)
        except Exception as e:
            print(f'Error occurred in is_win_trade: {e}')
        finally:
            conn.close()