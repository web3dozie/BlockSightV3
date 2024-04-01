# FILL IN THE DB
# Takes token mint, start timestamp and end timestamp
# Get Price Data For that Single Token between the time
# 5-Minute Timeframe
# It should check if that range is covered and if it is not
# Fetch prices for that range and add to db


import aiohttp, asyncio, backoff, time, asyncpg


pg_db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight'


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def token_exists(token_mint, db_url=pg_db_url):
    # Connect to the database using the provided URL
    conn = await asyncpg.connect(dsn=db_url)
    try:
        # Prepare and execute the query to check for the token's existence
        query = "SELECT EXISTS(SELECT 1 FROM token_prices WHERE token_mint = $1 LIMIT 1)"
        exists = await conn.fetchval(query, token_mint)
        return bool(exists)
    finally:
        await conn.close()


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def update_price_data(token_mint, start_timestamp, end_timestamp, db_url=pg_db_url):
    # Fetch and insert price data into the database

    url = (f"https://public-api.birdeye.so/defi/history_price?address={token_mint}&address_type"
           f"=token&type=1m&time_from={start_timestamp}&time_to={end_timestamp}")
    headers = {
        "x-chain": "solana",
        "X-API-KEY": "your_api_key_here"  # Use your actual API key
    }
    max_retries = 3  # Number of retries
    retries = 0
    price_data = None

    while retries < max_retries:
        try:
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

    try:
        items = price_data.get("data", {}).get("items", [])
        conn = await asyncpg.connect(dsn=db_url)
        try:
            async with conn.transaction():
                for item in items:
                    await conn.execute(
                        'INSERT INTO token_prices (token_mint, price, timestamp) '
                        'VALUES ($1, $2, $3) ON CONFLICT (token_mint, timestamp) DO NOTHING',
                        token_mint, item['value'], item['unixTime']
                    )
        finally:
            await conn.close()
    except Exception as e:
        print(f"Unexpected error: {e}")
        # Optionally, re-raise the exception if you want the calling function to handle it
        raise


async def token_prices_to_db(token_mint, start_timestamp, end_timestamp, db_url=pg_db_url):
    # Define extreme values for the timestamps
    MIN_TIMESTAMP = 0  # e.g., Unix epoch start
    MAX_TIMESTAMP = 1e12  # e.g., a timestamp far in the future

    conn = await asyncpg.connect(dsn=db_url)
    try:
        # Check if token_mint exists
        if not await token_exists(token_mint, db_url):
            await update_price_data(token_mint, start_timestamp, end_timestamp, db_url)
        else:
            # Fetch current min and max timestamps for token_mint
            result = await conn.fetch("SELECT MIN(timestamp), MAX(timestamp) FROM token_prices WHERE token_mint = $1",
                                      token_mint)
            min_timestamp, maximum_timestamp = result[0] if result else (MAX_TIMESTAMP, MIN_TIMESTAMP)

            # Adjust the range for fetching data to avoid duplication
            if min_timestamp is None or start_timestamp < min_timestamp:
                await update_price_data(token_mint, start_timestamp, min_timestamp or start_timestamp, db_url)
            if maximum_timestamp is None or end_timestamp > (maximum_timestamp + (60 * 60)):
                await update_price_data(token_mint, (maximum_timestamp or start_timestamp) + (60 * 60), end_timestamp,
                                        db_url)
    finally:
        await conn.close()


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
async def is_win_trade(token_mint, timestamp, db_url=pg_db_url):
    # Assuming token_prices_to_db has been updated to use asyncpg
    await token_prices_to_db(token_mint, timestamp, int(time.time()), db_url)

    three_point_five_days_in_seconds = 3.5 * 24 * 60 * 60

    # Connect to the PostgreSQL database using asyncpg
    conn = await asyncpg.connect(dsn=db_url)
    try:
        # Find the closest price at or after the given timestamp
        query = """
            SELECT price FROM token_prices
            WHERE token_mint = $1 AND timestamp >= $2
            ORDER BY timestamp ASC
            LIMIT 1
        """
        initial_price_data = await conn.fetchval(query, token_mint, timestamp)

        # If no price data found for the timestamp, return False
        if not initial_price_data:
            return False

        initial_price = initial_price_data

        # Check for a price that is 2.5x or more within 3.5 days after the timestamp
        query = """
            SELECT EXISTS(
                SELECT 1 FROM token_prices
                WHERE token_mint = $1 AND timestamp BETWEEN $2 AND $3 AND price >= $4 * 2.5
            )
        """
        result = await conn.fetchval(query, token_mint, timestamp, timestamp + three_point_five_days_in_seconds,
                                     initial_price)

        return bool(result)
    finally:
        # Ensure the connection is closed after operation
        await conn.close()
