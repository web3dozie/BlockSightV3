# FILL IN THE DB
# Takes token mint, start timestamp and end timestamp
# Get Price Data For that Single Token between the time
# 5-Minute Timeframe
# It should check if that range is covered and if it is not
# Fetch prices for that range and add to db


import sqlite3, aiosqlite, aiohttp, asyncio, backoff, time


prices_db_path = 'C:\\Users\\dozie\\Desktop\\BlockSight\\BlockSight V.1.0.0\\dbs\\token_prices.db'


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=8)
async def token_exists(token_mint, db_path=prices_db_path):
    async with aiosqlite.connect(db_path) as conn:
        async with conn.cursor() as cur:
            # Prepare and execute the query to check for the token's existence
            await cur.execute("SELECT EXISTS(SELECT 1 FROM token_prices WHERE token_mint = ? LIMIT 1)", (token_mint,))
            exists, = await cur.fetchone()
            return bool(exists)


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=8)
async def update_price_data(token_mint, start_timestamp, end_timestamp, db_path=prices_db_path):
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
        async with aiosqlite.connect(db_path) as db:
            await db.execute('BEGIN')
            for item in items:
                await db.execute(
                    'INSERT OR IGNORE INTO token_prices (token_mint, price, timestamp) VALUES (?, ?, ?)',
                    (token_mint, item['value'], item['unixTime'])
                )
            await db.commit()
    except aiosqlite.Error as e:
        print(f"SQLite error: {e}")
        # Optionally, re-raise the exception if you want the calling function to handle it
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        # Optionally, re-raise the exception if you want the calling function to handle it
        raise


async def token_prices_to_db(token_mint, start_timestamp, end_timestamp, db_path=prices_db_path):

    # Define extreme values for the timestamps
    MIN_TIMESTAMP = 0  # e.g., Unix epoch start
    MAX_TIMESTAMP = 1e12  # e.g., a timestamp far in the future

    async with aiosqlite.connect(db_path) as conn:
        async with conn.cursor() as cur:
            # Check if token_mint exists
            if not await token_exists(token_mint, db_path):
                await update_price_data(token_mint, start_timestamp, end_timestamp, db_path)
            else:
                # Fetch current min and max timestamps for token_mint
                await cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM token_prices WHERE token_mint = ?",
                                  (token_mint,))
                result = await cur.fetchone()
                min_timestamp, maximum_timestamp = result if result else (MAX_TIMESTAMP, MIN_TIMESTAMP)

                # Adjust the range for fetching data to avoid duplication
                if min_timestamp is None or start_timestamp < min_timestamp:
                    await update_price_data(token_mint, start_timestamp, min_timestamp, db_path)
                if maximum_timestamp is None or end_timestamp > (maximum_timestamp + (60 * 60)):
                    await update_price_data(token_mint, maximum_timestamp, end_timestamp, db_path)


# CHECK ATH FROM CALL
# Takes a timestamp, token mint, price
# If token in db
# Fetch prices from most recent time in db to right now
# Else fetch for the last 30 days
# Returns max price after start timestamp

async def max_timestamp(token_mint, db_path=prices_db_path):
    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT MAX(timestamp) FROM token_prices WHERE token_mint = ?", (token_mint,)) as cursor:
            result = await cursor.fetchone()
            # Check if result is not None and the first element is not None
            if result and result[0] is not None:
                return result[0]
            else:
                # Return default value if no data is found
                return 0


async def max_price_after(token_mint, timestamp, db_path=prices_db_path):
    # Get the maximum price of a given token_mint that appears after a specified timestamp.

    async with aiosqlite.connect(db_path) as conn:
        # Prepare the SQL query to select the maximum price
        query = "SELECT MAX(price) FROM token_prices WHERE token_mint = ? AND timestamp >= ?"
        async with conn.execute(query, (token_mint, timestamp)) as cursor:
            max_price_row = await cursor.fetchone()
            max_price = max_price_row[0] if max_price_row else None
            return max_price


async def get_post_call_ath(token_mint, price_at_call=0.0, call_timestamp=0):
    right_now = int(time.time())

    if await token_exists(token_mint):
        most_recent_timestamp_for_token = await max_timestamp(token_mint)
        await update_price_data(token_mint, most_recent_timestamp_for_token, right_now)
    else:
        thirty_days_ago = right_now - (30 * 24 * 60 * 60)  # 30 days in seconds
        await update_price_data(token_mint, thirty_days_ago, right_now)

    max_price = await max_price_after(token_mint, call_timestamp)

    if max_price is None:
        return 0

    return round((max_price / price_at_call), 2)


# CHECK W or L TRADE
# Takes a token mint and a timestamp
# if the token is in the db
# check if its most recent time is >= 1 days from timestamp or today
# note the price at the time closest to the timestamp
# check if there is a price within 3.5 days after the timestamp that is 3x or more the initial price
# if there is, return True
# else return false
@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=8)
async def is_win_trade(token_mint, timestamp, db_path=prices_db_path):
    await token_prices_to_db(token_mint, timestamp, int(time.time()))

    three_point_five_days_in_seconds = 3.5 * 24 * 60 * 60

    async with aiosqlite.connect(db_path) as conn:
        async with conn.cursor() as cur:
            # Find the closest price at or after the given timestamp
            await cur.execute("""
                SELECT price FROM token_prices
                WHERE token_mint = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT 1
            """, (token_mint, timestamp,))
            initial_price_data = await cur.fetchone()

            # If no price data found for the timestamp, return False
            if not initial_price_data:
                return False

            initial_price = initial_price_data[0]

            # Check for a price that is 3x or more within 3.5 days after the timestamp
            await cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM token_prices
                    WHERE token_mint = ? AND timestamp BETWEEN ? AND ? AND price >= ? * 2.5
                )
            """, (token_mint, timestamp, timestamp + three_point_five_days_in_seconds, initial_price,))
            result = await cur.fetchone()

            return bool(result[0])
