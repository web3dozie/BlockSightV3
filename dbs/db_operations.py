import pandas as pd
import asyncio, time, sqlite3, aiosqlite, asyncpg, backoff, joblib

from datetime import datetime
from priceDataModule.price_utils import get_post_call_ath

wallets_db_path = 'C:\\Users\\dozie\\Desktop\\BlockSight\\BlockSight V.1.0.0\\dbs\\wallets.db'
tokens_db_path = 'C:\\Users\\dozie\\Desktop\\BlockSight\\BlockSight V.1.0.0\\dbs\\tokens.db'
calls_db_path = 'C:\\Users\\dozie\\Desktop\\BlockSight\\BlockSight V.1.0.0\\dbs\\calls.db'
txs_db_path = 'C:\\Users\\dozie\\Desktop\\BlockSight\\BlockSight V.1.0.0\\dbs\\txs.db'
new_pairs_db_path = 'postgresql://postgres:Akara007@localhost:5432/new_pairs'


async def predict_new_record(new_record):
    if new_record == {}:
        return 0

    # Load the saved model
    model_filename = '../dbs/xgb_model_test.joblib'
    loaded_model = joblib.load(model_filename)

    # Define the list of feature names used in your model training
    feature_names = [
        'lp_age', 'fdv', 'liquidity', 'num_holders', 'mint_safe', 'lp_safe',
        'socials', 'buys_5m', 'sells_5m', 'volume_5m', 'price_change_5m',
        'buys_1h', 'sells_1h', 'volume_1h', 'price_change_1h', 'top_10',
        'top_20', 'starting_liq', 'starting_fdv'
    ]

    # Filter the new_record to include only the keys that are in feature_names
    filtered_record = {key: new_record[key] for key in feature_names if key in new_record}

    # Convert the filtered record to a DataFrame
    new_record_df = pd.DataFrame([filtered_record])

    # Make prediction
    predicted_ath_after = loaded_model.predict(new_record_df)

    if predicted_ath_after >= 15:
        print(f'Called at: {datetime.now().strftime('%I:%M %p')} ||| Predicts {predicted_ath_after}X')
        print(new_record)
        print()
        print()

    return predicted_ath_after


async def wallet_exists(wallet_address, db_path=wallets_db_path):
    """
    Check if a wallet_address exists in the wallets table asynchronously.

    :param db_path: Path to the SQLite database file
    :param wallet_address: The wallet address to check
    :return: True if the wallet_address exists, False otherwise
    """
    try:
        # Connect to the SQLite database asynchronously
        async with aiosqlite.connect(db_path) as conn:
            async with conn.cursor() as cursor:
                # Prepare and execute the SQL query asynchronously
                query = "SELECT EXISTS(SELECT 1 FROM wallets WHERE wallet = ?)"
                await cursor.execute(query, (wallet_address,))

                # Fetch the result asynchronously
                exists = (await cursor.fetchone())[0]

                return bool(exists)

    except aiosqlite.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=8)
async def mint_exists(token_mint, db=tokens_db_path):
    try:
        async with aiosqlite.connect(db) as conn:
            async with conn.cursor() as cursor:
                query = "SELECT EXISTS(SELECT 1 FROM metadata WHERE token_mint = ? LIMIT 1);"
                await cursor.execute(query, (token_mint,))
                exists = (await cursor.fetchone())[0]
                return exists == 1  # Returns True if exists, False otherwise
    except aiosqlite.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Exception in query: {e}")


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=8)
async def is_over_a_week(wallet_address, db_path=wallets_db_path):
    # Define one week in seconds
    one_week_in_seconds = 7 * 24 * 60 * 60

    # Get the current time as a Unix timestamp
    current_time = int(time.time())

    async with aiosqlite.connect(db_path) as db:
        async with db.execute("SELECT last_checked FROM wallets WHERE wallet_address = ?", (wallet_address,)) as cursor:
            result = await cursor.fetchone()
            if result is None:
                return False  # Wallet address not found

            last_checked = result[0]
            # Check if the difference is more than one week
            return (current_time - last_checked) > one_week_in_seconds


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=5)
async def add_metadata_to_db(data, db_path=tokens_db_path):
    async with aiosqlite.connect(db_path, timeout=10.0) as db:
        try:
            # Start a transaction
            await db.execute('BEGIN')

            # Define the maximum SQLite INTEGER value
            MAX_SQLITE_INT = 9223372036854775807

            # Prepare data, checking integer limits
            supply_value = data['supply']
            if not isinstance(supply_value, int) or supply_value > MAX_SQLITE_INT:
                supply_value = MAX_SQLITE_INT

            # Insert into metadata table
            await db.execute('''
                INSERT OR IGNORE INTO metadata (token_mint, symbol, name, img_url, twitter, telegram, other_links,
                 lp_creation_time, deployer, supply, decimals)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data['token_mint'], data['symbol'], data['name'], data['img_url'],
                data['twitter'], data['telegram'], data['other_links'], data['lp_creation_time'],
                data['deployer'], supply_value, data['decimals']
            ))

            # Insert into security table
            await db.execute('''
                INSERT OR IGNORE INTO security (token_mint, is_mintable, is_mutable, lp_burnt_percentage,
                 lp_locked_percentage)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                data['token_mint'], data['is_mintable'], data['is_mutable'], data['lp_burnt_percentage'],
                data['lp_locked_percentage']
            ))

            # Commit the transaction
            await db.commit()

        except Exception as e:
            print(f'An error occurred: {e}')
            await db.rollback()  # Rollback in case of error
            print('Transaction rolled back')
            raise e


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=8)
async def get_metadata_from_db(token_mint, db_path=tokens_db_path):
    async with aiosqlite.connect(db_path) as db:
        # Join the metadata and security tables to retrieve all necessary data
        cursor = await db.execute('''
            SELECT m.*, s.is_mintable, s.is_mutable, s.lp_burnt_percentage, s.lp_locked_percentage
            FROM metadata m
            JOIN security s ON m.token_mint = s.token_mint
            WHERE m.token_mint = ?
        ''', (token_mint,))

        row = await cursor.fetchone()
        if row:
            # Construct the dictionary from the row
            data = {
                'token_mint': row[0],
                'symbol': row[1],
                'name': row[2],
                'img_url': row[3],
                'twitter': row[5],
                'telegram': row[6],
                'other_links': row[7],
                'lp_creation_time': row[8],
                'deployer': row[9],
                'supply': row[10],
                'decimals': row[11],
                'is_mintable': row[12],
                'is_mutable': row[13],
                'lp_burnt_percentage': row[14],
                'lp_locked_percentage': row[15]
            }
            return data
        else:
            return None


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=12)
async def update_txs_db(txs_data, db_path=txs_db_path):
    insert_sql = '''
        INSERT OR IGNORE INTO TXs (txId, wallet, in_mint, in_amt, out_mint, out_amt, timestamp) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
    try:
        async with aiosqlite.connect(db_path) as conn:
            await conn.execute('BEGIN')
            async with conn.cursor() as cursor:
                txs_tuples = [(tx['tx_id'], tx['wallet'], tx['in_mint'], tx['in_amt'], tx['out_mint'], tx['out_amt'],
                               tx['timestamp']) for tx in txs_data]
                await cursor.executemany(insert_sql, txs_tuples)
            await conn.commit()
    except Exception as e:  # Catch all exceptions, could be more specific if needed
        print(f"An error occurred. \nWhere: update_txs_db(): \n{e}")
        await conn.rollback()
        raise  # Reraise the exception to trigger the backoff


async def useful_wallets(db_path=wallets_db_path):
    # The grades we want to filter by
    target_grades = ['S', 'SS', 'A', 'A+', str(int(time.time()) - (2 * 7 * 24 * 60 * 60))]  # 2 weeks ago

    # Connect to the SQLite database
    async with aiosqlite.connect(db_path) as db:
        # Prepare the SQL query
        query = "SELECT wallet FROM wallets WHERE (overall_grade IN (?, ?, ?, ?)) AND (last_checked >= ?)"

        # Execute the query
        async with db.execute(query, target_grades) as cursor:
            # Fetch all results
            rows = await cursor.fetchall()

            # Extract wallets from the rows and return them
            return [row[0] for row in rows]


async def get_symbol_with_mint(mint):
    data = await get_metadata_from_db(mint)
    return data['symbol']


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=8)
async def log_call(token_mint='', price=0.0, call_time=0, lp_age=0, fdv=0, liquidity=0, socials=True, safe_lp=True,
                   mintable=True, channel='', db_path=calls_db_path):
    # Insert the call data
    insert_query = '''
        INSERT INTO calls(token_mint, channel, price, call_time, lp_age, fdv, liquidity, socials, safe_lp, mint_safe)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        '''
    async with aiosqlite.connect(db_path) as db:
        await db.execute(insert_query,
                         (token_mint, channel, price, call_time, lp_age, fdv, liquidity, socials, safe_lp, mintable))
        await db.commit()


@backoff.on_exception(backoff.expo, sqlite3.OperationalError, max_tries=8)
async def update_ath_after(path_to_db=calls_db_path):
    # Connect to the SQLite database
    async with aiosqlite.connect(path_to_db) as db:

        # Select records where call_time is within the last 24 hours
        async with db.execute("SELECT ROWID, mint, price, unix_timestamp FROM fresh_wallets") as cursor:
            async for row in cursor:
                row_id, token_mint, price, call_time = row
                # Call get_post_call_ath for each record
                ath_after = await get_post_call_ath(token_mint, price, call_time)
                # Update the ath_after column for the record
                await db.execute("UPDATE fresh_wallets SET ath_after = ? WHERE ROWID = ?", (ath_after, row_id))
                print(f'{token_mint} pumped {ath_after}X')

        # Commit the changes to the database
        await db.commit()


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=100)
async def insert_new_pair(data, db_path=new_pairs_db_path):

    if data == {}:
        return

    insert_query = """
    INSERT INTO new_pairs (
        mint, price, call_time, lp_age, fdv, liquidity, num_holders, 
        mint_safe, lp_safe, socials, buys_5m, sells_5m, volume_5m, price_change_5m,
        buys_1h, sells_1h, volume_1h, price_change_1h, top_10, top_20, starting_liq, starting_fdv
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, 
        $8, $9, $10, $11, $12, $13, $14,
        $15, $16, $17, $18, $19, $20, $21, $22
    );
    """
    conn = await asyncpg.connect(db_path)
    try:
        # Prepare data tuple from the dictionary values
        data_tuple = tuple(data.values())
        await conn.execute(insert_query, *data_tuple)
    finally:
        await conn.close()

    return await predict_new_record(data)


async def get_liq_fdv(mint: str) -> tuple:
    conn = await asyncpg.connect(new_pairs_db_path)
    try:
        # Use the PostgreSQL parameter format ($1, $2, etc.)
        query = 'SELECT starting_liq, starting_fdv FROM new_pairs WHERE mint = $1'
        row = await conn.fetchrow(query, mint)
        if row:
            return row['starting_liq'], row['starting_fdv']
        else:
            return None, None
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(update_ath_after(path_to_db='C:\\Users\\Dozie\\Desktop\\Code Projects\\LP_Burn_Filter\\lp_burns.db'))
