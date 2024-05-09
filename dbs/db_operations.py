from pprint import pprint

import asyncpg, backoff

from walletVettingModule.wallet_vetting_utils import determine_wallet_grade

pg_db_url = 'postgresql://bmaster:BlockSight%23Master@109.205.180.184/blocksight'


async def predict_new_record(model, snapshots):
    # TODO TAKE A BATCH OF SNAPSHOTS AND RETURN A PREDICTED ATH_AFTER VALUE
    pass


async def wallet_exists(wallet_address, db_url=pg_db_url, pool=None):
    """
    Check if a wallet_address exists in the wallets table asynchronously using PostgreSQL.

    :param pool:
    :param db_url: URL to the PostgreSQL database
    :param wallet_address: The wallet address to check
    :return: True if the wallet_address exists, False otherwise
    """
    try:
        if pool:
            async with pool.acquire() as conn:
                try:
                    # Prepare and execute the SQL query asynchronously
                    query = "SELECT EXISTS(SELECT 1 FROM wallets WHERE wallet = $1)"
                    exists = await conn.fetchval(query, wallet_address)

                    return bool(exists)

                finally:
                    # Ensure the database connection is closed
                    await conn.close()
        else:
            # Connect to the PostgreSQL database asynchronously
            conn = await asyncpg.connect(dsn=db_url)
            try:
                # Prepare and execute the SQL query asynchronously
                query = "SELECT EXISTS(SELECT 1 FROM wallets WHERE wallet = $1)"
                exists = await conn.fetchval(query, wallet_address)

                return bool(exists)

            finally:
                # Ensure the database connection is closed
                await conn.close()

    except asyncpg.PostgresError as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


async def channel_exists(channel, db_url=pg_db_url):
    """
    Check if a tg channel exists in the wallets table asynchronously using PostgreSQL.

    :param db_url: URL to the PostgreSQL database
    :param channel: The channel to check
    :return: True if the wallet_address exists, False otherwise
    """
    try:
        # Connect to the PostgreSQL database asynchronously
        conn = await asyncpg.connect(dsn=db_url)
        try:
            # Prepare and execute the SQL query asynchronously
            query = "SELECT EXISTS(SELECT 1 FROM channel_stats WHERE channel_name = $1)"
            exists = await conn.fetchval(query, channel)

            return bool(exists)

        finally:
            # Ensure the database connection is closed
            await conn.close()

    except asyncpg.PostgresError as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


async def user_exists(username: str, db_url=pg_db_url, pool=None):
    """
    Check if a username exists in the users table asynchronously using PostgreSQL.

    :param pool:
    :param db_url: URL to the PostgreSQL database
    :param username: The username to check
    :return: True if the username exists, False otherwise
    """

    try:
        # Connect to the PostgreSQL database asynchronously
        if pool:
            async with pool.acquire() as conn:  # Use a connection from the pool
                try:
                    # Prepare and execute the SQL query asynchronously
                    query = "SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)"
                    exists = await conn.fetchval(query, username)

                    return bool(exists)

                finally:
                    # Ensure the database connection is closed
                    await conn.close()

        else:
            conn = await asyncpg.connect(dsn=db_url)
            try:
                # Prepare and execute the SQL query asynchronously
                query = "SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)"
                exists = await conn.fetchval(query, username)

                return bool(exists)

            finally:
                # Ensure the database connection is closed
                await conn.close()

    except asyncpg.PostgresError as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def mint_exists(token_mint, db_url=pg_db_url, pool=None, table_name='metadata'):
    if pool:
        try:
            async with pool.acquire() as conn:  # Use a connection from the pool
                if table_name in ['metadata', 'security']:
                    query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE token_mint = $1 LIMIT 1);"
                    exists = await conn.fetchval(query, token_mint)
                else: return False

                return bool(exists)
        except asyncpg.PostgresError as e:
            print(f"Database error: {e}")
        except Exception as e:
            print(f"Exception in query: {e}")
    else:
        try:
            # Connect to the PostgreSQL database asynchronously
            conn = await asyncpg.connect(dsn=db_url)
            try:
                # Prepare and execute the SQL query asynchronously
                query = "SELECT EXISTS(SELECT 1 FROM metadata WHERE token_mint = $1 LIMIT 1);"
                exists = await conn.fetchval(query, token_mint)
                return bool(exists)
            finally:
                # Ensure the database connection is closed
                await conn.close()
        except asyncpg.PostgresError as e:
            print(f"Database error: {e}")
        except Exception as e:
            print(f"Exception in query: {e}")


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=5)
async def add_metadata_to_db(data, db_url=pg_db_url, pool=None):
    if pool:
        conn = await pool.acquire()
    else:
        conn = await asyncpg.connect(dsn=db_url)
    try:
        # Insert into metadata table
        await conn.execute('''
                INSERT INTO metadata (token_mint, symbol, name, img_url, starting_mc, starting_liq, twitter,
                 telegram, other_links, lp_creation_time, deployer, bundled, airdropped, supply, decimals,
                 lp_address, initial_lp_supply)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                ON CONFLICT (token_mint) DO NOTHING
            ''',
                           data['token_mint'], data['symbol'], data['name'], data['img_url'],
                           data['starting_mc'], data['starting_liq'], data['twitter'], data['telegram'],
                           data['other_links'], data['lp_creation_time'], data['deployer'], data['bundled'],
                           data['airdropped'], data['supply'], data['decimals'], data['lp_address'],
                           data['initial_lp_supply']
                           )
    except Exception as e:
        print(f'An error occurred while adding metadata to db: {e}')

    finally:
        await conn.close()  # Ensure the connection is closed


# DONE(update with new schema)
@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def get_metadata_from_db(token_mint, db_url=pg_db_url, pool=None):
    if pool:
        conn = await pool.acquire()
    else:
        conn = await asyncpg.connect(dsn=db_url)

    try:
        # Join the metadata and security tables to retrieve all necessary data
        row = await conn.fetchrow('''
            SELECT * FROM metadata
            WHERE token_mint = $1
        ''', token_mint)

        if row:
            # Construct the dictionary from the row, asyncpg returns a Record which can be accessed similarly to a dict
            data = {
                'token_mint': row['token_mint'],
                'symbol': row['symbol'],
                'name': row['name'],
                'img_url': row['img_url'],
                'starting_mc': row['starting_mc'],
                'starting_liq': row['starting_liq'],
                'twitter': row['twitter'],
                'telegram': row['telegram'],
                'other_links': row['other_links'],
                'lp_creation_time': row['lp_creation_time'],
                'deployer': row['deployer'],
                'bundled': row['bundled'],
                'airdropped': row['airdropped'],
                'supply': row['supply'],
                'decimals': row['decimals'],
                'lp_address': row['lp_address'],
                'initial_lp_supply': row['initial_lp_supply']
            }
            return data
        else:
            return None
    finally:
        await conn.close()  # Ensure the connection is closed


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=12)
async def update_txs_db(txs_data, db_url=pg_db_url, pool=None):
    insert_sql = '''
        INSERT INTO txs (txid, wallet, in_mint, in_amt, out_mint, out_amt, timestamp) 
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (txid) DO NOTHING
        '''
    if pool:
        async with pool.acquire() as conn:
            try:
                async with conn.transaction():
                    txs_tuples = [
                        (tx['tx_id'], tx['wallet'], tx['in_mint'], tx['in_amt'], tx['out_mint'], tx['out_amt'],
                         tx['timestamp']) for tx in txs_data]
                    await conn.executemany(insert_sql, txs_tuples)
            except Exception as e:  # Catch all exceptions, could be more specific if needed
                print(f"An error occurred. \nWhere: update_txs_db(): \n{e}")
                # asyncpg automatically rolls back the transaction in case of errors within the context manager
                raise  # Reraise the exception to trigger the backoff
            finally:
                await conn.close()  # Ensure the connection is always closed

    else:
        conn = await asyncpg.connect(dsn=db_url)
        try:
            async with conn.transaction():
                txs_tuples = [(tx['tx_id'], tx['wallet'], tx['in_mint'], tx['in_amt'], tx['out_mint'], tx['out_amt'],
                               tx['timestamp']) for tx in txs_data]
                await conn.executemany(insert_sql, txs_tuples)
        except Exception as e:  # Catch all exceptions, could be more specific if needed
            print(f"An error occurred. \nWhere: update_txs_db(): \n{e}")
            # asyncpg automatically rolls back the transaction in case of errors within the context manager
            raise  # Reraise the exception to trigger the backoff
        finally:
            await conn.close()  # Ensure the connection is always closed


async def useful_wallets(pool=None):
    smart_wallets = []

    async with pool.acquire() as conn:

        query = "SELECT wallet, trades, win_rate, avg_size, pnl FROM wallets WHERE trades >= 5"
        rows = await conn.fetch(query)

        for wallet in rows:
            grades = determine_wallet_grade(
                wallet['trades'], wallet['win_rate'], wallet['avg_size'], wallet['pnl']
            )
            if grades.get('overall_grade') in ['SS', 'S', 'A+']:
                smart_wallets.append(wallet['wallet'])

    return smart_wallets





async def get_symbol_with_mint(mint, pool=None):
    data = await get_metadata_from_db(mint, pool=pool)
    return data['symbol']


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=12)
async def update_ath_after(max_time, db_url=pg_db_url, pool=None):
    # TODO works with central DB
    # TODO Updates the ATH_AFTER COLUMNs based on the timestamps and price
    pass


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=12)
async def insert_snapshot_into_db(data, db_url=pg_db_url, pool=None):
    """
    TODO
    very heavy concurrency needs
    add a snapshot to the db, based on the staging table, removes a mint from the staging table if it sees a flag
    """

    pass


async def get_tx_list(wallet, pool=None, conn=None):
    if not conn:
        conn = await pool.acquire()

    query = "SELECT * FROM txs WHERE wallet = $1 ORDER BY timestamp DESC LIMIT 50"

    rows = await conn.fetch(query, wallet)

    tx_list = [{column: value for column, value in zip(row.keys(), row.values())} for row in rows]


    return tx_list

