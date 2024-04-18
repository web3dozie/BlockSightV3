import asyncpg, backoff

pg_db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight'


async def predict_new_record(model, snapshots):
    # TODO TAKE A BATCH OF SNAPSHOTS AND RETURN A PREDICTED ATH_AFTER VALUE
    pass


async def wallet_exists(wallet_address, db_url=pg_db_url):
    """
    Check if a wallet_address exists in the wallets table asynchronously using PostgreSQL.

    :param db_url: URL to the PostgreSQL database
    :param wallet_address: The wallet address to check
    :return: True if the wallet_address exists, False otherwise
    """
    try:
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


async def user_exists(username: str, db_url=pg_db_url):
    """
    Check if a username exists in the users table asynchronously using PostgreSQL.

    :param db_url: URL to the PostgreSQL database
    :param username: The username to check
    :return: True if the username exists, False otherwise
    """
    try:
        # Connect to the PostgreSQL database asynchronously
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
async def mint_exists(token_mint, db_url=pg_db_url, pool=None):
    if pool:
        try:
            async with pool.acquire() as conn:  # Use a connection from the pool
                query = "SELECT EXISTS(SELECT 1 FROM metadata WHERE token_mint = $1 LIMIT 1);"
                exists = await conn.fetchval(query, token_mint)
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
        try:
            async with pool.acquire() as conn:  # Use a connection from the pool

                # Start a transaction
                async with conn.transaction():
                    # Insert into metadata table
                    await conn.execute('''
                            INSERT INTO metadata (token_mint, symbol, name, img_url, starting_mc, starting_liq, twitter, telegram,
                             other_links, lp_creation_time, deployer, bundled, airdropped, supply, decimals)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                            ON CONFLICT (token_mint) DO NOTHING
                        ''',
                                       data['token_mint'], data['symbol'], data['name'], data['img_url'],
                                       data['starting_mc'],
                                       data['starting_liq'],
                                       data['twitter'], data['telegram'], data['other_links'], data['lp_creation_time'],
                                       data['deployer'], data['bundled'], data['airdropped'], data['supply'],
                                       data['decimals']
                                       )
        except Exception as e:
            print(f'An error occurred while adding metadata to db: {e}')

    else:
        # Connect to the PostgreSQL database asynchronously
        conn = await asyncpg.connect(dsn=db_url)
        try:
            # Start a transaction
            async with conn.transaction():
                # Insert into metadata table
                await conn.execute('''
                    INSERT INTO metadata (token_mint, symbol, name, img_url, starting_mc, starting_liq, twitter, telegram,
                     other_links, lp_creation_time, deployer, bundled, airdropped, supply, decimals)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (token_mint) DO NOTHING
                ''',
                                   data['token_mint'], data['symbol'], data['name'], data['img_url'],
                                   data['starting_mc'],
                                   data['starting_liq'],
                                   data['twitter'], data['telegram'], data['other_links'], data['lp_creation_time'],
                                   data['deployer'], data['bundled'], data['airdropped'], data['supply'],
                                   data['decimals']
                                   )

        except Exception as e:
            print(f'An error occurred while adding metadata to db: {e}')
            raise e
        finally:
            await conn.close()  # Ensure the connection is closed


# DONE(update with new schema)
@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def get_metadata_from_db(token_mint, db_url=pg_db_url):
    # Connect to the PostgreSQL database asynchronously
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
                'symbol': ['symbol'],
                'name': ['name'],
                'img_url': ['img_url'],
                'starting_mc': ['starting_mc'],
                'starting_liq': ['starting_liq'],
                'twitter': ['twitter'],
                'telegram': ['telegram'],
                'other_links': ['other_links'],
                'lp_creation_time': ['lp_creation_time'],
                'deployer': ['deployer'],
                'bundled': ['bundled'],
                'airdropped': ['airdropped'],
                'supply': ['supply'],
                'decimals': ['decimals']
            }
            return data
        else:
            return None
    finally:
        await conn.close()  # Ensure the connection is closed


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=12)
async def update_txs_db(txs_data, db_url=pg_db_url):
    insert_sql = '''
        INSERT INTO txs (txid, wallet, in_mint, in_amt, out_mint, out_amt, timestamp) 
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (txid) DO NOTHING
        '''
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


async def useful_wallets(db_path=pg_db_url):
    # TODO -> determines if a wallet is useful based on its stats
    # TODO -> Returns a list of useful wallets.

    # TODO will be used by the TX pipeline
    pass


async def get_symbol_with_mint(mint):
    data = await get_metadata_from_db(mint)
    return data['symbol']


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def update_ath_after(max_time, path_to_db=pg_db_url):
    # TODO works with central DB
    # TODO Updates the ATH_AFTER COLUMNs based on the timestamps and price
    pass


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=100)
async def insert_snapshot_into_db(data, db_path=pg_db_url):
    '''
    """
    # TODO
    very heavy concurrency needs
    add a snapshot to the db, based on the staging table, removes a mint from the staging table if it sees a flag
    """

    return await predict_new_record(data, data)  # also uses the ML model to make a prediction and trigger responses
    # if the trade is good
    '''

    pass
