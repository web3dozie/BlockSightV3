from pprint import pprint
from decimal import Decimal

import asyncpg, backoff, time

from walletVettingModule.wallet_vetting_utils import determine_wallet_grade, determine_tg_grade

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
async def mint_exists(token_mint, pool=None, table_name='metadata'):
    try:
        new_conn = not bool(pool)
        conn = await asyncpg.connect(dsn=pg_db_url) if not pool else await pool.acquire()
        try:
            if table_name in ['metadata', 'security']:
                query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE token_mint = $1 LIMIT 1);"
                exists = await conn.fetchval(query, token_mint)
            else:
                print(table_name)
                return False
        finally:
            if new_conn:
                await conn.close()
            else:
                await pool.release(conn)

        return bool(exists)
    except asyncpg.PostgresError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Exception in mint_exists: {e}. Pool is: {type(pool)}. Token is {token_mint}")


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=5)
async def add_metadata_to_db(data, db_url=pg_db_url, pool=None):
    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

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
        pprint(f'An error occurred while adding {data} metadata to db: {e}\n')

    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=8)
async def get_metadata_from_db(token_mint, db_url=pg_db_url, pool=None):
    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

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
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=12)
async def update_txs_db(txs_data, db_url=pg_db_url, pool=None, is_useful_wallet=False):
    insert_sql = '''
        INSERT INTO txs (txid, wallet, in_mint, in_amt, out_mint, out_amt, timestamp) 
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (txid) DO NOTHING
        '''
    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    try:
        if is_useful_wallet:
            for tx in txs_data:
                if int(time.time()) - tx['timestamp'] <= 300:
                    if tx['out_mint'] not in ['So11111111111111111111111111111111111111112',
                                              'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v']:
                        await insert_into_snapshot_queue(mint=tx['out_mint'], timestamp=tx['timestamp'],
                                                         source='smart_wallet')
        async with conn.transaction():
            txs_tuples = [
                (tx['tx_id'], tx['wallet'], tx['in_mint'], tx['in_amt'], tx['out_mint'], tx['out_amt'],
                 tx['timestamp']) for tx in txs_data]
            await conn.executemany(insert_sql, txs_tuples)
    except Exception as e:  # Catch all exceptions, could be more specific if needed
        print(f"An error occurred. \nWhere: update_txs_db(): \n{e}")
        # asyncpg automatically rolls back the transaction in case of errors within the context manager
        raise e  # Reraise the exception to trigger the backoff
    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


async def useful_wallets(pool=None, db_url=pg_db_url, window=30):
    smart_wallets = []

    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    try:
        query = "SELECT wallet, trades, win_rate, avg_size, pnl FROM wallets WHERE trades >= 5"
        start = time.time()
        rows = await conn.fetch(query)
        # print(f'It took {time.time() - start:.2f} secs to all wallets.')

        for wallet in rows:
            grades = determine_wallet_grade(
                wallet['trades'], wallet['win_rate'], wallet['avg_size'], wallet['pnl']
                , window=window)
            if grades.get('overall_grade') in ['SS', 'S', 'A+']:
                smart_wallets.append(wallet['wallet'])

    except Exception as e:
        print(f'useful_wallets() failed: {e}')
        raise e
    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)

    return list(set(smart_wallets))


async def get_id_from_channel(channel_name, pool=None, db_url=pg_db_url):
    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    try:
        query = "SELECT channel_id FROM channel_stats WHERE channel_name = $1"

        channel_id = await conn.fetchval(query, channel_name)

        return channel_id

    except Exception as e:
        print(f'get_id_from_channel() failed: {e}')
        raise e
    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


async def useful_channels(pool=None, db_url=pg_db_url, window=30):
    smart_channels = []

    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    try:
        query = "SELECT channel_id, channel_name, trades_count, win_rate, last_updated, channel_name FROM channel_stats WHERE trades_count >= 5"
        start = time.time()
        rows = await conn.fetch(query)
        # print(f'It took {time.time() - start:.2f} secs to all wallets.')

        for channel in rows:
            grades = determine_tg_grade(channel['trades_count'], channel['win_rate'], window=window)

            if grades.get('overall_grade') in ['SS', 'S', 'A+', 'A', 'B+']:
                smart_channels.append({(int('-100' + str(channel['channel_id']))): channel['channel_name']})

    except Exception as e:
        print(f'useful_channels() failed: {e}')
        raise e
    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)

    return smart_channels


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=12)
async def update_ath_after(max_time, db_url=pg_db_url, pool=None):
    # TODO works with central DB
    # TODO Updates the ATH_AFTER COLUMNs based on the timestamps and price
    pass


async def insert_snapshot_into_db(data, db_url=pg_db_url, pool=None):
    def convert_decimals_to_floats(data_dict):
        for key, value in data_dict.items():
            if isinstance(value, Decimal):
                data_dict[key] = float(value)
        return data_dict

    data = convert_decimals_to_floats(data)

    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    try:
        query = """
        INSERT INTO snapshots (
            airdropped, bundled, buys_1h, buys_5m, buys_6h, call_time, fdv, liquidity,
            lp_age, lp_safe, mint_safe, num_holders, price, price_change_1h, price_change_5m,
            price_change_6h, sells_1h, sells_5m, sells_6h, smart_buys_1h, smart_buys_5m,
            smart_buys_6h, smart_netflows_1h, smart_netflows_5m, smart_netflows_6h,
            smart_sells_1h, smart_sells_5m, smart_sells_6h, smart_tg_calls_1h,
            smart_tg_calls_5m, smart_tg_calls_6h, smart_volume_1h, smart_volume_5m,
            smart_volume_6h, socials, starting_liq, starting_mc, token_mint, top_10, top_20,
            volume_1h, volume_5m, volume_6h
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
            $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34,
            $35, $36, $37, $38, $39, $40, $41, $42, $43
        )
        """
        try:
            await conn.execute(query,
                           data['airdropped'], data['bundled'], data['buys_1h'], data['buys_5m'], data['buys_6h'],
                           data['call_time'], data['fdv'], data['liquidity'], data['lp_age'], data['lp_safe'],
                           data['mint_safe'], data['num_holders'], data['price'], data['price_change_1h'],
                           data['price_change_5m'], data['price_change_6h'], data['sells_1h'], data['sells_5m'],
                           data['sells_6h'], data['smart_buys_1h'], data['smart_buys_5m'], data['smart_buys_6h'],
                           data['smart_netflows_1h'], data['smart_netflows_5m'], data['smart_netflows_6h'],
                           data['smart_sells_1h'], data['smart_sells_5m'], data['smart_sells_6h'],
                           data['smart_tg_calls_1h'], data['smart_tg_calls_5m'], data['smart_tg_calls_6h'],
                           data['smart_volume_1h'], data['smart_volume_5m'], data['smart_volume_6h'],
                           data['socials'], data['starting_liq'], data['starting_mc'], data['token_mint'],
                           data['top_10'], data['top_20'], data['volume_1h'], data['volume_5m'], data['volume_6h']
                           )
        except KeyError:
            return

    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


async def get_tx_list(wallet, pool=None):
    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=pg_db_url)

    try:
        query = "SELECT * FROM txs WHERE wallet = $1 ORDER BY timestamp DESC LIMIT 50"
        rows = await conn.fetch(query, wallet)
        tx_list = [{column: value for column, value in zip(row.keys(), row.values())} for row in rows]

        return tx_list

    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=12)
async def insert_into_snapshot_queue(mint, timestamp, source, db_url=pg_db_url, pool=None):
    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    try:
        # Insert or update the record
        upsert_query = """
        INSERT INTO snapshot_queue (token_mint, timestamp, source)
        VALUES ($1, $2, $3)
        ON CONFLICT (token_mint)
        DO UPDATE SET timestamp = EXCLUDED.timestamp, source = EXCLUDED.source
        """

        await conn.execute(upsert_query, mint, timestamp, source)

        # Delete rows with timestamps older than 2 hours
        delete_query = """
        DELETE FROM snapshot_queue
        WHERE timestamp < $1
        """
        cutoff_time = int(time.time()) - 2 * 3600  # 2 hours in seconds
        await conn.execute(delete_query, cutoff_time)

    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


@backoff.on_exception(backoff.expo, asyncpg.PostgresError, max_tries=12)
async def delete_snapshot_from_queue(token_mint, db_url=pg_db_url, pool=None):
    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    try:
        query = "DELETE FROM snapshot_queue WHERE token_mint = $1"
        await conn.execute(query, token_mint)

    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)
