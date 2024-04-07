import time, aiohttp, asyncio, base58, asyncpg

from pprint import pprint

from priceDataModule.price_utils import is_win_trade, token_prices_to_db
from metadataAndSecurityModule.metadataUtils import get_metadata

from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError

pg_db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight'
helius_api_key = '41a2ecf6-41ff-4ef8-997f-c9b905388725'


def is_valid_wallet(string):
    try:
        # Decode the string from to bytes
        point_bytes = base58.b58decode(string)

        # Attempt to create a VerifyKey object, which will validate the point
        VerifyKey(point_bytes)

        return True
    except (ValueError, BadSignatureError):
        # If there's an error in decoding or the point is not on the curve
        return False


def deduplicate_transactions(txs):
    # Sort transactions by 'out_mint' and then by 'timestamp'
    sorted_txs = sorted(txs, key=lambda x: (x['out_mint'], x['timestamp']))

    # Function to deduplicate transactions
    deduped = []
    last_seen = {}

    for tx in sorted_txs:
        out_mint = tx['out_mint']
        timestamp = tx['timestamp']

        # Check if this out_mint was seen before and within 15 minutes (900 seconds)
        if out_mint in last_seen and timestamp - last_seen[out_mint] <= 900:
            continue

        # Update the last seen time for this out_mint
        last_seen[out_mint] = timestamp
        deduped.append(tx)

    return deduped


def determine_grade(trades, win_rate, avg_size, pnl):
    # Helper function to determine points based on value and thresholds
    def get_points(value, thresholds):
        if value >= thresholds['S']:
            return 25
        elif value >= thresholds['A']:
            return 15
        elif value >= thresholds['B']:
            return 10
        elif value >= thresholds['C']:
            return 5
        else:
            return 0

    # Define thresholds for each category
    win_rate_thresholds = {'S': 25, 'A': 20, 'B': 15, 'C': 10, 'F': 10}
    trades_thresholds = {'S': 100, 'A': 75, 'B': 50, 'C': 20, 'F': 20}
    size_thresholds = {'S': 2000, 'A': 1000, 'B': 500, 'C': 200, 'F': 200}
    pnl_thresholds = {'S': 25000, 'A': 10000, 'B': 2500, 'C': 1000, 'F': 1000}

    # Calculate points for each category
    win_rate_points = get_points(win_rate, win_rate_thresholds) * 2  # Double points for win rate
    trades_points = get_points(trades, trades_thresholds)
    size_points = get_points(avg_size, size_thresholds)
    pnl_points = get_points(pnl, pnl_thresholds)

    # Calculate overall points
    overall_points = win_rate_points + trades_points + size_points + pnl_points

    # Adjust overall points for Tier F
    if win_rate < win_rate_thresholds['F']:
        overall_points -= 15

    if trades < trades_thresholds['F']:
        overall_points -= 15

    if trades < 10:
        overall_points -= 20

    if avg_size < size_thresholds['F']:
        overall_points -= 5

    if pnl < pnl_thresholds['F']:
        overall_points -= 15

    # Determine overall tier
    if overall_points >= 115:
        overall_tier = 'SS'
    elif overall_points >= 105:
        overall_tier = 'S'
    elif overall_points >= 90:
        overall_tier = 'A+'
    elif overall_points >= 80:
        overall_tier = 'A'
    elif overall_points >= 65:
        overall_tier = 'B+'
    elif overall_points >= 55:
        overall_tier = 'B'
    elif overall_points >= 40:
        overall_tier = 'C+'
    elif overall_points >= 30:
        overall_tier = 'C'
    else:
        overall_tier = 'F'

    # Determine tier for each category (reusing get_points function for simplicity)
    def get_tier(points):
        if points == 25:
            return 'S'
        elif points == 15:
            return 'A'
        elif points == 10:
            return 'B'
        elif points == 5:
            return 'C'
        else:
            return 'F'

    # Return results
    return {
        "overall": overall_tier,
        "win_rate": get_tier(win_rate_points // 2),  # Undo doubling for accurate tier
        "trades": get_tier(trades_points),
        "size": get_tier(size_points),
        "pnl": get_tier(pnl_points)
    }


async def get_sol_price(token_mint='So11111111111111111111111111111111111111112'):
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_mint}'
    max_retries = 3  # Number of retries
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        token_data = await response.json()

                        price = float(token_data['pairs'][0]['priceUsd'])

                        return price

                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")

        except Exception as e:
            retries += 1
            print(f"Error: {e}, DXS retrying in 1 second...")
            await asyncio.sleep(1)
        else:
            # If successful, exit the loop
            break

    if retries >= max_retries:
        print("Failed to fetch data after retries.")
        return 200


async def get_weth_price(token_mint='7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs'):
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_mint}'
    max_retries = 3  # Number of retries
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        token_data = await response.json()

                        price = float(token_data['pairs'][0]['priceUsd'])

                        return price

                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")

        except Exception as e:
            retries += 1
            print(f"Error: {e}, DXS retrying in 1 second...")
            await asyncio.sleep(1)
        else:
            # If successful, exit the loop
            break

    if retries >= max_retries:
        print("Failed to fetch data after retries.")
        return 2300


# DONE -> Fix insertions
async def insert_wallet_into_db(data, db_url=pg_db_url):
    # Connect to the PostgreSQL database asynchronously
    conn = await asyncpg.connect(dsn=db_url)
    try:
        # Prepare the INSERT INTO statement with PostgreSQL syntax
        query = """
        INSERT INTO wallets(wallet, trades, win_rate, avg_size,last_checked, pnl, window_value)
        VALUES($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (wallet) DO UPDATE SET
        trades = EXCLUDED.trades, 
        win_rate = EXCLUDED.win_rate,
        avg_size = EXCLUDED.avg_size,
        last_checked = EXCLUDED.last_checked,
        pnl = EXCLUDED.pnl,
        window_value = EXCLUDED.window_value
        """
        # Convert the dictionary to a list of values in the correct order for the placeholders
        values = [data['wallet'], data['trades'], data['win_rate'],
                  data['avg_size'], data['last_checked'], data['pnl'], data['window_value']]
        # Execute the query
        await conn.execute(query, *values)
    finally:
        await conn.close()  # Ensure the connection is closed


async def is_wallet_outdated(wallet_address, db_url=pg_db_url):
    # Calculate the threshold timestamp for 1 day ago
    one_day_ago = int(time.time()) - (24 * 60 * 60)

    # Connect to the PostgreSQL database asynchronously
    conn = await asyncpg.connect(dsn=db_url)
    try:
        # Prepare the SELECT statement to find the last_checked value for the given wallet
        query = """
        SELECT last_checked FROM wallets WHERE wallet = $1
        """
        # Execute the query
        result = await conn.fetchval(query, wallet_address)

        # Check if the wallet was found and if its last_checked is older than one day ago
        if not result:
            return True
        elif result < one_day_ago:
            return True
        else:
            return False
    finally:
        await conn.close()  # Ensure the connection is closed


# DONE -> Fix data retrieval
async def get_wallet_data(wallet_address, db_url=pg_db_url):
    # Connect to the PostgreSQL database asynchronously
    conn = await asyncpg.connect(dsn=db_url)
    try:
        # Prepare the SELECT statement to find the wallet by address
        query = "SELECT * FROM wallets WHERE wallet = $1"
        row = await conn.fetchrow(query, wallet_address)
        if row:
            # Map the row to a dictionary. asyncpg returns a Record which can be accessed by keys.
            data = {
                'wallet': row['wallet'],
                'trades': row['trades'],
                'win_rate': row['win_rate'],
                'avg_size': row['avg_size'],
                'last_checked': row['last_checked'],
                'pnl': row['pnl'],
                'window_value': row['window_value']
            }
            return data
        else:
            return {}  # Return an empty dict if no wallet is found
    finally:
        await conn.close()  # Ensure the connection is closed


async def process_wallet(wallet_address, window=30):
    if await is_wallet_outdated(wallet_address):
        # Get last 30 days of SPL Buy TXs
        print('FETCHING TXS')
        thirty_day_swaps = await get_wallet_txs(wallet_address, window=window)
        print('FETCHED TXS')

        sol_price = await get_sol_price()

        pnl = round(await calculate_pnl(thirty_day_swaps, sol_price), 2)

        # pprint(f'PNL DONE: ${pnl}')

        thirty_day_buys = filter_for_buys(thirty_day_swaps)

        print(f'BUYS FILTERED')

        thirty_day_buys = deduplicate_transactions(thirty_day_buys)

        trades = len(thirty_day_buys)
        token_mints_and_timestamps = []
        token_mints_and_timestamps = [[buy["out_mint"], buy["timestamp"]] for buy in thirty_day_buys
                                      if [buy["out_mint"], buy["timestamp"]] not in token_mints_and_timestamps]

        # pprint(f'TMT: \n{token_mints_and_timestamps}')

        pool = await asyncpg.create_pool(dsn=pg_db_url)

        print("POOL CREATED")

        # fetch token info up front and at once
        tasks = [token_prices_to_db(tmt[0], tmt[1], int(time.time()), pool=pool) for tmt in token_mints_and_timestamps]
        # TODO the line above is very expensive, check for further optimisations.
        await asyncio.gather(*tasks)

        print('PRICES_UPDATED')

        wins = 0
        size = 0
        weth_price = await get_weth_price()

        # DONE -> Cleanup up db operations and added concurrent processing.
        tasks = []
        for trade in thirty_day_buys:
            in_mint = trade['in_mint']
            in_amt = trade['in_amt']
            token_mint = trade['out_mint']
            timestamp = trade['timestamp']

            # Update `size` based on conditions
            if in_mint == 'So11111111111111111111111111111111111111112':
                size += in_amt * sol_price
            elif in_mint == '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs':
                size += in_amt * weth_price
            else:
                size += in_amt

            # Create a task for each `is_win_trade` call
            task = asyncio.create_task(is_win_trade(token_mint, timestamp, pool=pool))
            tasks.append(task)

        # Await all tasks to finish and count wins
        win_results = await asyncio.gather(*tasks)
        wins += sum(win_results)

        await pool.close()

        try:
            win_rate = round((wins / trades * 100), 2)
            avg_size = round((size / trades), 2) * -1
            print(
                f'Win Rate For {wallet_address} is {win_rate}\nThey took {trades} trades\n'
                f'Their Avg. Size is ${avg_size}')
        except ZeroDivisionError:
            win_rate = 0
            avg_size = 0

        wallet_summary = {
            'wallet': wallet_address,
            'trades': trades,
            'win_rate': win_rate,
            'avg_size': avg_size,
            'last_checked': int(time.time()),
            'pnl': pnl,
            "window_value": f"{window}d".zfill(3)
        }

        pprint(f'{wallet_address[:5]}\'s Wallet Summary')
        pprint(wallet_summary)
        print('\n\n')

        await insert_wallet_into_db(wallet_summary)
        return wallet_summary

    else:
        return await get_wallet_data(wallet_address)


async def calculate_pnl(transactions, sol_price):
    # Initialize net amounts
    net_sol = 0
    net_usd = 0

    # Constants for mint addresses
    SOL_MINT = 'So11111111111111111111111111111111111111112'
    USD_MINTS = ['EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB']

    # Iterate through each transaction
    for txn in transactions:
        if txn['out_mint'] == SOL_MINT or txn['in_mint'] == SOL_MINT:
            net_sol += txn['out_amt'] if txn['out_mint'] == SOL_MINT else 0
            net_sol += txn['in_amt'] if txn['in_mint'] == SOL_MINT else 0
        if txn['out_mint'] in USD_MINTS or txn['in_mint'] in USD_MINTS:
            net_usd += txn['out_amt'] if txn['out_mint'] in USD_MINTS else 0
            net_usd += txn['in_amt'] if txn['in_mint'] in USD_MINTS else 0

    # Calculate PnL
    pnl = (net_sol * sol_price) + net_usd
    return pnl


def filter_for_buys(txs):
    buy_tokens = {
        'So11111111111111111111111111111111111111112',
        'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
        '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs'
    }
    return [tx for tx in txs if tx['in_mint'] in buy_tokens]


async def get_swap_amounts(swap_data, wallet_address):
    balance_changes = {}
    for transfer in swap_data:
        mint = transfer['mint']

        # await get_metadata(mint)

        amount = transfer['tokenAmount']

        if transfer['fromUserAccount'] == wallet_address:
            balance_change = -amount
        elif transfer['toUserAccount'] == wallet_address:
            balance_change = amount
        else:
            continue

        if mint in balance_changes:
            balance_changes[mint] += balance_change
        else:
            balance_changes[mint] = balance_change

    # Filter out items with a balance change of zero
    return [[mint, round(balance, 2)] for mint, balance in balance_changes.items() if balance != 0]


async def parse_tx_get_swaps(tx: dict):
    wallet = tx['feePayer']
    tx_id = tx['signature']
    source = tx['source']
    timestamp = tx['timestamp']
    swap_data = tx['tokenTransfers']

    bad_tx = {'tx_id': tx_id,
              'wallet': None,
              'in_mint': None,
              'in_amt': None,
              'out_mint': None,
              'out_amt': None,
              'timestamp': None
              }

    if wallet == 'DCAKxn5PFNN1mBREPWGdk1RXg5aVH9rPErLfBFEi2Emb':
        return bad_tx

    if source not in ("JUPITER", "UNKNOWN", "RAYDIUM", "ORCA"):
        return bad_tx

    elif source in ('UNKNOWN', 'JUPITER', 'RAYDIUM', "ORCA"):

        # if NFT return bad tx

        swap_amounts = await get_swap_amounts(swap_data, wallet)

        if len(swap_amounts) != 2:
            return bad_tx

        if swap_amounts[0][1] < 0:
            in_mint = swap_amounts[0][0]
            in_amt = swap_amounts[0][1]

            out_mint = swap_amounts[1][0]
            out_amt = swap_amounts[1][1]
        else:
            in_mint = swap_amounts[1][0]
            in_amt = swap_amounts[1][1]

            out_mint = swap_amounts[0][0]
            out_amt = swap_amounts[0][1]

        payload = {'tx_id': tx_id,
                   'wallet': wallet,
                   'in_mint': in_mint,
                   'in_amt': in_amt,
                   'out_mint': out_mint,
                   'out_amt': out_amt,
                   'timestamp': timestamp
                   }

        return payload


# DONE
# "window should be 1, 7, or 30. represents no. of days to fetch txs for"

async def get_wallet_txs(wallet: str, api_key=helius_api_key, tx_type='', db_url=pg_db_url, window=30):
    conn = await asyncpg.connect(dsn=db_url)

    # Fetching the latest transaction timestamp for the wallet
    query = "SELECT MAX(timestamp) FROM txs WHERE wallet = $1"
    last_db_tx_timestamp = await conn.fetchval(query, wallet)

    # print(f'LAST DB TIMESTAMP: {last_db_tx_timestamp}')

    days_of_data_to_fetch = 0

    if not last_db_tx_timestamp:
        time_since_last_update = 31 * 24 * 60 * 60
    else:
        time_since_last_update = int(time.time()) - last_db_tx_timestamp

    if time_since_last_update > 24 * 60 * 60:  # seconds in one day
        days_of_data_to_fetch = round(time_since_last_update / (24 * 60 * 60))

    base_url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={api_key}"
    if tx_type != '':
        base_url += f'&type={tx_type}'
    secs_ago = int(days_of_data_to_fetch * 24 * 60 * 60)
    count = 0
    tx_data = []
    last_tx_sig = None
    zero_trigger = True
    start_time = int(time.time())
    right_now_timestamp = int(time.time())  # Current timestamp
    max_retries = 3  # Number of retries

    while ((right_now_timestamp >= (start_time - secs_ago)) and zero_trigger and (count <= 35)
           and days_of_data_to_fetch > 0):
        url = base_url
        count += 1
        if last_tx_sig:  # Append 'before' parameter only for subsequent requests
            url += f'&before={last_tx_sig}'

        retries = 0
        while retries < max_retries:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            tx_data_batch = await response.json()

                            if not tx_data_batch:  # Empty response, exit loop
                                zero_trigger = False
                                break

                            for tx in tx_data_batch:

                                if last_db_tx_timestamp and tx['timestamp'] <= last_db_tx_timestamp:
                                    continue
                                tx_data.append(tx)

                            last_tx = tx_data_batch[-1]
                            last_tx_sig = last_tx['signature']

                            right_now_timestamp = last_tx['timestamp']
                            break  # Break from retry loop on success

                        else:
                            raise Exception(f"Failed to fetch data, status code: {response.status}")

            except Exception as e:
                retries += 1
                print(f"Error: {e}, retrying in 5 seconds...")
                await asyncio.sleep(5)

            if retries >= max_retries:
                print("Failed to fetch data after retries.")
                return []

    # slow version of the comprehension
    # trfsList = [tx["tokenTransfers"] for tx in thirty_day_txs if (tx['feePayer'] !=
    # 'DCAKxn5PFNN1mBREPWGdk1RXg5aVH9rPErLfBFEi2Emb' and tx['source'] not in ("JUPITER", "UNKNOWN"))]

    # GET METADATA IS CALLED ON BAD TOKENS
    # FILTER DOWN BEFORE GATHERING TASKS

    '''
    trfsList = [tx["tokenTransfers"] for tx in tx_data]
    mints = {trf["mint"] for trfs in trfsList for trf in trfs}
    tasks = [get_metadata(mint) for mint in mints]
    await asyncio.gather(*tasks)
    '''

    swap_txs = await parse_for_swaps(tx_data)  # No I/O in here

    swap_txs_tuples = [(tx['tx_id'], tx['wallet'], tx['in_mint'], tx['in_amt'], tx['out_mint'], tx['out_amt'],
                        tx['timestamp']) for tx in swap_txs]
    try:
    # Inserting new swap transactions into the database
        if swap_txs_tuples:  # TODO I/O -> Check later for improvable parts.
            # await conn.executemany(
            #     "INSERT INTO txs (txid, wallet, in_mint, in_amt, out_mint, out_amt, timestamp) "
            #     "VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (txid) DO NOTHING",
            #     swap_txs_tuples
            # )

            await conn.copy_records_to_table("txs", records=swap_txs_tuples)

        # Retrieving swap transactions within a specific time window
        end_time = int(time.time())
        start_time = end_time - (window * 24 * 60 * 60)

        query = "SELECT * FROM txs WHERE wallet = $1 AND timestamp BETWEEN $2 AND $3"

        rows = await conn.fetch(query, wallet, start_time, end_time)  # TODO -> Some I/O here check later for improvements
        swap_txs_in_window = [{column: value for column, value in zip(row.keys(), row.values())} for row in rows]

    
    except Exception as e:
        print(f"Error {e} while running db insertion/retrieval operations for wallet {wallet}.")
        return []

    return swap_txs_in_window


async def parse_for_swaps(tx_data):
    txs = []
    async def wrapper(tx):
        str_val = str(tx)
        substrings = ('NonFungible', 'TENSOR', "'ProgrammableNFT'", 'FLiPggWYQyKVTULFWMQjAk26JfK5XRCajfyTmD5weaZ7')
        if any(sub in str_val for sub in substrings):
            return None
        # pprint(f'\n\n\nValid TX: \n{tx}')
        payload = await parse_tx_get_swaps(tx)
        # Only valid swap txs
        if payload['wallet'] is not None:
            return payload

    # Filter to include only swaps
    tasks = [wrapper(tx) for tx in tx_data]
    txs = await asyncio.gather(*tasks)
    txs = [tx for tx in txs if tx]
    return txs

if __name__ == '__main__':
    asyncio.run(process_wallet('98HgTyeHTp18q32RHiZtjXKbumZoC8JThrG31YkXLQeV'))
