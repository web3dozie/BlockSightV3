import time, aiohttp, asyncio, aiosqlite, base58

from pprint import pprint as pr

from priceDataModule.price_utils import is_win_trade
from metadataAndSecurityModule.metadataUtils import get_metadata

from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError


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
        return 95


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


async def insert_wallet_into_db(data,
                                db_path='C:\\Users\\Dozie\\Desktop\\BlockSight\\BlockSight V.1.0.0\\dbs\\wallets.db'):
    async with aiosqlite.connect(db_path) as db:
        # Prepare the INSERT INTO statement
        query = """
        INSERT OR REPLACE INTO wallets(wallet, trading_frequency, win_rate, overall_grade, avg_size, last_checked, pnl, 
        n_trading_frequency, n_win_rate, n_avg_size, n_pnl)
        VALUES(:wallet, :trading_frequency, :win_rate, :overall_grade, :avg_size, :last_checked, :pnl, 
        :n_trading_frequency, :n_win_rate, :n_avg_size, :n_pnl)
        """
        # Execute the query
        await db.execute(query, data)
        # Commit changes
        await db.commit()


async def is_wallet_outdated(wallet_address,
                             db_path='C:\\Users\\Dozie\\Desktop\\BlockSight\\BlockSight V.1.0.0\\dbs\\wallets.db'):
    # Calculate the threshold timestamp for 3 days ago
    one_week_ago = int(time.time()) - (3 * 24 * 60 * 60)

    async with (aiosqlite.connect(db_path) as db):
        # Prepare the SELECT statement to find the last_checked value for the given wallet
        query = """
        SELECT last_checked FROM wallets WHERE wallet = :wallet
        """
        # Execute the query
        async with db.execute(query, {'wallet': wallet_address}) as cursor:
            result = await cursor.fetchone()
            # Check if the wallet was found and if its last_checked is older than one week ago
            if not result:
                return True
            elif result[0] < one_week_ago:
                return True
            else:
                return False


async def get_wallet_data(wallet_address,
                          db_path='C:\\Users\\Dozie\\Desktop\\BlockSight\\BlockSight V.1.0.0\\dbs\\wallets.db'):
    async with aiosqlite.connect(db_path) as db:
        # Prepare the SELECT statement to find the wallet by address
        query = "SELECT * FROM wallets WHERE wallet = ?"
        async with db.execute(query, (wallet_address,)) as cursor:
            row = await cursor.fetchone()
            if row:
                # Map the row to a dictionary
                data = {
                    'wallet': row[0],
                    'trading_frequency': row[1],
                    'win_rate': row[2],
                    'overall_grade': row[3],
                    'avg_size': row[4],
                    'last_checked': row[5],
                    'pnl': row[6],
                    'n_trading_frequency': row[7],
                    'n_win_rate': row[8],
                    'n_avg_size': row[9],
                    'n_pnl': row[10]
                }
                return data
            else:
                return {}  # Return None if no wallet is found


async def process_wallet(wallet_address):
    if await is_wallet_outdated(wallet_address):
        # Get last 30 days of SPL Buy TXs
        thirty_day_txs = await get_wallet_txs(wallet_address, start_days_ago=31)
        thirty_day_swaps = await parse_for_swaps(thirty_day_txs)

        sol_price = await get_sol_price()

        pnl = round(await calculate_pnl(thirty_day_swaps, sol_price), 2)

        thirty_day_buys = filter_for_buys(thirty_day_swaps)
        thirty_day_buys = deduplicate_transactions(thirty_day_buys)

        trades = len(thirty_day_buys)
        wins = 0
        size = 0
        weth_price = await get_weth_price()

        for trade in thirty_day_buys:
            in_mint = trade['in_mint']
            in_amt = trade['in_amt']

            token_mint = trade['out_mint']
            timestamp = trade['timestamp']

            if in_mint == 'So11111111111111111111111111111111111111112':
                size += in_amt * sol_price
            elif in_mint == '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs':
                size += in_amt * weth_price
            else:
                size += in_amt

            if await is_win_trade(token_mint, timestamp):
                wins += 1

        try:
            win_rate = round((wins / trades * 100), 2)
            avg_size = round((size / trades), 2) * -1
            print(
                f'Win Rate For {wallet_address} is {win_rate}\nThey took {trades} trades\n'
                f'Their Avg. Size is ${avg_size}')
        except ZeroDivisionError:
            win_rate = 0
            avg_size = 0

        grades = determine_grade(trades, win_rate, avg_size, pnl)

        data_dump = {
            'n_trading_frequency': trades,  # INTEGER
            'n_win_rate': win_rate,  # FLOAT
            'n_avg_size': avg_size,  # FLOAT
            'n_pnl': pnl # INTEGER
        }

        wallet_summary = {
            'wallet': wallet_address,  # TEXT PRIMARY KEY
            'trading_frequency': grades['trades'],  # CHAR
            'win_rate': grades['win_rate'],  # CHAR
            'overall_grade': grades['overall'],  # CHAR
            'avg_size': grades['size'],  # CHAR
            'last_checked': int(time.time()),  # INTEGER
            'pnl': grades['pnl']
            }

        wallet_summary = {**wallet_summary, **data_dump}

        pr(f'{wallet_address[:5]}\'s Wallet Summary')
        pr(wallet_summary)
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

        await get_metadata(mint)

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

    if source not in ("JUPITER", "UNKNOWN"):
        return bad_tx

    elif source in ('UNKNOWN', 'JUPITER'):

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


async def get_wallet_txs(wallet: str, api_key='cfc89cfc-2749-487b-9a76-58b989e70909', start_days_ago=30, tx_type=''):
    base_url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={api_key}"
    if tx_type != '':
        base_url += f'&type={tx_type}'
    secs_ago = int(start_days_ago * 24 * 60 * 60)
    count = 0
    tx_data = []
    last_tx_sig = None
    zero_trigger = True
    start_time = int(time.time())
    last_tx_timestamp = int(time.time())  # Current timestamp
    max_retries = 3  # Number of retries

    while (last_tx_timestamp >= (start_time - secs_ago)) and zero_trigger and (count <= 35):
        url = base_url
        count += 1
        if last_tx_sig:  # Append 'before' parameter only for subsequent requests
            url += f'&before={last_tx_sig}'

        if count > 35:
            return tx_data

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
                                tx_data.append(tx)

                            last_tx = tx_data_batch[-1]
                            last_tx_sig = last_tx['signature']

                            last_tx_timestamp = last_tx['timestamp']
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
    return tx_data


async def parse_for_swaps(tx_data):
    txs = []
    # Filter to include only swaps
    for tx in tx_data:
        # If it's an NFT tx Skip it
        if 'NonFungible' in str(tx):
            continue

        payload = await parse_tx_get_swaps(tx)
        # Only valid swap txs
        if payload['wallet'] is not None:
            txs.append(payload)
    return txs
