import re
import time, aiohttp, asyncio, base58, asyncpg, random

from pprint import pprint

from priceDataModule.price_utils import is_win_trade, token_prices_to_db

from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError

pg_db_url = 'postgresql://bmaster:BlockSight%23Master@109.205.180.184:6432/blocksight'
helius_api_key = 'cfc89cfc-2749-487b-9a76-58b989e70909'


def is_valid_wallet(wallet_address: str) -> bool:
    """
    Check if the given wallet address is valid by attempting to decode it and create a VerifyKey object.

    Args:
        wallet_address (str): The wallet address to validate.

    Returns:
        bool: True if the wallet address is valid, False otherwise.
    """
    try:
        # Decode the string from to bytes
        point_bytes = base58.b58decode(wallet_address)

        # Attempt to create a VerifyKey object, which will validate the point
        VerifyKey(point_bytes)

        return True
    except (ValueError, BadSignatureError):
        # If there's an error in decoding or the point is not on the curve
        return False


def is_valid_channel(channel_name) -> bool:
    """
    Check if the given channel is valid.

    Args:
        channel_name (str): The wallet address to validate.

    Returns:
        bool: True if the wallet address is valid, False otherwise.
    """

    match = re.search(r't\.me/([^/]+)', channel_name)
    if match:
        channel_name = match.group(1)

    return bool(re.match(r'^[a-zA-Z0-9_]+$', channel_name))


def deduplicate_transactions(transactions: list) -> list:
    """
    Deduplicate transactions by sorting them and ensuring that each out_mint appears only once within a 15-minute window.

    Args:
        transactions (list): A list of transaction dictionaries to deduplicate.

    Returns:
        list: A deduplicated list of transaction dictionaries.
    """
    # Sort transactions by 'out_mint' and then by 'timestamp'
    sorted_txs = sorted(transactions, key=lambda x: (x['out_mint'], x['timestamp']))

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


def determine_wallet_grade(trades: int, win_rate: float, avg_size: float, pnl: float, window=30) -> dict:
    """
    Determine the grade of a wallet based on its trading performance metrics.

    Args:
        window
        trades (int): The number of trades.
        win_rate (float): The win rate percentage.
        avg_size (float): The average size of trades.
        pnl (float): The profit and loss.

    Returns:
        dict: A dictionary containing the overall grade and individual grades for each metric.

    """
    # Define thresholds for each category
    win_rate_thresholds = {'S': 30, 'A': 25, 'B': 20, 'C': 15, 'F': 10}
    size_thresholds = {'S': 1500, 'A': 1000, 'B': 750, 'C': 500, 'F': 200}

    if window == 30:
        trades_thresholds = {'S': 150, 'A': 100, 'B': 50, 'C': 25, 'F': 10}
        pnl_thresholds = {'S': 40000, 'A': 25000, 'B': 15000, 'C': 7500, 'F': 2500}
    elif window == 7:
        trades_thresholds = {'S': 150 / 4, 'A': 100 / 4, 'B': 50 / 4, 'C': 25 / 4, 'F': 10 / 4}
        pnl_thresholds = {'S': 40000 / 4, 'A': 25000 / 4, 'B': 15000 / 4, 'C': 7500 / 4, 'F': 2500 / 4}
    else:
        trades_thresholds = {'S': 150 / 10, 'A': 100 / 10, 'B': 50 / 10, 'C': 25 / 10, 'F': 10 / 10}
        pnl_thresholds = {'S': 40000 / 10, 'A': 25000 / 10, 'B': 15000 / 10, 'C': 7500 / 10, 'F': 2500 / 10}

    # Helper function to determine points based on value and thresholds
    def get_points(value, thresholds):
        try:
            if value >= thresholds['S']:
                return 25
            elif value >= thresholds['A']:
                return 15
            elif value >= thresholds['B']:
                return 10
            elif value >= thresholds['C']:
                return 5
            else:
                return 1
        except Exception as e:
            print(e)
            raise e

    # -------------------------------------------------- #
    def help_me():
        # Calculate points for each category
        win_rate_points = get_points(win_rate, win_rate_thresholds) * 2  # Double points for win rate
        trades_points = get_points(trades, trades_thresholds)
        size_points = get_points(avg_size, size_thresholds)
        pnl_points = get_points(pnl, pnl_thresholds)

        return win_rate_points, trades_points, size_points, pnl_points

    win_rate_points, trades_points, size_points, pnl_points = help_me()

    # Calculate overall points
    overall_points = win_rate_points + trades_points + size_points + pnl_points

    # Adjust overall points for Tier F
    if win_rate < win_rate_thresholds['F']:
        overall_points -= 15

    if trades < trades_thresholds['F']:
        overall_points -= 15

    if trades < window / 3:
        overall_points -= 20

    if trades > window * 20:
        overall_points -= 10

    if avg_size < size_thresholds['F']:
        overall_points -= 5

    if pnl < pnl_thresholds['F']:
        overall_points -= 20

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
        "overall_grade": overall_tier,
        "win_rate_grade": get_tier(win_rate_points // 2),  # Undo doubling for accurate tier
        "trades_grade": get_tier(trades_points),
        "size_grade": get_tier(size_points),
        "pnl_grade": get_tier(pnl_points),
        "points": overall_points
    }


def determine_tg_grade(trades: int, win_rate: float, window=30) -> dict:
    """
    Determine the grade of a tg channel based on its performance metrics.

    Args:
        window
        trades (int): The number of trades.
        win_rate (float): The win rate percentage.

    Returns:
        dict: A dictionary containing the overall grade and individual grades for each metric.
    """

    # Define thresholds for each category
    win_rate_thresholds = {'S': 45, 'A': 30, 'B': 20, 'C': 15, 'F': 10}
    trades_thresholds = {'S': 30, 'A': 25, 'B': 20, 'C': 15, 'F': 10}

    profit_per_bet = 2.5
    win_prob = win_rate / 100
    pnl = (profit_per_bet * trades * win_prob - 1 * trades)

    trades_thresholds = {'S': 30, 'A': 25, 'B': 20, 'C': 15, 'F': 10}
    pnl_thresholds = {'S': 10, 'A': 7, 'B': 5, 'C': 3, 'F': 1}

    if window == 7:
        trades_thresholds = {key: value // 4 for key, value in trades_thresholds.items()}
        pnl_thresholds = {key: value // 4 for key, value in pnl_thresholds.items()}
    elif window == 3:
        trades_thresholds = {key: value // 10 for key, value in trades_thresholds.items()}
        pnl_thresholds = {key: value // 10 for key, value in pnl_thresholds.items()}

    # Helper function to determine points based on value and thresholds
    def get_points(value, thresholds):
        try:
            if value >= thresholds['S']:
                return 25
            elif value >= thresholds['A']:
                return 15
            elif value >= thresholds['B']:
                return 10
            elif value >= thresholds['C']:
                return 5
            else:
                return 1
        except Exception as e:
            print(e)
            raise e

    # -------------------------------------------------- #
    def help_me():
        # Calculate points for each category
        win_rate_points = get_points(win_rate, win_rate_thresholds) * 2  # Double points for win rate
        trades_points = get_points(trades, trades_thresholds)
        pnl_points = get_points(pnl, pnl_thresholds)

        return win_rate_points, trades_points, pnl_points

    win_rate_points, trades_points, pnl_points = help_me()

    # Calculate overall points
    overall_points = win_rate_points + trades_points + pnl_points

    # Adjust overall points for Tier F
    if win_rate < win_rate_thresholds['F']:
        overall_points -= 10

    if trades < trades_thresholds['F']:
        overall_points -= 5

    if trades < 10:
        overall_points -= 10

    if pnl < pnl_thresholds['F']:
        overall_points -= 10

    # Determine overall tier
    if overall_points >= 100:
        overall_tier = 'SS'
    elif overall_points >= 90:
        overall_tier = 'S'
    elif overall_points >= 80:
        overall_tier = 'A+'
    elif overall_points >= 70:
        overall_tier = 'A'
    elif overall_points >= 60:
        overall_tier = 'B+'
    elif overall_points >= 50:
        overall_tier = 'B'
    elif overall_points >= 45:
        overall_tier = 'C+'
    elif overall_points >= 40:
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
        "overall_grade": overall_tier,
        "win_rate_grade": get_tier(win_rate_points // 2),  # Undo doubling for accurate tier
        "trades_grade": get_tier(trades_points),
        "pnl_grade": get_tier(pnl_points),
        "pnl": round(pnl, 2)
    }


def generate_trader_message(data):
    grade = data['overall_grade']
    pnl = data['pnl_grade']
    frequency = data['trades_grade']
    win_rate = data['win_rate_grade']
    avg_size = data['size_grade']

    # Define your message categories
    if grade in ['SS', 'S'] and avg_size in ['S', 'A'] and frequency in ['S', 'A']:
        return ("The Solana market's VIP! A trading juggernaut, splashing around big bets like a "
                "celebrity at a Vegas pool party. Who the fuck are you and why are you so good at everything?")

    elif grade in ['SS', 'S'] and avg_size in ['C', 'F'] and pnl in ['S', 'A']:
        return ("The silent assassin! With a portfolio stealthier than a cat burglar, "
                "you’re raking in consistent wins. Small, sneaky, and successful.")

    elif grade in ['S', 'A'] and avg_size in ['C', 'F'] and pnl in ['S', 'A']:
        return ("The sniper, picking off wins with surgical precision. You might not be making it rain,"
                " but your steady hand is writing a success story one trade at a time.")

    elif grade in ['A'] and frequency in ['S', 'A'] and avg_size in ['S', 'A']:
        return ("The celebrity trader! Flashy, frequent, and fabulously wealthy. "
                "You’re not just in the market, you ARE the market. Autographs, please?")

    elif grade in ['A', 'B+'] and avg_size in ['S', 'A'] and pnl in ['C', 'F']:
        return ("The bold gambler, throwing around SOL like it’s confetti at a New Year’s party."
                " Sure, it’s a bit of a coin flip, but who doesn't love a bit of drama?")

    elif grade in ['B', 'C'] and avg_size in ['C', 'F'] and win_rate in ['S', 'A']:
        return ("The scrappy underdog! Not the flashiest wallet in the room, "
                "but you’ve got a golden touch. Like a ninja in a bank vault, making each move count.")

    elif grade in ['C', 'F'] and avg_size in ['B', 'C'] and pnl in ['C', 'F']:
        return "You're trash, Get a life off your screen"

    elif grade in ['B', 'C'] and frequency in ['S', 'A'] and avg_size in ['B', 'C']:
        return ("Busy as a bee, buzzing from one trade to the next. Not the biggest player on the field,"
                " but definitely one of the most energetic. Go, Speed Racer, go!")

    elif grade in ['F'] and frequency in ['C', 'F'] and avg_size in ['C', 'F']:
        return "Shit Shit Shit -- Your wallet is a waste of good SOL. Fuck Off."

    elif grade in ['C'] and frequency in ['C', 'F'] and pnl in ['C', 'F']:
        return "While you aren't total trash, Stop trading. You're NGMI."

    elif grade in ['A', 'B+'] and frequency in ['B', 'C'] and avg_size in ['B', 'C']:
        return "You're okay, I guess."

    elif grade in ['A', 'B'] and avg_size in ['S', 'A'] and pnl in ['S', 'A']:
        return ("The steady giant, moving through the market like a wise old elephant. "
                "Big, sure, but with a grace that keeps the wins coming. Slow and steady wins the race!")

    elif grade in ['S', 'A'] and frequency in ['B', 'C'] and avg_size in ['B', 'C']:
        return ("You're the Solana market's all-rounder. Not too hot, not too cold – just right. "
                "Like the Goldilocks of trading, but with a sharper suit.")

    elif grade in ['C', 'F'] and pnl in ['F'] and frequency in ['S', 'A']:
        return "You are a professional -- AT LOSING MONEY HA!"

    elif grade in ['C', 'F'] and avg_size in ['S', 'A'] and frequency in ['C', 'F']:
        return "Traders like you pay my bills"

    elif grade in ['SS', 'S'] and frequency in ['S', 'A'] and pnl in ['C', 'F']:
        return ("The speed demon with a penchant for danger! Racing through trades like a "
                "Formula 1 driver. Fast, furious, and living on the edge!")

    elif grade in ['S'] and avg_size in ['S'] and pnl in ['S'] and win_rate in ['S'] and frequency in ['B', 'C', 'F']:
        return "What a degen, talk about conviction trades."

    elif grade in ['SS', 'S', 'A'] and pnl in ['S', 'A'] and avg_size in ['C', 'F']:
        return ("The silent winner, sneaking in wins like a ninja in the night. "
                "Small in size, but colossal in impact. Who says you need to shout to be heard?")

    elif grade in ['S', 'A'] and avg_size in ['A', 'B', 'C'] and pnl in ['A', 'S'] and win_rate in ['S']:
        return "Who are you and why is your wallet so sexy?"

    elif grade in ['A', 'B+'] and frequency in ['C', 'F'] and avg_size in ['B', 'C']:
        return ("The casual trader, strolling through the Solana market like it’s a "
                "Sunday walk in the park. Not too rushed, enjoying the scenery, making moves at a leisurely pace.")

    elif grade in ['B+', 'A'] and avg_size in ['A', 'B'] and pnl in ['A', 'B'] and win_rate in ['B', 'C']:
        return "You're quite profitable, but you should buy dumb shit less often."

    elif (grade in ['B+', 'B', 'C'] and avg_size in ['B', 'C'] and pnl in ['F'] and win_rate in ['S'] and
          frequency in ['S']):
        return "You're good at a few things, but you aren't making money. Get good or get out."

    elif grade in ['C', 'F'] and avg_size in ['C', 'F'] and pnl in ['S', 'A']:
        return "Schrodinger's Degen - You're shit but you make money? Teach me ser."

    else:
        # Default message for unclassified scenarios
        return random.choice(["Hmm", "What do we have here?", "Weird..", "The voices, make them stoppp",
                              "I just bought a used car"])


def generate_tg_message(data):
    grade = data['overall_grade']
    pnl = data['pnl_grade']
    frequency = data['trades_grade']
    win_rate = data['win_rate_grade']

    if grade == 'SS':
        return random.choice([
            'This caller is a gift to Solana.',
            'Who are you and why are you so perfect?',
            'A bona fide money printer, ape everything they call.'
        ])

    elif grade == 'S':
        if frequency != 'S':
            if frequency in ['F', 'C']:
                return 'You\'re a winner, but you really love to play it safe, huh?'
            return random.choice([
                'You\'re amazing, I just wish you made a few more calls.',
                'Every degen should worship you. Just make a few more calls.'
            ])
        return 'Top tier performance! Keep up the great calls!'

    elif grade == 'A':
        if frequency in ['C', 'F']:
            return random.choice([
                'You\'re doing okay, but you need to make more calls.',
                'Nice calls! Making more good calls should give you a boost.'
            ])
        elif frequency in ['A', 'B']:
            return random.choice([
                'Solid performance with consistent results!',
                'You\'re managing well, just a bit more activity could skyrocket your results!'
            ])
        return 'Great job! You’re on your way to the top tiers.'

    elif grade == 'B':
        if frequency in ['C', 'F']:
            return random.choice([
                'Decent effort! Increasing your trade frequency might help.',
                'You’ve got potential; just need to take more action!'
            ])
        elif frequency in ['A', 'B']:
            return random.choice([
                'Good foundation! A little tweak here and there could make a big difference.',
                'You\'re on the right track, now push a little harder to improve your calls.'
            ])
        return 'Not bad at all, but there’s room for improvement.'

    elif grade == 'C':
        if frequency in ['S', 'A']:
            return random.choice([
                'You’re quite active, which is great! Now let’s focus on making each call count.',
                'Activity isn’t your issue; it’s time to improve the quality of those calls.'
            ])
        elif frequency in ['C', 'F']:
            return random.choice([
                'Needs improvement, but keep pushing! Focus on both the quality and quantity of calls.',
                'More activity could help, but also revisit your strategies.'
            ])
        return 'Keep pushing! Consistency and quality need some work.'

    elif grade == 'F':
        return random.choice([
            'This caller is really really bad. Don\'t waste time or money with this channel.',
            'What a waste of time lmao. I\'m a bot and I almost puked after looking at his channel.',
            'Nope, just nope.'
        ])


async def get_sol_price(token_mint: str = 'So11111111111111111111111111111111111111112') -> float:
    """
    Fetch the current price of SOL from the Dexscreener API.

    Args:
        token_mint (str, optional): The mint address of the SOL token. Defaults to the official SOL mint address.

    Returns:
        float: The current price of SOL in USD.
    """
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
        return 180


async def get_weth_price(token_mint: str = '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs') -> float:
    """
    Fetch the current price of WETH from the Dexscreener API.

    Args:
        token_mint (str, optional): The mint address of the WETH token. Defaults to the official WETH mint address.

    Returns:
        float: The current price of WETH in USD.
    """
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
        return 3500


# DONE -> Fix insertions
async def insert_wallet_into_db(data: dict, db_url: str = pg_db_url, pool=None) -> None:
    """
    Insert or update wallet data into the database.

    Args:
        data (dict): A dictionary containing wallet data with keys corresponding to database columns.
        db_url (str): Database connection URL.

    Returns:
        None
    """
    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)
    try:
        # Prepare the INSERT INTO statement with PostgreSQL syntax
        query = """
        INSERT INTO wallets(wallet, trades, win_rate, avg_size, last_checked, pnl, window_value)
        VALUES($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (wallet, window_value) DO UPDATE SET
            trades = EXCLUDED.trades, 
            win_rate = EXCLUDED.win_rate,
            avg_size = EXCLUDED.avg_size,
            last_checked = EXCLUDED.last_checked,
            pnl = EXCLUDED.pnl;
        """
        # Convert the dictionary to a list of values in the correct order for the placeholders
        values = [data['wallet'], data['trades'], data['win_rate'],
                  data['avg_size'], data['last_checked'], data['pnl'], data['window_value']]
        # Execute the query
        await conn.execute(query, *values)
    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


async def is_wallet_outdated(wallet_address: str, db_url: str = pg_db_url, window: int = 30, pool=None) -> bool:
    """
    Check if the wallet data in the database is outdated (older than 1 day).

    Args:
        pool: pg pool
        window:
        wallet_address (str): The wallet address to check.
        db_url (str): Database connection URL.

    Returns:
        bool: True if the wallet data is outdated, False otherwise.
    """
    # Calculate the threshold timestamp for 1 day ago
    one_day_ago = int(time.time()) - (24 * 60 * 60)
    window = f"{window}d"

    if window in [3, 7]:
        window = f"0{window}d"

    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)

    try:
        # Prepare the SELECT statement to find the last_checked value for the given wallet
        query = """
        SELECT last_checked FROM wallets WHERE wallet = $1 and window_value = $2;
        """
        # Execute the query
        result = await conn.fetchval(query, wallet_address, window)

        # Check if the wallet was found and if its last_checked is older than one day ago
        if not result:
            return True
        elif result < one_day_ago:
            return True
        else:
            return False
    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)


async def get_wallet_data(wallet_address: str, pool, window=30) -> dict:
    """
    Retrieve wallet data from the database.

    Args:
        wallet_address (str): The wallet address for which to retrieve data.
        pool: Database connection pool.

    Returns:
        dict: A dictionary containing the wallet data, or an empty dictionary if not found.
    """
    # Connect to the PostgreSQL database asynchronously
    conn = await pool.acquire()
    try:
        # Prepare the SELECT statement to find the wallet by address
        query = "SELECT * FROM wallets WHERE wallet = $1 AND window_value = $2"
        row = await conn.fetchrow(query, wallet_address, f"{window}d".zfill(3))
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
        await pool.release(conn)  # Ensure the connection is closed


async def process_wallet(wallet_address: str, window: int = 30, pool=None) -> dict:
    """
    Process a wallet by fetching transactions, calculating PnL, and updating the database.

    Args:
        pool:
        wallet_address (str): The wallet address to process.
        window (int): The number of days to consider for transaction history.

    Returns:
        dict: A dictionary containing the processed wallet summary.
    """

    if await is_wallet_outdated(wallet_address, window=window, pool=pool):
        # Get last 30 days of SPL Buy TXs
        # print('fetching TXS for {wallet_address}')
        try:
            start = float(time.time())
            thirty_day_swaps = await get_wallet_txs(wallet_address, window=window, pool=pool)
            end = float(time.time())
            print(f'This wallet\'s swaps took: {end - start:.2f} secs to fetch')
        except Exception as e:
            raise e
        # print('FETCHED TXS')
        sol_price = await get_sol_price()

        pnl = round(await calculate_pnl(thirty_day_swaps, sol_price), 2)

        thirty_day_buys = filter_for_buys(thirty_day_swaps)

        # print(f'BUYS FILTERED')

        thirty_day_buys = deduplicate_transactions(thirty_day_buys)

        trades = len(thirty_day_buys)
        token_mints_and_timestamps = []

        token_mints_and_timestamps = [[buy["out_mint"], buy["timestamp"]] for buy in thirty_day_buys
                                      if [buy["out_mint"], buy["timestamp"]] not in token_mints_and_timestamps]

        def trim_list(data):
            unique_entries = {}
            for key, t in data:
                if key not in unique_entries or t < unique_entries[key]:
                    unique_entries[key] = t
            return [[key, unique_entries[key]] for key in sorted(unique_entries)]

        token_mints_and_timestamps = trim_list(token_mints_and_timestamps)
        print(f'There are {len(token_mints_and_timestamps)} tokens to work on')

        start = time.time()
        # prices_pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=20, max_size=150)

        # print(f"POOL CREATED in {time.time() - start:.2f} secs: There are {len(token_mints_and_timestamps)} tokens to fetch data for")

        start = float(time.time())
        try:
            sem = asyncio.BoundedSemaphore(20)

            async def limited_task(tmt):
                async with sem:
                    await token_prices_to_db(tmt[0], tmt[1], int(time.time()), pool=pool)

            # Fetch token info up front and at once
            tasks = [limited_task(tmt) for tmt in token_mints_and_timestamps]

            # Execute tasks concurrently with a limit of 100 tasks at a time
            await asyncio.gather(*tasks)

            # print('PRICES_UPDATED')
        finally:
            pass  # await prices_pool.close()
        end = float(time.time())
        print(
            f"PRICES UPDATED in {end - start:.2f} secs")
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

        await insert_wallet_into_db(wallet_summary, pool=pool)
        wallet_data = wallet_summary

    else:
        wallet_data = await get_wallet_data(wallet_address, window=window, pool=pool)

    if window == 30:
        await process_wallet(wallet_address, window=7, pool=pool)
    if window == 7:
        await process_wallet(wallet_address, window=3, pool=pool)

    return wallet_data


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


def get_swap_amounts(swap_data, wallet_address):
    balance_changes = {}
    for transfer in swap_data:
        mint = transfer['mint']

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


def parse_tx_get_swaps(tx: dict):
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

        swap_amounts = get_swap_amounts(swap_data, wallet)

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


async def get_wallet_txs(wallet: str, api_key=helius_api_key, tx_type='', end_time=None, db_url=pg_db_url,
                         window=30, pool=None):
    end_time = end_time or int(time.time())

    new_conn = not bool(pool)
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=db_url)
    try:
        last_db_tx_timestamp = await conn.fetchval("SELECT MAX(timestamp) FROM txs WHERE wallet = $1", wallet)
        time_since_last_update = (end_time - last_db_tx_timestamp) if last_db_tx_timestamp else 31 * 24 * 60 * 60
        days_of_data_to_fetch = max(0, time_since_last_update // (24 * 60 * 60))

        base_url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={api_key}"
        if tx_type:
            base_url += f'&type={tx_type}'

        secs_ago = days_of_data_to_fetch * 24 * 60 * 60
        count = 0
        tx_data = []
        last_tx_sig = None
        zero_trigger = True
        latest_timestamp = int(time.time())  # Current timestamp

        async with aiohttp.ClientSession() as session:
            while latest_timestamp >= end_time - secs_ago and zero_trigger and count < 30 and days_of_data_to_fetch > 0:
                count += 1
                url = f"{base_url}&before={last_tx_sig}" if last_tx_sig else base_url
                response = await session.get(url)
                if response.status == 200:
                    tx_data_batch = await response.json()
                    if not tx_data_batch:
                        break

                    tx_data.extend(tx for tx in tx_data_batch if
                                   not last_db_tx_timestamp or tx['timestamp'] > last_db_tx_timestamp)
                    last_tx = tx_data_batch[-1]
                    last_tx_sig = last_tx['signature']
                    latest_timestamp = last_tx['timestamp']
                else:
                    print(f"Failed to fetch data, status code: {response.status}")
                    break

        if tx_data:
            swap_txs = await parse_for_swaps(tx_data)  # Assuming no I/O in here
            swap_txs_tuples = {(tx['tx_id'], tx['wallet'], tx['in_mint'], tx['in_amt'], tx['out_mint'], tx['out_amt'],
                                tx['timestamp']) for tx in swap_txs}
            if swap_txs_tuples:
                await conn.copy_records_to_table("txs", records=swap_txs_tuples)

        query = "SELECT * FROM txs WHERE wallet = $1 AND timestamp BETWEEN $2 AND $3"
        rows = await conn.fetch(query, wallet, end_time - (window * 24 * 60 * 60), end_time)

    finally:
        if new_conn:
            await conn.close()
        else:
            await pool.release(conn)

    return [dict(row) for row in rows]


async def parse_for_swaps(tx_data):
    async def wrapper(tx):
        str_val = str(tx)
        substrings = ('NonFungible', 'TENSOR', "'ProgrammableNFT'", 'FLiPggWYQyKVTULFWMQjAk26JfK5XRCajfyTmD5weaZ7')
        if any(sub in str_val for sub in substrings):
            return None
        # pprint(f'\n\n\nValid TX: \n{tx}')
        payload = parse_tx_get_swaps(tx)
        # Only valid swap txs
        if payload['wallet'] is not None:
            return payload

    # Filter to include only swaps
    tasks = [wrapper(tx) for tx in tx_data]

    txs = await asyncio.gather(*tasks)

    txs = [tx for tx in txs if tx]
    return txs


async def fetch_wallet_leaderboard(pool, window='30d', sort_by='win_rate', direction='desc'):

    if window not in ['30d', '03d', '07d']:
        window = '30d'

    min_trades = 5 if window == '30d' else 2
    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=pg_db_url)

    query = (f"SELECT * FROM wallets WHERE trades >= {min_trades} AND window_value = $1 AND avg_size >= 10 "
             "ORDER BY win_rate")
    window_int = int(window[:-1])

    rows = await conn.fetch(query, window)

    graded_list = []
    for record in rows:
        wallet = dict(record)

        wallet['win_rate'] = float(wallet['win_rate'])
        wallet['avg_size'] = float(wallet['avg_size'])
        wallet['pnl'] = float(wallet['pnl'])

        # Calculate wallet grades and update the wallet dictionary directly
        wallet.update(determine_wallet_grade(wallet['trades'], wallet['win_rate'],
                                             wallet['avg_size'], wallet['pnl'], window=window_int))
        graded_list.append(wallet)

    await pool.release(conn) if pool else await conn.close()

    # Sorting step
    if direction == 'desc':
        graded_list = sorted(graded_list, key=lambda d: d[sort_by], reverse=True)
    else:
        graded_list = sorted(graded_list, key=lambda d: d[sort_by])

    return graded_list


async def fetch_tg_leaderboard(pool, window='30d', sort_by='win_rate', direction='desc'):

    if window not in ['30d', '03d', '07d']:
        window = '30d'

    if sort_by not in ['win_rate', 'overall_grade', 'pnl', 'trades_count']:
        sort_by = 'win_rate'

    conn = await pool.acquire() if pool else await asyncpg.connect(dsn=pg_db_url)

    min_calls = 4 if window == '30d' else 1

    query = f"SELECT * FROM channel_stats WHERE trades_count >= {min_calls} AND window_value = $1 ORDER BY win_rate"

    window_int = int(window[:-1])

    rows = await conn.fetch(query, window)

    graded_list = []
    for record in rows:
        channel = dict(record)

        channel['win_rate'] = float(channel['win_rate'])

        # Calculate channel grades and update the channel dictionary directly
        channel.update(determine_tg_grade(channel['trades_count'], channel['win_rate'], window=window_int))
        graded_list.append(channel)

    await pool.release(conn) if pool else await conn.close()

    # Sorting step
    if direction == 'desc':
        graded_list = sorted(graded_list, key=lambda d: d[sort_by], reverse=True)
    else:
        graded_list = sorted(graded_list, key=lambda d: d[sort_by])

    return graded_list


if __name__ == '__main__':
    print(asyncio.run(process_wallet('3AL3N6WgbyMX8XpAV7TSJrHdDxQNDX7R1j5neXVAQVxA', window=7)))
