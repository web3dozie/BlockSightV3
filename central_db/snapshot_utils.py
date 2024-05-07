import asyncio, aiohttp, time, ast

from pprint import pprint
from solana.rpc.api import Client
from solders.pubkey import Pubkey

from dbs.db_operations import useful_wallets
from metadataAndSecurityModule.metadataUtils import rpc_url, get_data_from_helius, get_metadata, get_num_holders


async def get_full_dxs_data(token_mint):
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_mint}'
    max_retries = 2  # Number of retries
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        token_data = await response.json()  # This is a list of pools the token has open

                        lp_age = int(time.time()) - int(token_data['pairs'][0]['pairCreatedAt'] / 1000)
                        fdv = int(token_data['pairs'][0]['fdv'])
                        price = float(token_data['pairs'][0]['priceUsd'])
                        liquidity = token_data['pairs'][0]["liquidity"]["usd"]

                        buys_5m = token_data['pairs'][0]['txns']['m5']['buys']
                        sells_5m = token_data['pairs'][0]['txns']['m5']['sells']
                        volume_5m = token_data['pairs'][0]['volume']['m5']
                        price_change_5m = token_data['pairs'][0]['priceChange']['m5']

                        buys_1h = token_data['pairs'][0]['txns']['h1']['buys']
                        sells_1h = token_data['pairs'][0]['txns']['h1']['sells']
                        volume_1h = token_data['pairs'][0]['volume']['h1']
                        price_change_1h = token_data['pairs'][0]['priceChange']['h1']

                        buys_6h = token_data['pairs'][0]['txns']['h6']['buys']
                        sells_6h = token_data['pairs'][0]['txns']['h6']['sells']
                        volume_6h = token_data['pairs'][0]['volume']['h6']
                        price_change_6h = token_data['pairs'][0]['priceChange']['h6']

                        return {
                            'price': price,
                            'fdv': fdv,
                            'lp_age': lp_age,
                            'liquidity': liquidity,

                            'buys_5m': buys_5m,
                            'sells_5m': sells_5m,
                            'volume_5m': volume_5m,
                            'price_change_5m': price_change_5m,

                            'buys_1h': buys_1h,
                            'sells_1h': sells_1h,
                            'volume_1h': volume_1h,
                            'price_change_1h': price_change_1h,

                            'buys_6h': buys_6h,
                            'sells_6h': sells_6h,
                            'volume_6h': volume_6h,
                            'price_change_6h': price_change_6h
                        }

                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")

        except Exception as e:
            retries += 1
            print(f"Error: {e}, DXS retrying in 10 seconds...: Mint: {token_mint}")
            await asyncio.sleep(0.25)
        else:
            # If successful, exit the loop
            break

    if retries >= max_retries:
        print("Failed to fetch data after retries.")
        return {}


async def get_metadata_security_for_snapshot(token_mint, pool=None, session=None):
    # ------ Initialize data ----- #

    # Run all four I/O bound tasks concurrently
    token_data_future = get_data_from_helius(token_mint, session=session)
    token_metadata_future = get_metadata(token_mint, pool=pool)
    holders_future = get_num_holders(mint=token_mint, session=session)

    # Gather results from all tasks
    token_data, token_metadata, holders = await asyncio.gather(token_data_future, token_metadata_future,
                                                               holders_future)

    client = Client(rpc_url)
    mint_pubkey = Pubkey.from_string(token_mint)
    holders_data = [d['uiAmount'] for d in ast.literal_eval(client.get_token_largest_accounts(mint_pubkey)
                                                            .to_json())['result']['value']]

    # ------                         ----- #

    # ------ Process Data ----- #
    token_decimals = token_data['token_info']['decimals']
    token_supply = round((token_data['token_info']['supply'] / (10 ** token_decimals)), 2)

    if token_metadata['deployer'] == 'TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM':
        lp_current_supply = 0
        lp_initial_supply = 1
    else:
        lp_address = token_metadata['lp_address']
        if not lp_address:
            lp_current_supply = 1
            lp_initial_supply = 2
        else:
            print('LP DATA TRIGGERED')
            lp_data = await get_data_from_helius(lp_address)
            lp_decimals = lp_data['token_info']['decimals']
            lp_current_supply = round(((lp_data['token_info']['supply']) / (10 ** lp_decimals)), 2)
            lp_initial_supply = token_metadata['initial_lp_supply']

    safe_round = lambda x: round(x, 2) if isinstance(x, float) else 100.0

    # ------                ----- #

    # ------ Data to return ----- #
    is_mintable = bool(token_data.get('token_info').get('mint_authority'))
    lp_burnt_percentage = None if lp_current_supply == 1 and lp_initial_supply == 2 else int(
        100 - (lp_current_supply / lp_initial_supply * 100))
    top_10 = safe_round((sum(holders_data[1:10]) / token_supply * 100) if token_supply else 0)
    top_20 = safe_round((sum(holders_data[1:21]) / token_supply * 100) if token_supply else 0)

    snapshot_data = {
        'mint_safe': not is_mintable,
        'lp_safe': None if lp_burnt_percentage is None else lp_burnt_percentage >= 90,
        'num_holders': holders,
        'top_10': top_10,
        'top_20': top_20,
        'starting_liq': token_metadata['starting_liq'],
        'starting_mc': token_metadata['starting_mc'],
        'airdropped': token_metadata['airdropped'],
        'bundled': token_metadata['bundled'],
        'socials': any(token_metadata.get(key) for key in ['twitter', 'telegram', 'other_links'])

    }

    return snapshot_data


async def get_smart_wallets_data(token_mint, pool, sol_price, smart_wallets, window=''):
    if window == '5m':
        time_ago = int(time.time()) - (5 * 60)
    elif window == '1h':
        time_ago = int(time.time()) - (60 * 60)
    elif window == '6h':
        time_ago = int(time.time()) - (6 * 60 * 60)
    else:
        return

    query = """
    WITH buy_transactions AS (
        SELECT
            COUNT(*) AS buy_count,
            SUM(CASE 
                    WHEN in_mint = 'So11111111111111111111111111111111111111112' THEN in_amt * $1
                    WHEN in_mint = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB' THEN in_amt
                    WHEN in_mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' THEN in_amt
                    ELSE 0 
                END) * -1 AS buy_volume
        FROM txs
        WHERE
            wallet = ANY($2) AND
            in_mint IN ('So11111111111111111111111111111111111111112', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
             'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v') AND
            out_mint = $3 AND
            timestamp >= $4
    ),
    sell_transactions AS (
        SELECT
            COUNT(*) AS sell_count,
            SUM(CASE 
                    WHEN out_mint = 'So11111111111111111111111111111111111111112' THEN out_amt * $1
                    WHEN out_mint = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB' THEN out_amt
                    WHEN out_mint = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' THEN out_amt
                    ELSE 0 
                END) AS sell_volume
        FROM txs
        WHERE
            wallet = ANY($2) AND
            out_mint IN ('So11111111111111111111111111111111111111112', 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
             'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v') AND
            in_mint = $3 AND
            timestamp >= $4
    )
    SELECT
        buy_count,
        sell_count,
        (buy_volume + sell_volume) AS total_volume,
        (sell_volume - buy_volume) AS netflows
    FROM buy_transactions, sell_transactions;
    """

    async with pool.acquire() as conn:
        result = await conn.fetchrow(query, sol_price, smart_wallets, token_mint, time_ago)

    buys, sells, total_volume, netflows = (result['buy_count'], result['sell_count'], result['total_volume'],
                                           result['netflows'])
    return {
        f'smart_buys_{window}': buys,
        f'smart_sells_{window}': sells,
        f'smart_volume_{window}': 0 if total_volume is None else total_volume,
        f'smart_netflows_{window}': 0 if netflows is None else netflows
    }


async def get_smart_wallets_data_wrapper(token_mint, pool, sol_price=150):  # TODO CACHE SOL_PRICE SOMEWHERE
    smart_wallets = await useful_wallets(pool=pool)

    # Collect results concurrently
    smart_5m, smart_1h, smart_6h = await asyncio.gather(
        get_smart_wallets_data(token_mint, pool, sol_price, window='5m', smart_wallets=smart_wallets),
        get_smart_wallets_data(token_mint, pool, sol_price, window='1h', smart_wallets=smart_wallets),
        get_smart_wallets_data(token_mint, pool, sol_price, window='6h', smart_wallets=smart_wallets)
    )

    # Merge results
    return {**smart_5m, **smart_1h, **smart_6h}


async def get_smart_tg_calls(token_mint, pool, smart_channels, window=''):
    return {}
    if window == '5m':
        time_ago = int(time.time()) - (5 * 60)
    elif window == '1h':
        time_ago = int(time.time()) - (60 * 60)
    elif window == '6h':
        time_ago = int(time.time()) - (6 * 60 * 60)
    else:
        return

    query = """
    SELECT COUNT(*) AS calls FROM tg_calls
    WHERE 
        channel_id = ANY($1) AND
        token_mint = $2 AND
        timestamp >= $3
    );
    """

    async with pool.acquire() as conn:
        result = await conn.fetchrow(query, smart_channels, token_mint, time_ago)

    return {f'smart_tg_calls{window}': result['calls']}


async def get_smart_tg_calls_wrapper(token_mint, pool):
    # TODO implement useful_channels
    smart_channels = []
    # smart_channels = await useful_channels(pool=pool)

    # Collect results concurrently
    smart_5m, smart_1h, smart_6h = await asyncio.gather(
        get_smart_tg_calls(token_mint=token_mint, pool=pool, smart_channels=smart_channels, window='5m'),
        get_smart_tg_calls(token_mint=token_mint, pool=pool, smart_channels=smart_channels, window='1h'),
        get_smart_tg_calls(token_mint=token_mint, pool=pool, smart_channels=smart_channels, window='6h')
    )

    # Merge results
    return {**smart_5m, **smart_1h, **smart_6h}


async def take_snapshot(token_mint, pool=None, sol_price=150):
    dxs, met_sec, smt_wlt, smt_tg = await asyncio.gather(get_full_dxs_data(token_mint),
                                                         get_metadata_security_for_snapshot(token_mint, pool=pool),
                                                         get_smart_wallets_data_wrapper(token_mint, pool=pool,
                                                                                        sol_price=sol_price),
                                                         get_smart_tg_calls_wrapper(token_mint, pool))

    return {**dxs, **met_sec, **smt_wlt, **smt_tg, **{'token_mint': token_mint, 'call_time': int(time.time())}}
