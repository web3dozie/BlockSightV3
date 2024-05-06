
import asyncio, aiohttp, time, ast

from pprint import pprint
from solana.rpc.api import Client
from solders.pubkey import Pubkey
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
    client = Client(rpc_url)
    mint_pubkey = Pubkey.from_string(token_mint)

    async def data_for_holders():
        return [d['uiAmount'] for d in ast.literal_eval(client.get_token_largest_accounts(mint_pubkey)
                                                                .to_json())['result']['value']]

    # Run all four I/O bound tasks concurrently
    token_data_future = get_data_from_helius(token_mint, session=session)
    token_metadata_future = get_metadata(token_mint, pool=pool)
    holders_future = get_num_holders(mint=token_mint, session=session)
    data_folders_future = data_for_holders()

    # Gather results from all tasks
    token_data, token_metadata, holders, holders_data = await asyncio.gather(token_data_future, token_metadata_future,
                                                                             holders_future, data_folders_future)

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


async def get_smart_wallets_data(token_mint, pool=None):
    '''
        GET SMART WALLETS (ALL WALLETS -> CHECK GRADES -> KEEP VALID ONES)

        BUYS -> GET COUNT TXS (WHERE WALLETS IN SMART_WALLETS AND IN_MINT IN (SOL, USDT, USDC, WSOL) AND OUT_MINT = token_mint
        BUY VOLUME -> SUM(IN_MINT) FOR BUYS

        SELLS -> GET COUNT TXS (WHERE WALLETS IN SMART_WALLETS AND OUT_MINT IN (SOL, USDT, USDC, WSOL) AND IN_MINT = token_mint
        BUY VOLUME -> SUM(OUT_MINT) FOR SELLS

        TOTAL VOLUME -> SELL VOLUME + BUY VOLUME
        NETFLOWS -> SELL VOLUME - BUY VOLUME

        RETURN BUYS, SELLS, TOTAL_VOLUME, NETFLOWS
    '''
    return



