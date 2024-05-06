import time, asyncio, ast, asyncpg

from pprint import pprint

import aiohttp
from solana.rpc.api import Client
from solders.pubkey import Pubkey

from central_db.snapshot_utils import get_metadata_security_for_snapshot
from dbs.db_operations import pg_db_url
from metadataAndSecurityModule.metadataUtils import get_data_from_helius, get_metadata, get_num_holders

helius_api_key = 'cfc89cfc-2749-487b-9a76-58b989e70909'
rpc_url = f'https://mainnet.helius-rpc.com/?api-key={helius_api_key}'


async def get_security(token_mint, pool=None, session=None):
    # ------ Initialize data ----- #
    client = Client(rpc_url)
    mint_pubkey = Pubkey.from_string(token_mint)

    async def data_for_holders():
        return [d['uiAmount'] for d in ast.literal_eval(client.get_token_largest_accounts(mint_pubkey)
                                                                .to_json())['result']['value']]

    # Run all three I/O bound tasks concurrently
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
    is_mutable = token_data.get('mutable')
    lp_burnt_percentage = None if lp_current_supply == 1 and lp_initial_supply == 2 else int(
        100 - (lp_current_supply / lp_initial_supply * 100))
    top_10 = safe_round((sum(holders_data[1:10]) / token_supply * 100) if token_supply else 0)
    top_20 = safe_round((sum(holders_data[1:21]) / token_supply * 100) if token_supply else 0)

    sec_data = {
        'is_mintable': is_mintable,
        'is_mutable': is_mutable,
        'lp_burnt_percentage': lp_burnt_percentage,
        'num_holders': holders,
        'top_10': top_10,
        'top_20': top_20
    }

    return sec_data


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    session = aiohttp.ClientSession()

    start = float(time.time())
    pprint(await get_metadata_security_for_snapshot('EkM3AvBo8hFmuCiHqsX4s14Ue8NsUhQnnLVs1vATHRxC', pool=pool, session=session))
    end = float(time.time())

    print(f'Overall Time taken: {end - start}')
    await session.close()


asyncio.run(main())
