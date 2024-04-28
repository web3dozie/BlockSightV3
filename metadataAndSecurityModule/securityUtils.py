import json
import random, re, time, aiohttp, asyncio, ast

from pprint import pprint

import asyncpg
from solana.exceptions import SolanaRpcException
from solana.rpc.api import Client
from solana.rpc.commitment import Commitment
from solana.rpc.core import RPCException
from solders.pubkey import Pubkey
from datetime import datetime, timedelta
from dbs.db_operations import mint_exists, add_metadata_to_db, get_metadata_from_db, pg_db_url
from metadataAndSecurityModule.metadataUtils import retrieve_metadata, get_data_from_helius, get_metadata

helius_api_key = 'cfc89cfc-2749-487b-9a76-58b989e70909'
rpc_url = f'https://mainnet.helius-rpc.com/?api-key={helius_api_key}'


async def get_security(token_mint, pool=None):
    # ------ Initialize token data and metadata ----- #
    token_data = await get_data_from_helius(token_mint)
    token_decimals = token_data['token_info']['decimals']
    token_supply = round(((token_data['token_info']['supply']) / (10 ** token_decimals)), 2)
    token_metadata = await get_metadata(token_mint, pool=pool)
    pprint(token_metadata)
    # ------                         ----- #

    # ------ Get lp address and data ----- #
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
    # ------                      ----- #

    # ------ Get top holders data ----- #
    client = Client(rpc_url)
    mint_pubkey = Pubkey.from_string(token_mint)
    holders_data = [d['uiAmount'] for d in ast.literal_eval(client.get_token_largest_accounts(mint_pubkey)
                                                            .to_json())['result']['value']]
    safe_round = lambda x: round(x, 2) if isinstance(x, float) else 100.0
    # ------                ----- #

    # ------ Data to return ----- #
    is_mintable = bool(token_data.get('token_info').get('mint_authority'))
    is_mutable = token_data.get('mutable')
    lp_burnt_percentage = None if lp_current_supply == 1 and lp_initial_supply == 2 else int(
        100 - (lp_current_supply / lp_initial_supply * 100))
    top_10 = safe_round((sum(holders_data[1:10]) / token_supply * 100) if token_supply else 0)
    top_20 = safe_round((sum(holders_data[1:21]) / token_supply * 100) if token_supply else 0)

    return {
        'is_mintable': is_mintable,
        'is_mutable': is_mutable,
        'lp_burnt_percentage': lp_burnt_percentage,
        'top_10': top_10,
        'top_20': top_20
    }


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    # session = aiohttp.ClientSession()

    start = float(time.time())
    print(await get_security('GpQmR58NthSXCj68xjGp9qxz6uhRRzA8rekN7NNNLs3g', pool=pool))  # , session=session)
    end = float(time.time())
    # await session.close()

    print(f'Time taken: {end - start}')


asyncio.run(main())
