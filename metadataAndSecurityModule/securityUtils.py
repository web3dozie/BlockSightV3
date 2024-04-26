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


# LP PERCENTAGE BURN AND LOCK
async def get_initial_lp_supply(token_mint: str, pool=None):
    data = await get_metadata(token_mint, pool=pool)
    return data['initial_lp_supply']


async def get_lp_burnt(token_mint: str, pool=None):
    lp_address = await get_metadata(token_mint, pool=pool)
    lp_address = lp_address['lp_address']

    data = await get_data_from_helius(lp_address)
    decimals = data['token_info']['decimals']

    return round(((data['token_info']['supply']) / (10 ** decimals)), 2)


async def get_just_supply(token_mint: str, pool=None):
    data = await get_metadata(token_mint, pool=pool)
    return data['supply']


async def get_top_holder_percentages(token_mint: str):
    """
    Asynchronously fetches the 20 largest accounts for a specified SPL Token mint.

    Args:
    - mint_address (str): The base-58 encoded public key of the SPL Token Mint.
    - cluster_url (str, optional): URL of the Solana cluster to query. Defaults to mainnet-beta.

    Returns:
    - list: A list of dictionaries containing the 20 largest accounts for the specified SPL Token.
    """

    supply = await get_just_supply(token_mint)

    client = Client(rpc_url)

    # Convert the mint address string to a Pubkey object
    mint_pubkey = Pubkey.from_string(token_mint)

    # Fetch the 20 largest accounts
    try:
        data = ast.literal_eval(client.get_token_largest_accounts(mint_pubkey).to_json())['result']['value']
    except Exception as e:
        await asyncio.sleep(3)
        data = ast.literal_eval(client.get_token_largest_accounts(mint_pubkey).to_json())['result']['value']
        print(e)

    data = [{k: v for k, v in d.items() if k == 'uiAmount'} for d in data]
    data = [d['uiAmount'] for d in data]
    try:
        top_10 = round((sum(data[1:10]) / supply * 100), 2)
        top_20 = round((sum(data[1:]) / supply * 100), 2)
    except TypeError:
        top_10 = 100.0
        top_20 = 100.0
    return {
        'top_10': top_10,
        'top_20': top_20
    }


async def get_mintable_mutable_supply(token_mint: str):
    data = await get_data_from_helius(token_mint)

    mintable = data.get('token_info')
    mintable = mintable.get('mint_authority')
    if mintable:
        mintable = True
    else:
        mintable = False

    return {
        'is_mutable': data.get('mutable'),
        'is_mintable': mintable
    }


async def get_security(token_mint):
    return


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    # session = aiohttp.ClientSession()

    start = float(time.time())
    await get_metadata('8GYGViQWHX7ZYunCUiyXKo5dp3KobcN2gqKoHDyqDYW', pool=pool)  # , session=session)
    end = float(time.time())
    # await session.close()

    print(f'Time taken: {end - start}')


asyncio.run(main())
