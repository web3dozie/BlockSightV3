import asyncio
import time
from pprint import pprint

import asyncpg

from central_db.snapshot_utils import (get_smart_wallets_data_wrapper, get_full_dxs_data,
                                       get_metadata_security_for_snapshot)
from dbs.db_operations import pg_db_url, get_tx_list
from metadataAndSecurityModule.metadataUtils import get_data_from_helius, retrieve_metadata, get_metadata, \
    get_num_holders
from walletVettingModule.wallet_vetting_utils import fetch_wallet_leaderboard


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    mint = 'HdhqjYX8neVicp5EWVhF6vWLMvkTqPLTCjX7vbNKaCau'
    start = float(time.time())
    x = await get_smart_wallets_data_wrapper(mint, pool=pool)  # , pool=pool)
    # x = await get_full_dxs_data(mint)
    # x = await get_metadata_security_for_snapshot(mint, pool=pool)
    end = float(time.time())

    pprint(x)

    print(end - start)


asyncio.run(main())

y = {
    'airdropped': 0.0,
    'bundled': 0.39,
    'buys_1h': 0,
    'buys_5m': 0,
    'buys_6h': 0,
    'fdv': 3317,
    'liquidity': 6094.36,
    'lp_age': 1514024,
    'lp_safe': True,
    'mint_safe': True,
    'num_holders': 189,
    'price': 3.406e-06,
    'price_change_1h': 0,
    'price_change_5m': 0,
    'price_change_6h': 0,
    'sells_1h': 0,
    'sells_5m': 0,
    'sells_6h': 0,
    'smart_buys_1h': 0,
    'smart_buys_5m': 0,
    'smart_buys_6h': 0,
    'smart_netflows_1h': 0,
    'smart_netflows_5m': 0,
    'smart_netflows_6h': 0,
    'smart_tg_calls_1h': 0,
    'smart_tg_calls_5m': 0,
    'smart_tg_calls_6h': 0,
    'smart_sells_1h': 0,
    'smart_sells_5m': 0,
    'smart_sells_6h': 0,
    'smart_volume_1h': 0,
    'smart_volume_5m': 0,
    'smart_volume_6h': 0,
    'socials': False,
    'starting_liq': 24700.24,
    'starting_mc': 58131.79,
    'top_10': 6.84,
    'top_20': 7.48,
    'volume_1h': 0,
    'volume_5m': 0,
    'volume_6h': 0,
}

print(len(y.keys()))
