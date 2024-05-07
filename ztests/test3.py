import asyncio
import time
from pprint import pprint

import asyncpg

from central_db.snapshot_utils import (get_smart_wallets_data_wrapper, get_full_dxs_data,
                                       get_metadata_security_for_snapshot, take_snapshot)
from dbs.db_operations import pg_db_url, get_tx_list
from metadataAndSecurityModule.metadataUtils import get_data_from_helius, retrieve_metadata, get_metadata, \
    get_num_holders
from walletVettingModule.wallet_vetting_utils import fetch_wallet_leaderboard


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    mint = 'HdhqjYX8neVicp5EWVhF6vWLMvkTqPLTCjX7vbNKaCau'
    start = float(time.time())
    x = await take_snapshot(mint, pool=pool)  # , pool=pool)
    # x = await get_full_dxs_data(mint)
    # x = await get_metadata_security_for_snapshot(mint, pool=pool)
    end = float(time.time())

    pprint(x)

    print(f'This took {end-start:.2f} seconds to run.')


asyncio.run(main())

