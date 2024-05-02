import asyncio
import time
from pprint import pprint

import asyncpg

from dbs.db_operations import pg_db_url, get_tx_list
from metadataAndSecurityModule.metadataUtils import get_data_from_helius, retrieve_metadata, get_metadata
from walletVettingModule.wallet_vetting_utils import fetch_wallet_leaderboard


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)

    start = float(time.time())
    x = await fetch_wallet_leaderboard(pool, window='30d')
    end = float(time.time())

    pprint(x)
    print(end - start)

asyncio.run(main())
