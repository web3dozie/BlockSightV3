"""
 This module provides functionality to process wallet data.
 It interfaces with pg db tables such as wallets, metadata, and token_prices
 to retrieve and update wallet information.
"""
import time
from pprint import pprint

import asyncpg

from dbs.db_operations import pg_db_url
from process_wallets_utils import wallet_processor, read_csv_wallets, remove_wallet_from_csv
from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio


async def main():
    # Process wallet data from zipped files in the specified directory
    # wallet_processor('./wallet_zips')

    wallet_list = read_csv_wallets('./wallet_counts.csv')
    semaphore = asyncio.Semaphore(16)

    # Try to initialize the pool
    try:
        pool = await asyncpg.create_pool(
            dsn=pg_db_url,
            min_size=50,
            max_size=150,
            max_inactive_connection_lifetime=1000,
            timeout=100,
            statement_cache_size=1000
        )
    except Exception as e:
        print(f"Failed to create pool: {e}")
        return

    async def process_wallet_with_semaphore(wallet):
        async with semaphore:
            start = time.time()
            try:
                retv = await process_wallet(wallet, pool=pool)
            except Exception as e:
                print(f"Error processing wallet {wallet}: {e}")
                return
            end = time.time()
            pprint(f"Processed {wallet} in {end-start:.2f} seconds")
            if retv is not None:
                remove_wallet_from_csv('./wallet_counts.csv', wallet)

    wallet_list = list(reversed(wallet_list))
    tasks = [process_wallet_with_semaphore(wallet) for wallet in wallet_list]

    try:
        await asyncio.gather(*tasks)
    finally:
        await pool.close()

if __name__ == '__main__':
    asyncio.run(main())
