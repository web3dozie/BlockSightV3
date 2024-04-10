"""
 This module allows me to easily process wallets.
 It should work with wallets.db, tokens.db and prices.db
"""

from process_wallets_utils import wallet_processor, read_csv_wallets, remove_wallet_from_csv
from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio


async def main():
    # wallet_processor('./wallet_zips')
    wallet_list = read_csv_wallets('./wallet_counts.csv')

    # Create a semaphore to limit concurrent tasks to 10
    semaphore = asyncio.Semaphore(10)

    async def process_wallet_with_semaphore(wallet):
        async with semaphore:
            retv = await process_wallet(wallet)
            print("processed wallet", retv)
            if retv is not None:
                remove_wallet_from_csv('./wallet_counts.csv', wallet)


    # Create tasks list
    tasks = [process_wallet_with_semaphore(wallet) for wallet in wallet_list]

    # Wait for all tasks to complete
    await asyncio.gather(*tasks)


# Ensure asyncio.run is only called when running the script directly
if __name__ == '__main__':
    asyncio.run(main())
