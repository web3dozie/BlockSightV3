"""
 This module provides functionality to process wallet data.
 It interfaces with pg db tables such as wallets, metadata, and token_prices
 to retrieve and update wallet information.
"""

from process_wallets_utils import wallet_processor, read_csv_wallets, remove_wallet_from_csv
from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio


async def main():
    # Process wallet data from zipped files in the specified directory
    # wallet_processor('./wallet_zips')

    wallet_list = read_csv_wallets('./wallet_counts.csv')

    # Create a semaphore to limit concurrent tasks to 10
    semaphore = asyncio.Semaphore(10)

    # Define an asynchronous function to process a wallet with semaphore control
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
