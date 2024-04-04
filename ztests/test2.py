import time

from priceDataModule.price_utils import update_price_data
from walletVettingModule.wallet_vetting_utils import get_wallet_txs
import asyncio
from pprint import pprint


async def main():
    txs = await update_price_data('2j7htQxfcwH5U3cyLWtsrLw9pKtckG7tDuhmerdQpcVs', 1711529459, int(time.time()))
    pprint(txs)


asyncio.run(main())
