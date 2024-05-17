import asyncio

from dbs.db_operations import useful_wallets


async def main():
    wallet_list = await useful_wallets()

    print(wallet_list)
    print(len(wallet_list))

asyncio.run(main())


