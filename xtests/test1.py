import asyncio
import time
from pprint import pprint

import asyncpg

from dbs.db_operations import pg_db_url
from walletVettingModule.wallet_vetting_utils import process_wallet

wallets = ['2AnSRuVoRyPBUE1FvgEqdiYi55p42u7LLBB91eGVAviT',
           '2e6xkkPWPvncyuUQ97Y1XGVV9psghRD373TtghBZEZ1R',
           '8EgH6xn9rAVSMtTUV915mrCuriFixLQ7ZRCtxpY4kj4D',
           '4ERXcciCg8V89DdnJjN8rAZcZRiSWEfzrHK8iDcYbaCx',
           '7sRF3XHhDGiuuwAr8hzkF2Q4BEkRi7J4Ei6rfUWBig1v',
           'Gw8QfcFFE1Mvo5VBBPWAhRcfUB75bz37b4EaNZECKCsu',
           '872GmVq2iSBNd7g4N31XZ8S7mVnpPxDHJL3UbwaGnrnp',
           '4ZCo5NLb1KxRymHaWh341WidGF3nYqBGanhHoq4HUGaf',
           '5xL655MuivWhrbKe341jbbQv861ye7icAtjdu3uNi7x3',
           'DoTifJ1QePrZjtWwCXnsyyYzJCUJbdY7CSefzMyGFSAd',
           'HM1NYR4zdnyMhypYXbGrMcc3C5VBkXcNGK7ANXDfDFMy',
           '28RpvwXqRtKKYCWmtpPZfepYFWXH9MPwDYy3HHTprLuS']


async def main():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    wallet = wallets[7]

    start = float(time.time())
    x = await process_wallet(wallet, pool=pool)
    end = float(time.time())

    pprint(x)
    print(f'This wallet took: {end - start:.2f} secs to get processed')


asyncio.run(main())
