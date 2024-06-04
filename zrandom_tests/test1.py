from pprint import pprint

from asyncio import run

from walletVettingModule.wallet_vetting_utils import fetch_tg_leaderboard


async def main():
    pprint(await fetch_tg_leaderboard(pool=None))

run(main())