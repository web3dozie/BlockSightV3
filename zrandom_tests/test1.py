import math
from pprint import pprint

from asyncio import run

from walletVettingModule.wallet_vetting_utils import fetch_tg_leaderboard


async def tg_leaderboard(window='30d'):
    # page = request.args.get("page", default=1, type=int)
    total_pages = 15

    page = 1

    ld_data = await fetch_tg_leaderboard(window=window, pool=None, sort_by='pnl', direction='desc')

    rows_per_page = math.floor(len(ld_data) / total_pages)

    page_data = ld_data[rows_per_page * (page - 1): rows_per_page * page]

    if page > 1:
        prev = f"/get-tg-leaderboard/{window}?page={page - 1}"
    else:
        prev = "None"

    if page < total_pages:
        next_page = f"/get-tg-leaderboard/{window}?page={page + 1}"
    else:
        next_page = "None"

    return {"page-data": page_data, "prev": prev, "next": next_page}


async def main():
    x = await tg_leaderboard()
    x = x['page-data']
    pprint(x)
    print(len(x))

run(main())


