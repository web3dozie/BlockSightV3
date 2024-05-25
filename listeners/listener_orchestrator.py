import asyncio

from listeners.smart_tg_listener import tg_listener
from listeners.smart_wallet_listener import maintain_txs


async def main():

    wallet_listener_task = asyncio.create_task(maintain_txs())
    tg_listener_task = asyncio.create_task(tg_listener())

    await asyncio.gather(wallet_listener_task, tg_listener_task)


asyncio.run(main())


