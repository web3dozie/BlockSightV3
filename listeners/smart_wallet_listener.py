import asyncio, asyncpg
import socket

import aiohttp
from aiohttp import ClientTimeout, TCPConnector

from dbs.db_operations import update_txs_db, useful_wallets
from metadataAndSecurityModule.metadataUtils import get_wallet_txs, get_metadata
from walletVettingModule.wallet_vetting_utils import parse_for_swaps


class SmartWalletListener:
    def __init__(self, pool: asyncpg.Pool, wait_time=200):
        self.wait_time = wait_time
        self.tasks_ready = asyncio.Queue()
        self.currently_running = set()
        self.pool = pool
        self.simultaneous = 10
        self.timeout = ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(connector=TCPConnector(family=socket.AF_INET), timeout=self.timeout)

    async def add_wallet_task(self, wallet):
        # Pass self when adding a task
        await self.tasks_ready.put((wallet, lambda w: wallet_task(w, self)))

    async def start_scheduler(self):
        while True:
            if len(self.currently_running) < self.simultaneous:
                await self.process_ready_tasks()
            await asyncio.sleep(1)  # Small delay to prevent a tight loop

    async def process_ready_tasks(self):
        while not self.tasks_ready.empty() and len(self.currently_running) < self.simultaneous:
            wallet, task_func = await self.tasks_ready.get()
            task = asyncio.create_task(self.run_wallet_task(wallet, task_func))
            self.currently_running.add(task)
            task.add_done_callback(self.task_done)

    @staticmethod
    async def run_wallet_task(wallet, task_func):
        # Run the wallet task and return its result
        return await task_func(wallet)

    def task_done(self, task):
        self.currently_running.remove(task)
        result = task.result()
        asyncio.create_task(self.reschedule_task(result))

    async def reschedule_task(self, task_result):
        # Wait for the specified wait time before rescheduling the task
        await asyncio.sleep(self.wait_time)
        await self.tasks_ready.put(task_result)


# Define a task for each wallet
async def wallet_task(wallet: str, listener):
    txs = await parse_for_swaps(await get_wallet_txs(wallet, start_days_ago=0, session=listener.session))

    async def handle_semaphore(task, sem):
        async with sem:
            return await task

    semaphore = asyncio.Semaphore(10)
    mints = {mint for tx in txs for mint in (tx['in_mint'], tx['out_mint'])}

    tasks = [update_txs_db(txs, pool=listener.pool)] + [get_metadata(mint, pool=listener.pool, regular_use=False, session=listener.session) for mint in mints]

    limited_tasks = [asyncio.create_task(handle_semaphore(task, semaphore)) for task in tasks]
    await asyncio.gather(*limited_tasks)

    return wallet, wallet_task


async def maintain_txs(pool: asyncpg.Pool):
    """
    This function creates a scheduler and makes sure each
    wallet's txs gets fetched exactly once every n minutes.

    It is designed to run forever
    """
    wallet_list = await useful_wallets(pool=pool)
    listener = SmartWalletListener(pool)
    for wallet in wallet_list:
        await listener.add_wallet_task(wallet)
    await listener.start_scheduler()
