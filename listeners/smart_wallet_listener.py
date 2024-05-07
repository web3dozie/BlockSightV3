import asyncio, asyncpg

from dbs.db_operations import update_txs_db, useful_wallets
from metadataAndSecurityModule.metadataUtils import get_wallet_txs, get_metadata
from walletVettingModule.wallet_vetting_utils import parse_for_swaps


class SmartWalletListener:
    def __init__(self, pool: asyncpg.Pool, wait_time=200):
        self.wait_time = wait_time
        self.tasks_ready = asyncio.Queue()
        self.currently_running = set()
        self.pool = pool

    async def add_wallet_task(self, wallet, task_func):
        # Initially, all tasks are considered ready.
        await self.tasks_ready.put((wallet, task_func))

    async def start_scheduler(self):
        while True:
            if len(self.currently_running) < 40:  # Max 40 concurrent tasks
                await self.process_ready_tasks()
            await asyncio.sleep(1)  # Small delay to prevent a tight loop

    async def process_ready_tasks(self):
        while not self.tasks_ready.empty() and len(self.currently_running) < 40:
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
async def wallet_task(wallet: str, pool=None):
    txs = await parse_for_swaps(await get_wallet_txs(wallet, start_days_ago=0))

    # Use set comprehension to gather all unique mints in a more concise and Pythonic way
    mints = {mint for tx in txs for mint in (tx['in_mint'], tx['out_mint'])}

    # Use asyncio.gather to fetch metadata for all mints concurrently, improving efficiency
    # This updates the DB with fresh tokens
    await asyncio.gather(*(get_metadata(mint, pool=pool) for mint in mints), update_txs_db(txs, pool=pool))

    # Return the wallet and task function for rescheduling
    return wallet, wallet_task


async def maintain_txs(pool: asyncpg.Pool):
    """
    This function creates a scheduler and makes sure each
    wallet's txs gets fetched exactly once every n minutes.

    It is designed to run forever

    :param:
    :return: no return values
    """
    wallet_list = await useful_wallets(pool=pool)

    listener = SmartWalletListener(pool)

    for wallet in wallet_list:
        await listener.add_wallet_task(wallet, wallet_task)

    await listener.start_scheduler()
