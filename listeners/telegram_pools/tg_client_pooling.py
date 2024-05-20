import asyncio
from telethon import TelegramClient
from asyncio import Lock

tg_id = 21348081
tg_hash = '841396171d9b111fa191dcdce768d223'


class TelegramClientPool:
    def __init__(self, api_id, api_hash, session_prefix='session', client_count=10):
        self.__clients = []
        self.__lock = Lock()
        self.__in_use = set()

        for i in range(1, client_count + 1):
            client = TelegramClient(f'{session_prefix}{i}', api_id, api_hash)
            self.__clients.append(client)

    async def acquire(self):
        async with self.__lock:
            for client in self.__clients:
                if client not in self.__in_use:
                    self.__in_use.add(client)
                    await client.start()
                    return client
            return None  # All clients are in use

    async def release(self, client):
        async with self.__lock:
            if client in self.__in_use:
                self.__in_use.remove(client)
                await client.disconnect()

    async def start_all(self):
        await self._run_on_all_clients('connect')

    async def stop_all(self):
        await self._run_on_all_clients('disconnect')

    async def _run_on_all_clients(self, method_name):
        tasks = [getattr(client, method_name)() for client in self.__clients]
        await asyncio.gather(*tasks)


async def main():
    tg_pool = TelegramClientPool(api_hash=tg_hash, api_id=tg_id)

    # USAGE 1
    async with await tg_pool.acquire() as client:
        await client.send_message('me', 'ZZZZZZZZZZZZ, myself!')

    # USAGE 2
    client = await tg_pool.acquire()
    await client.send_message('me', 'ZZZZZZZZZZZZ, myself!')
    await tg_pool.release(client)


asyncio.run(main())
