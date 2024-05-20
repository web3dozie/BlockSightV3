import asyncio
from telethon import TelegramClient
from asyncio import Lock

class TelegramClientPool:
    def __init__(self, api_id, api_hash, session_prefix='session', client_count=30):
        self.clients = []
        self.lock = Lock()
        self.in_use = set()

        for i in range(1, client_count + 1):
            client = TelegramClient(f'{session_prefix}{i}', api_id, api_hash)
            self.clients.append(client)

    async def acquire(self):
        async with self.lock:
            for client in self.clients:
                if client not in self.in_use:
                    self.in_use.add(client)
                    await client.start()
                    return client
            return None  # All clients are in use

    async def release(self, client):
        async with self.lock:
            if client in self.in_use:
                self.in_use.remove(client)
                await client.disconnect()

    async def start_all(self):
        await self._run_on_all_clients('connect')

    async def stop_all(self):
        await self._run_on_all_clients('disconnect')

    async def _run_on_all_clients(self, method_name):
        tasks = [getattr(client, method_name)() for client in self.clients]
        await asyncio.gather(*tasks)
