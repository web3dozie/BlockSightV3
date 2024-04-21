import asyncio

from telethon import TelegramClient
from vet_tg_channel import vetChannel, api_id, api_hash

telegram_channels = [
    "Archerrgambles",
    "SafePlayOnly_4AM",
    "degensgems",
    "sugarydick",
    "WhoDis6964",
    "EZMoneyCalls",
    "GabbensCalls",
    "HellsingGamble",
    "PapasCall",
    "SapphireCalls",
    "ralverogems",
    "AlphaArbitrageCalls",
    "CryptoFrogsGems",
    "bagcalls",
    "maythouscalls"
]


async def vetChannelLimited(semaphore, channel, tg_client):
    try:
        async with semaphore:
            return await vetChannel(channel=channel, tg_client=tg_client)
    except Exception as e:
        print(f"Exception {e} while vetting channel  {channel}")


async def main_func():
    semaphore = asyncio.Semaphore(5)  # Limits the number of concurrent tasks to
    client = TelegramClient('anon', api_id, api_hash)

    tasks = [vetChannelLimited(semaphore, channel, client) for channel in telegram_channels]
    results = await asyncio.gather(*tasks)

    await client.disconnect()

    return results

if __name__ == "__main__":
    asyncio.run(main_func())
