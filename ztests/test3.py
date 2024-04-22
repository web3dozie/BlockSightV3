import asyncio

from metadataAndSecurityModule.metadataUtils import get_data_from_helius


async def main():
    x = await get_data_from_helius('7h14cQZhER1oNSfjCE4xw1ZQjGd2efeFMufnK4GURAUY')
    print(x)

asyncio.run(main())
