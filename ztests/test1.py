import asyncio

from metadataAndSecurityModule.metadataUtils import retrieve_metadata, get_data_from_helius
from pprint import pprint


async def main():
    data = await get_data_from_helius('9TgHi9gnHAtqxbuMpAtBGXKmGrH4v673JzqyT8ey227t')

    pprint(data)


asyncio.run(main())
