import asyncio
from pprint import pprint

from metadataAndSecurityModule.metadataUtils import get_data_from_helius

mint = 'x8837dfa596a8ee6381a6c9b3dc991891fbf4ced6'


async def main():
    pprint(await get_data_from_helius(mint))


asyncio.run(main())
