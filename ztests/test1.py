import asyncio

from metadataAndSecurityModule.metadataUtils import retrieve_metadata
from pprint import pprint


async def main():
    data = await retrieve_metadata('2KyZ3RsuFLpZyExSsUgCAZrjZxPFq7Xhp2ES2fqGkVST')

    pprint(data)


asyncio.run(main())
