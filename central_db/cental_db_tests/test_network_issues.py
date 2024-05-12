import socket

import aiohttp
import asyncio

from aiohttp import TCPConnector

from metadataAndSecurityModule.metadataUtils import helius_api_key


async def get_data_from_helius(token_mint, api_key, session=None):
    url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
    headers = {'Content-Type': 'application/json'}
    payload = {
        "jsonrpc": "2.0",
        "id": "web3dozie",
        "method": "getAsset",
        "params": {
            "id": token_mint,
            "displayOptions": {"showFungible": True}
        },
    }
    max_attempts = 5

    is_new_session = False
    if not session:
        session = aiohttp.ClientSession(connector=TCPConnector(family=socket.AF_INET))
        is_new_session = True

    try:
        for attempt in range(max_attempts):
            try:
                print(f"Sending request with headers: {headers} and payload: {payload}")  # Print headers and payload
                async with session.post(url, headers=headers, json=payload) as response:
                    print(f"Received response with headers: {response.headers}")  # Print response headers
                    if response.status == 200:
                        result = await response.json()
                        return result.get('result')
                    else:
                        print(f"Failed to fetch metadata for {token_mint} (helius). Status code: {response.status}")
                        continue
            except aiohttp.ClientError as e:
                print(f"Network error occurred: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    print("Maximum retry attempts reached")
                    raise
    finally:
        if is_new_session:
            await session.close()


# Example usage
async def main():
    token_mint = 'HQ7DaoiUxzC2K1Dr7KXRHccNtXvEYgNvoUextXe8dmBh'
    api_key = helius_api_key
    result = await get_data_from_helius(token_mint, api_key)
    print("API Result:", result)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
