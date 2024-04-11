import json, time, re, aiohttp, asyncio, sys
from pprint import pprint

from solana.rpc.api import Client
from solana.rpc.commitment import Commitment
from solana.rpc.core import RPCException

from metadataAndSecurityModule.metadataUtils import get_wallet_txs
from walletVettingModule.wallet_vetting_utils import get_sol_price

helius_api_key = 'cfc89cfc-2749-487b-9a76-58b989e70909'
rpc_url = 'https://multi-still-haze.solana-mainnet.quiknode.pro/31a3baf7ec201b729d156f47b25ca0cd7390c256/'
# api_key = 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

async def get_dxs_data(mint_token):
    dxs_api_url = f'https://api.dexscreener.com/latest/dex/tokens/{mint_token}'
    max_retries = 1  # Number of retries
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(dxs_api_url) as response:
                    if response.status == 200:
                        token_data = await response.json()  # This is a list of pools the token has open
                        lp_time = int(token_data['pairs'][0]['pairCreatedAt'] / 1000)

                        return {
                            'lp_creation_time': lp_time,
                            'pool_address': token_data['pairs'][0]['url'].split('/')[-1]
                        }

                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")

        except Exception as e:
            retries += 1
            print(f"Error: {e}, DXS retrying in 1 seconds... | Mint: {mint_token}")
            await asyncio.sleep(1)
        else:
            # If successful, exit the loop
            break

    if retries >= max_retries:
        return {
            'lp_creation_time': None,
            'pool_address': None
        }


async def get_current_slot_timestamp():
    # Returns the current slot's number and timestamp

    cluster_url = rpc_url
    client = Client(cluster_url)
    try:
        slot_number = client.get_block_height(commitment=Commitment('confirmed')).value
    except:
        await asyncio.sleep(0.1)
        slot_number = client.get_block_height().value

    slot_timestamp = int(time.time())
    slot_number += 19632867

    return slot_number, slot_timestamp


async def get_target_slot_timestamp(slot_number):
    cluster_url = rpc_url
    client = Client(cluster_url)
    initial_range = [0]  # Starting with the current slot
    extended_ranges = [[1, -1], [2, -2], [3, -3]]  # Expanding the range progressively

    for delta_range in [initial_range] + extended_ranges:
        for delta in delta_range:
            try:
                slot_timestamp = client.get_block_time(slot_number + delta).value
                return slot_timestamp  # Return on the first successful fetch
            except RPCException:
                continue  # Try the next delta in the range

    # If the function hasn't returned by this point, no valid timestamp was found
    raise ValueError(f"No valid timestamp found near slot number {slot_number}")


async def deep_deploy_tx_search(target_timestamp):
    # Start by getting the current slot number and timestamp
    current_slot_number, current_timestamp = await get_current_slot_timestamp()

    # If the current timestamp is less than the target, the slot doesn't exist yet
    if current_timestamp < target_timestamp:
        print("Target timestamp is in the future.")
        return None

    low = 100000000
    high = current_slot_number
    last_valid_mid = None  # Keep track of the last valid slot found

    while low <= high:
        mid = (low + high) // 2
        mid_timestamp = await get_target_slot_timestamp(mid)

        if mid_timestamp < target_timestamp:
            # This could be a valid slot, save it and try to find a closer one
            last_valid_mid = mid

            low = mid + 1
        else:
            high = mid - 1

    if last_valid_mid is not None:
        return last_valid_mid
    else:
        # If no valid slot was found but the loop completed, it means all slots are in the future
        print("No slots before target timestamp.")
        return None


def extract_links(text):
    # Regex pattern to find URLs
    url_pattern = r'https?://[^\s,\'"]+'
    urls = re.findall(url_pattern, text)

    # Categorizing URLs
    telegram_links = [url for url in urls if 't.me' in url]
    twitter_links = list(set([url for url in urls if 'x.com' in url or 'twitter.com' in url]))
    website_links = [url for url in urls if url not in telegram_links and url not in twitter_links]
    website_links = [url for url in website_links if len(url) < 50]

    return {
        'twitter': twitter_links,
        'telegram': telegram_links,
        'others': website_links
    }


async def parse_tx_list(tx_list, api_key=helius_api_key):
    url = f"https://api.helius.xyz/v0/transactions/?api-key={api_key}"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json={"transactions": tx_list}) as response:
            data = await response.json()
            return data


async def get_socials(json_url):
    if not json_url:
        return {
            'twitter': None,
            'telegram': None,
            'others': None
        }
    async with aiohttp.ClientSession() as session:
        async with session.get(json_url) as response:
            json_data = await response.json()
            json_data = json_data['extensions']

            return extract_links(str(json_data))


async def retrieve_metadata(token_mint: str, api_key=helius_api_key):
    url = f"https://mainnet.helius-rpc.com/?api-key={api_key}"
    headers = {
        'Content-Type': 'application/json',
    }
    payload = {
        "jsonrpc": "2.0",
        "id": "web3dozie",
        "method": "getAsset",
        "params": {
            "id": token_mint,
            "displayOptions": {
                "showFungible": True
            }
        },
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                result = await response.json()
                result = result.get('result')

            else:
                print(f"Failed to fetch metadata for {token_mint} (helius). Status code:", response.status)
                result = {}

    mint = token_mint
    symbol = result['content']['metadata']['symbol']
    name = result['content']['metadata']['name']

    # check two places and fall back on a default img_url
    img_url = result['content']['links']['image'] \
        if 'image' in result['content']['links'] and result['content']['links']['image'] else (
        result['content']['files']['uri'] if 'uri' in result['content']['files']
        else 'https://cdn-icons-png.flaticon.com/512/2748/2748558.png')

    # for socials check description for 3 or more links (if it isn't there pass to other function)
    description = result['content']['metadata']['description']
    socials = extract_links(description)

    # if any key in the dict is empty --> use the json metadata
    if any(value is None for value in socials.values()):
        socials = get_socials(result['content']['json_uri'])

    twitter, telegram, other_links = socials['twitter'], socials['telegram'], socials['others']

    decimals = result['token_info']['decimals']
    supply = round(((result['token_info']['supply']) / (10 ** decimals)), 2)

    deployer = result['authorities'][0]['address']

    max_retries = 2
    attempts = 0
    deploy_sig = None

    while attempts < max_retries:
        try:
            if deployer in ['TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM']:
                raise Exception(f'Pump Fun Token: Going Deep')

            deploy_tx = await get_wallet_txs(deployer, start_days_ago=1000, tx_type="CREATE_POOL")

            relevant_deploy = []

            for tx in deploy_tx:
                if mint in str(tx):
                    relevant_deploy.append(tx)

            deploy_tx = relevant_deploy

            if deploy_tx:
                deploy_sig = deploy_tx[0]['signature']

            if not deploy_tx:
                raise Exception(f'No deploy tx found')

            break

        except Exception as e:  # Catch the specific exception if possible, instead of using a bare except
            attempts += 1
            if attempts < max_retries:
                await asyncio.sleep(1)  # Wait for 1 second before retrying
            else:
                print(f'\n\nStarting Deep Search for {mint}')

                # After max retries, handle with the except logic

                dxs_data = await get_dxs_data(mint)
                lp_creation_time = dxs_data['lp_creation_time']
                lp_address = dxs_data['pool_address']

                deploy_slot = await deep_deploy_tx_search(lp_creation_time - 1)

                slot_txs_plus_10 = []
                for tries in range(0, 10):
                    cluster_url = rpc_url
                    client = Client(cluster_url)

                    trying_with = deploy_slot + tries

                    block_txs = client.get_block(trying_with, max_supported_transaction_version=0).to_json()
                    block_txs = json.loads(block_txs)['result']['transactions']
                    slot_txs_plus_10.extend(block_txs)

                deploy_sigs = []
                for tx in slot_txs_plus_10:
                    if lp_address in str(tx).lower():
                        deploy_sigs.append(tx['transaction']['signatures'][0])

                async def parse_large_tx_list(large_list_of_sigs, chunk_size=100):
                    # Initialize an empty list to hold all parsed transactions
                    all_parsed_txs = []

                    # Calculate the number of chunks needed
                    num_chunks = len(large_list_of_sigs) // chunk_size + (
                        1 if len(large_list_of_sigs) % chunk_size > 0 else 0)

                    for i in range(num_chunks):
                        # Calculate start and end indices for each chunk
                        start_index = i * chunk_size
                        end_index = start_index + chunk_size

                        # Slice the deploy_sigs list to get a chunk of at most 100 elements
                        chunk = large_list_of_sigs[start_index:end_index]

                        # Parse the current chunk
                        parsed_txs = await parse_tx_list(chunk)

                        # Extend the all_parsed_txs list with the results
                        all_parsed_txs.extend(parsed_txs)

                    return all_parsed_txs

                relevant_early_txs = await parse_large_tx_list(deploy_sigs)

                relevant_early_txs_confirmed = []
                for tx in relevant_early_txs:
                    if "'transactionError': None" in str(tx):
                        if "CREATE_POOL" in str(tx):
                            relevant_early_txs_confirmed.append(tx)
                            break

                deploy_sig = relevant_early_txs_confirmed[0]['signature']

    deploy_tx = await parse_tx_list([deploy_sig])

    slot = deploy_tx[0]['slot']
    deploy_sig = deploy_tx[0]['signature']

    cluster_url = rpc_url
    client = Client(cluster_url)
    block_txs = client.get_block(slot, max_supported_transaction_version=0).to_json()
    block_txs = json.loads(block_txs)['result']['transactions']

    relevant_txs = []
    for tx in block_txs:
        if mint in str(tx):
            relevant_txs.extend(tx['transaction']['signatures'])

    relevant_txs.remove(deploy_sig)

    relevant_txs = await parse_tx_list(relevant_txs)

    relevant_txs_confirmed = []
    for tx in relevant_txs:
        if "'transactionError': None" in str(tx):
            relevant_txs_confirmed.append(tx)

    relevant_txs = relevant_txs_confirmed

    # Initialize total amount
    total_bundled = 0

    # Add up bundled swaps
    for item in relevant_txs:
        # Check if 'token_transfers' key exists to avoid KeyError
        if 'tokenTransfers' in item:
            # Iterate through each dictionary in the 'token_transfers' list
            for transfer in item['tokenTransfers']:
                # Check if this dictionary's 'mint' matches our variable and add 'token_amount' to total
                if transfer['mint'] == mint:
                    total_bundled += transfer['tokenAmount']

    bundled = round((total_bundled / supply * 100), 2)
    print(f'Bundled: {bundled}%')

    parsed_deploy_tx = await parse_tx_list([deploy_sig])

    lp_creation_time = parsed_deploy_tx[0]['timestamp']

    starting_sol = 0
    starting_tokens = 0
    sol_price = await get_sol_price()

    # GET STARTING SOL AND STARTING TOKENS
    for item in parsed_deploy_tx:
        # Check if 'token_transfers' key exists to avoid KeyError
        if 'tokenTransfers' in item:
            # Iterate through each dictionary in the 'token_transfers' list
            for transfer in item['tokenTransfers']:
                # Check if this dictionary's 'mint' matches our variable and add 'token_amount' to total
                if transfer['mint'] == 'So11111111111111111111111111111111111111112':
                    starting_sol += transfer['tokenAmount']
                if transfer['mint'] == mint:
                    starting_tokens += transfer['tokenAmount']

    starting_liq = round((starting_sol * sol_price * 2), 2)
    starting_price = starting_sol * sol_price / starting_tokens
    starting_mc = round((starting_price * supply), 2)

    airdropped = round((supply - starting_tokens) / supply * 100, 2)

    if deployer in ['TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM']:
        airdropped = 0.00

    print(f'Airdropped: {airdropped}%')

    payload = {
        'token_mint': mint,
        'symbol': symbol,
        'name': name,
        'img_url': img_url,
        'starting_mc': starting_mc,
        'starting_liq': starting_liq,
        'twitter': twitter,
        'telegram': telegram,
        'other_links': other_links,
        'lp_creation_time': lp_creation_time,
        'deployer': deployer,
        'bundled': bundled,
        'airdropped': airdropped,
        'supply': supply,
        'decimals': decimals
    }
    return payload


asyncio.run(retrieve_metadata('DiLYebKmTfNdBxeSJYbgJGMhvW64DASa4D32gf93XcHm'))
