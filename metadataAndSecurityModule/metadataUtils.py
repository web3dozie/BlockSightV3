import json
import random, re, time, aiohttp, asyncio, ast

from pprint import pprint

from solana.exceptions import SolanaRpcException
from solana.rpc.api import Client
from solana.rpc.commitment import Commitment
from solana.rpc.core import RPCException
from solders.pubkey import Pubkey
from datetime import datetime, timedelta
from dbs.db_operations import mint_exists, add_metadata_to_db, get_metadata_from_db

helius_api_key = 'cfc89cfc-2749-487b-9a76-58b989e70909'
rpc_url = f'https://mainnet.helius-rpc.com/?api-key={helius_api_key}'


async def get_sol_price(token_mint='So11111111111111111111111111111111111111112'):
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_mint}'
    max_retries = 3  # Number of retries
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        token_data = await response.json()

                        price = float(token_data['pairs'][0]['priceUsd'])

                        return price

                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")

        except Exception as e:
            retries += 1
            # print(f"Error: {e}, DXS retrying in 1 second...")
            await asyncio.sleep(1)
        else:
            # If successful, exit the loop
            break

    if retries >= max_retries:
        print("Failed to fetch data after retries.")
        return 200


async def get_wallet_txs(wallet: str, api_key=helius_api_key, start_days_ago=30, tx_type=''):
    base_url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={api_key}"
    if tx_type != '':
        base_url += f'&type={tx_type}'
    secs_ago = int(start_days_ago * 24 * 60 * 60)
    count = 0
    tx_data = []
    last_tx_sig = None
    zero_trigger = True
    last_tx_timestamp = int(time.time())  # Current timestamp
    max_retries = 1  # Number of retries

    while (last_tx_timestamp >= (int(time.time()) - secs_ago)) and zero_trigger and (count <= 35):
        url = base_url
        count += 1
        if last_tx_sig:  # Append 'before' parameter only for subsequent requests
            url += f'&before={last_tx_sig}'

        retries = 0
        while retries < max_retries:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            tx_data_batch = await response.json()
                            # print(f'There are {len(tx_data_batch)} txs in this "batch")
                            if not tx_data_batch:  # Empty response, exit loop
                                zero_trigger = False
                                break

                            for tx in tx_data_batch:
                                tx_data.append(tx)

                            last_tx = tx_data_batch[-1]
                            last_tx_sig = last_tx['signature']
                            # print(f'With this sig: {last_tx_sig}\n')
                            last_tx_timestamp = last_tx['timestamp']
                            break  # Break from retry loop on success

                        else:
                            raise Exception(f"Failed to fetch tx data for {wallet}, status code: {response.status}")

            except Exception as e:
                retries += 1
                print(f"Error: {e}, retrying in 0.1 seconds...")
                await asyncio.sleep(0.1)

            if retries >= max_retries:
                print("Failed to fetch data after retries.")
                return []
    return tx_data


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
            # print(f"Error: {e}, DXS retrying in 1 seconds... | Mint: {mint_token}")
            await asyncio.sleep(0.25)
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

    extended_ranges = [[1, -1], [2, -2], [3, -3], [4, -4], [5, -5], [6, -6], [7, -7], [8, -8], [9, -9], [10, -10],
                       [11, -11], [12, -12], [13, -13], [14, -14], [15, -15], [16, -16], [17, -17], [18, -18],
                       [19, -19],
                       [20, -20], [21, -21], [22, -22], [23, -23], [24, -24], [25, -25], [26, -26], [27, -27],
                       [28, -28]]

    retries = 3
    for attempt in range(retries):
        for delta_range in [initial_range] + extended_ranges:
            try:
                for delta in delta_range:
                    try:
                        slot_timestamp = client.get_block_time(slot_number + delta).value
                        if slot_timestamp is not None:
                            return slot_timestamp  # Return on the first successful fetch
                    except RPCException:
                        await asyncio.sleep(0.2)  # Wait before trying the next delta
                        continue  # Proceed to try with the next delta in the range
            except SolanaRpcException:
                continue
        if attempt < retries - 1:  # If not the last attempt, reset to try the entire range again
            print(f"Attempt {attempt + 1} failed, retrying entire range after a short wait...")
            await asyncio.sleep(0.5)  # Wait before retrying the entire range
        else:
            return None

    # If the function hasn't returned by this point, no valid timestamp was found
    raise ValueError(f"No valid timestamp found near slot number {slot_number}")


async def deep_deploy_tx_search(target_timestamp):
    # Start by getting the current slot number and timestamp
    current_slot_number, current_timestamp = await get_current_slot_timestamp()

    # If the current timestamp is less than the target, the slot doesn't exist yet
    if current_timestamp < target_timestamp:
        print("Target timestamp is in the future.")
        return None

    low = 10000000
    high = current_slot_number
    last_valid_mid = None  # Keep track of the last valid slot found

    while low <= high:
        mid = (low + high) // 2
        mid_timestamp = await get_target_slot_timestamp(mid)

        if mid_timestamp is None:
            return

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


async def parse_tx_list(tx_list, api_key=helius_api_key, session=None):
    if tx_list == ['']:
        return []

    url = f"https://api.helius.xyz/v0/transactions/?api-key={api_key}"
    # Parameters for retry logic
    max_attempts = 5
    attempt = 0

    while attempt < max_attempts:
        try:
            if session is None:

                max_attempts = 5  # Set the maximum number of retry attempts

                for attempt in range(max_attempts):
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.post(url, json={"transactions": tx_list}) as response:
                                data = await response.json()
                        break  # Exit the loop if the function succeeds
                    except Exception as e:
                        if attempt < max_attempts - 1:
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        else:
                            raise  # Re-raise the last exception if all retries fail

                return data
            else:
                response = await session.post(url, json={"transactions": tx_list})
                data = await response.json()
                return data

        except aiohttp.client_exceptions.ServerDisconnectedError as e:
            print(f"Server disconnected. Retrying... ({attempt + 1}/{max_attempts})")
            attempt += 1
            if attempt < max_attempts:
                # Wait a bit before retrying to give the server some time (adjust as needed)
                await asyncio.sleep(0.5)
            else:
                raise e


async def get_data_from_helius(token_mint, api_key=helius_api_key):
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

    max_attempts = 5  # Set the maximum number of retry attempts
    for attempt in range(max_attempts):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status == 200:
                        result = await response.json()
                        result = result.get('result')

                    else:
                        print(f"Failed to fetch metadata for {token_mint} (helius). Status code:", response.status)
                        result = {}
            break  # Exit the loop if the function succeeds
        except Exception as e:
            if attempt < max_attempts - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise  # Re-raise the last exception if all retries fail

    return result


async def retrieve_metadata(token_mint: str, api_key=helius_api_key, session=None):
    result = await get_data_from_helius(token_mint, api_key) # TODO USE SESSION HERE

    try:
        symbol = result['content']['metadata']['symbol']
    except KeyError:
        print(f'Mint has an error could not retrieve metadata: {token_mint}')
        return

    name = result['content']['metadata']['name']

    # check two places and fall back on a default img_url
    img_url = (result['content']['links'].get('image') or result['content']['files'].get('uri') or
               'https://cdn-icons-png.flaticon.com/512/2748/2748558.png')

    # for socials check description for 3 or more links (if it isn't there pass to other function)
    try:
        description = result['content']['metadata']['description']
    except KeyError:
        description = ''

    socials = extract_links(description)

    # if any key in the dict is empty --> use the json metadata
    if any(value is None for value in socials.values()):
        socials = get_socials(result['content']['json_uri'])

    twitter, telegram, other_links = socials['twitter'], socials['telegram'], socials['others']

    decimals = result['token_info']['decimals']
    supply = round(((result['token_info']['supply']) / (10 ** decimals)), 2)

    deployer = result['authorities'][0]['address']

    max_retries = 1
    attempts = 0
    deploy_sig = None
    lp_creation_time = None

    while attempts < max_retries:
        try:
            if deployer in ['TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM']:
                raise Exception(f'Pump Fun Token: Going Deep')

            deploy_tx = await get_wallet_txs(deployer, start_days_ago=1000, tx_type="CREATE_POOL")

            relevant_deploy = []

            for tx in deploy_tx:
                if token_mint in str(tx):
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
                print(f'\n\nStarting Deep Search for {token_mint}')

                # After max retries, handle with the except logic

                dxs_data = await get_dxs_data(token_mint)

                lp_creation_time = dxs_data['lp_creation_time']
                lp_address = dxs_data['pool_address']

                try:
                    deploy_slot = await deep_deploy_tx_search(lp_creation_time - 1)

                    if deploy_slot is None:
                        raise TypeError

                except TypeError:
                    return {
                        'token_mint': token_mint,
                        'symbol': symbol,
                        'name': name,
                        'img_url': img_url,
                        'starting_mc': None,
                        'starting_liq': None,
                        'twitter': None,
                        'telegram': None,
                        'other_links': None,
                        'lp_creation_time': lp_creation_time,
                        'deployer': deployer,
                        'bundled': None,
                        'airdropped': None,
                        'supply': supply,
                        'decimals': decimals
                    }

                slot_txs_plus_10 = []
                for tries in range(0, 10):
                    cluster_url = rpc_url
                    client = Client(cluster_url)

                    if deploy_slot is None:
                        return

                    trying_with = deploy_slot + tries

                    try:
                        block_txs = client.get_block(trying_with, max_supported_transaction_version=0).to_json()
                    except RPCException:
                        continue
                    except SolanaRpcException:
                        continue

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

                        max_attempt = 5  # Set the maximum number of retry attempts

                        parsed_txs = []
                        for att in range(max_attempt):
                            try:
                                # Parse the current chunk
                                parsed_txs = await parse_tx_list(chunk)
                                break  # Exit the loop if the function succeeds
                            except Exception as e:
                                if att < max_attempt - 1:
                                    await asyncio.sleep(0.25)  # Exponential backoff
                                else:
                                    parsed_txs = []

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

                try:
                    deploy_sig = relevant_early_txs_confirmed[0]['signature']
                except IndexError:
                    deploy_sig = ''

    deploy_tx = await parse_tx_list([deploy_sig], session=session)

    if not deploy_tx:
        return {
            'token_mint': token_mint,
            'symbol': symbol,
            'name': name,
            'img_url': img_url,
            'starting_mc': None,
            'starting_liq': None,
            'twitter': None,
            'telegram': None,
            'other_links': None,
            'lp_creation_time': lp_creation_time,
            'deployer': deployer,
            'bundled': None,
            'airdropped': None,
            'supply': supply,
            'decimals': decimals,
            'lp_address': None,
            'initial_lp_supply': None
        }

    # TODO Calculate LP Supply and use it to calculate LP Burns
    deploy_trf = deploy_tx[0]['tokenTransfers'][-1]
    lp_address = deploy_trf['mint']
    initial_lp_supply = deploy_trf['tokenAmount']

    slot = deploy_tx[0]['slot']
    deploy_sig = deploy_tx[0]['signature']

    cluster_url = rpc_url
    client = Client(cluster_url)
    block_txs = client.get_block(slot, max_supported_transaction_version=0).to_json()
    block_txs = json.loads(block_txs)['result']['transactions']

    relevant_txs = []
    for tx in block_txs:
        if token_mint in str(tx):
            relevant_txs.extend(tx['transaction']['signatures'])

    relevant_txs.remove(deploy_sig)

    max_attempts = 5

    for attempt in range(max_attempts):
        try:
            relevant_txs = await parse_tx_list(relevant_txs)
            break  # Exit the loop if the function succeeds
        except Exception as e:
            if attempt < max_attempts - 1:
                await asyncio.sleep(0.25)  # Exponential backoff
            else:
                raise  # Re-raise the last exception if all retries fail

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
                if transfer['mint'] == token_mint:
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
                if transfer['mint'] == token_mint:
                    starting_tokens += transfer['tokenAmount']

    starting_liq = round((starting_sol * sol_price * 2), 2)
    starting_price = starting_sol * sol_price / starting_tokens
    starting_mc = round((starting_price * supply), 2)

    airdropped = round((supply - starting_tokens) / supply * 100, 2)

    if deployer in ['TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM']:
        airdropped = 0.00

    airdropped = None if abs(airdropped) >= 100 else airdropped
    bundled = None if abs(bundled) >= 100 else bundled

    twitter = [None] if not twitter else twitter
    telegram = [None] if not telegram else telegram
    other_links = [None] if not other_links else other_links

    payload = {
        'token_mint': token_mint,
        'symbol': symbol,
        'name': name,
        'img_url': img_url,
        'starting_mc': starting_mc,
        'starting_liq': starting_liq,
        'twitter': twitter[0],
        'telegram': telegram[0],
        'other_links': other_links[0],
        'lp_creation_time': lp_creation_time,
        'deployer': deployer,
        'bundled': bundled,
        'airdropped': airdropped,
        'supply': supply,
        'decimals': decimals,
        'lp_address': lp_address,
        'initial_lp_supply': initial_lp_supply
    }
    return payload


async def get_metadata(token_mint, regular_use: bool = True, pool=None, session=None):
    # if token is not in DB already, fetch metadata with helius API and add it to db[else get it from the db]

    if not await mint_exists(token_mint, pool=pool):  # TODO -> I/O that we can potentially remove
        # print(token_mint)
        metadata = await retrieve_metadata(token_mint, session=session)

        '''
        if not metadata:
            # TODO -> add a record that logs the bad token so that we can early exit if we see it again.

            metadata = {
                'token_mint': token_mint,
                'symbol': symbol,
                'name': name,
                'img_url': img_url,
                'starting_mc': None,
                'starting_liq': None,
                'twitter': None,
                'telegram': None,
                'other_links': None,
                'lp_creation_time': None,
                'deployer': deployer,
                'bundled': None,
                'airdropped': None,
                'supply': supply,
                'decimals': decimals
            }


            pprint(f'{token_mint} DOES NOT HAVE VALID METADATA')
            return
        '''

        # Add metadata to db
        try:
            await add_metadata_to_db(metadata, pool=pool)  # TODO -> I/O that we can optimize
            return metadata
        except Exception as metadata_error:
            pprint('METADATA ERROR')
            pprint(metadata_error)
            raise metadata_error

    else:
        if regular_use:
            # retrieve metadata from db
            metadata = await get_metadata_from_db(token_mint, pool=pool)  # TODO I/O that we can skip/optimise
            return metadata
        else:
            return None


async def get_dexscreener_data(token_mint):
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_mint}'
    max_retries = 3  # Number of retries
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        token_data = await response.json()  # This is a list of pools the token has open

                        lp_creation_time = int(token_data['pairs'][0]['pairCreatedAt'] / 1000)
                        try:
                            fdv = int(token_data['pairs'][0]['fdv'])
                        except Exception as e:
                            fdv = 0
                            pprint(e)
                        price = float(token_data['pairs'][0]['priceUsd'])
                        dxs_link = token_data['pairs'][0]['url']
                        symbol = token_data['pairs'][0]['baseToken']['symbol']
                        lp_address = token_data['pairs'][0]["pairAddress"]
                        liquidity = token_data['pairs'][0]["liquidity"]["usd"]

                        return {
                            'price': price,
                            'fdv': fdv,
                            'lp_creation_time': lp_creation_time,
                            'dxs_link': dxs_link,
                            'symbol': symbol,
                            'lp_address': lp_address,
                            'liquidity': liquidity
                        }

                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")

        except Exception as e:
            retries += 1
            print(f"Error: {e}, DXS retrying in 10 seconds...")
            await asyncio.sleep(10)
        else:
            # If successful, exit the loop
            break

    if retries >= max_retries:
        print("Failed to fetch data after retries.")
        return {
            'price': 0.0,
            'fdv': 0,
            'lp_creation_time': 0,
            'dxs_link': 'dexscreener.com',
            'symbol': 'NO_SYMBOL',
            'lp_address': 'LP_NOT_FOUND',
            'liquidity': 0
        }


async def get_full_dxs_data(token_mint):
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_mint}'
    max_retries = 3  # Number of retries
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        token_data = await response.json()  # This is a list of pools the token has open

                        lp_creation_time = int(token_data['pairs'][0]['pairCreatedAt'] / 1000)
                        fdv = int(token_data['pairs'][0]['fdv'])
                        price = float(token_data['pairs'][0]['priceUsd'])
                        liquidity = token_data['pairs'][0]["liquidity"]["usd"]

                        buys_5m = token_data['pairs'][0]['txns']['m5']['buys']
                        sells_5m = token_data['pairs'][0]['txns']['m5']['sells']
                        volume_5m = token_data['pairs'][0]['volume']['m5']
                        price_change_5m = token_data['pairs'][0]['priceChange']['m5']

                        buys_1h = token_data['pairs'][0]['txns']['h1']['buys']
                        sells_1h = token_data['pairs'][0]['txns']['h1']['sells']
                        volume_1h = token_data['pairs'][0]['volume']['h1']
                        price_change_1h = token_data['pairs'][0]['priceChange']['h1']

                        return {
                            'price': price,
                            'fdv': fdv,
                            'lp_creation_time': lp_creation_time,
                            'liquidity': liquidity,
                            'buys_5m': buys_5m,
                            'sells_5m': sells_5m,
                            'volume_5m': volume_5m,
                            'price_change_5m': price_change_5m,

                            'buys_1h': buys_1h,
                            'sells_1h': sells_1h,
                            'volume_1h': volume_1h,
                            'price_change_1h': price_change_1h
                        }

                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")

        except Exception as e:
            retries += 1
            print(f"Error: {e}, DXS retrying in 10 seconds...: Mint: {token_mint}")
            await asyncio.sleep(10)
        else:
            # If successful, exit the loop
            break

    if retries >= max_retries:
        print("Failed to fetch data after retries.")
        return {}


async def is_older_than(token_mint, minutes=150):
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_mint}'
    max_retries = 3  # Number of retries
    retries = 0

    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        token_data = await response.json()
                        two_and_half_hours_ago = int((datetime.now() - timedelta(minutes=minutes)).timestamp())
                        creation_time = int(token_data['pairs'][0]['pairCreatedAt'] / 1000)

                        if creation_time >= two_and_half_hours_ago:
                            return True
                        else:
                            return False

                    else:
                        raise Exception(f"Failed to fetch data, status code: {response.status}")

        except Exception as e:
            retries += 1
            if random.randint(1, 20) == 10:
                print(f"Error: {e}, YNGR retrying in 10 seconds...")
                print(f'Sus Mint: {token_mint}')
            await asyncio.sleep(1)
        else:
            # If successful, exit the loop
            break

    if retries >= max_retries:
        if random.randint(1, 20) == 10:
            print("Failed to fetch data after retries.")
        return False


async def get_num_holders(mint='', helius_key=helius_api_key):
    url = f'https://mainnet.helius-rpc.com/?api-key={helius_key}'
    url = rpc_url
    async with aiohttp.ClientSession() as session:
        page = 1
        all_owners = set()

        while True:
            payload = {
                "jsonrpc": "2.0",
                "method": "getTokenAccounts",
                "id": "helius-test",
                "params": {
                    "page": page,
                    "limit": 1000,
                    "displayOptions": {},
                    "mint": mint,
                },
            }
            async with session.post(url, json=payload) as response:
                data = await response.json()
                if not data.get('result') or len(data['result']['token_accounts']) == 0:
                    break

                for account in data['result']['token_accounts']:
                    all_owners.add(account['owner'])
                page += 1

        return len(all_owners)



