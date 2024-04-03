import random, re, time, aiohttp, asyncio, aiofiles, ast

from pprint import pprint as pr

from solana.rpc.api import Client
from solders.pubkey import Pubkey
from datetime import datetime, timedelta
from dbs.db_operations import mint_exists, add_metadata_to_db, get_metadata_from_db

helius_api_key = 'cfc89cfc-2749-487b-9a76-58b989e70909'


# LP PERCENTAGE BURN AND LOCK
async def get_just_supply(token_mint: str, api_key=helius_api_key):
    # TODO get supply from db (if not in db
    # fetch, add to db and re-get from db

    pass
    # return supply


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
    max_retries = 3  # Number of retries

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
                print(f"Error: {e}, retrying in 1 seconds...")
                await asyncio.sleep(1)

            if retries >= max_retries:
                print("Failed to fetch data after retries.")
                return []
    return tx_data


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
            if response.status == 200:  # Checking if request was successful
                result = await response.json()
                pr(result.get('result'))
            else:
                print("Failed to fetch asset. Status code:", response.status)


async def get_metadata(token_mint):
    # if token is not in DB already, fetch metadata with helius API and add it to db[else get it from the db]

    if not await mint_exists(token_mint):
        metadata = await retrieve_metadata(token_mint)

        # Add metadata to db
        try:
            await add_metadata_to_db(metadata)
        except Exception as metadata_error:
            pr('METADATA ERROR')
            pr(metadata_error)
            raise metadata_error

    else:
        # retrieve metadata from db
        metadata = await get_metadata_from_db(token_mint)

    return metadata


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
                            pr(e)
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


async def parse4helius(mint):
    data = await retrieve_metadata(mint)

    # pr(data)

    legacy_meta = data[0]['legacyMetadata']
    offchain_meta = data[0]['offChainMetadata']
    onchain_meta = data[0]['onChainMetadata']
    # onchain_info = data[0]['onChainAccountInfo']

    mint = data[0]['account']

    # SYMBOL
    try:
        symbol = onchain_meta['metadata']['data']['symbol']
    except (TypeError, KeyError):
        try:
            symbol = legacy_meta['symbol']
        except (KeyError, TypeError):
            print(f'INVALID SYMBOL | Mint: {mint}')
            symbol = 'NOT_FOUND'

    # NAME
    try:
        name = onchain_meta['metadata']['data']['name']
    except (TypeError, KeyError):
        try:
            name = legacy_meta['name']
        except (KeyError, TypeError):
            print('INVALID NAME')
            name = 'NOT_FOUND'

    # IMG_URL
    try:
        img_url = offchain_meta['metadata']['image']
    except (TypeError, KeyError):
        try:
            img_url = legacy_meta['logoURI']
        except (TypeError, KeyError):
            print('INVALID URL')
            img_url = 'https://cdn-icons-png.flaticon.com/512/2748/2748558.png'

    # SOCIALS
    def get_social_links():
        def extract_links(text):
            # Regex pattern to find URLs
            url_pattern = r'https?://[^\s,\'"]+'
            urls = re.findall(url_pattern, text)

            # Categorizing URLs
            telegram_links = [url for url in urls if 't.me' in url]
            twitter_links = list(set([url for url in urls if 'x.com' in url or 'twitter.com' in url]))
            website_links = [url for url in urls if url not in telegram_links and url not in twitter_links]
            website_links = [url for url in website_links if len(url) < 50]
            website_links = website_links[1:]

            return {
                'twitter': twitter_links,
                'telegram': telegram_links,
                'others': website_links
            }

        def remove_image_line(original_str, keyword='image'):
            # Split the string into lines
            lines = original_str.split('\n')

            # Filter out the line containing the keyword
            lines_without_keyword = [line for line in lines if keyword not in line]

            # Join the lines back into a single string, preserving original line breaks
            new_str = '\n'.join(lines_without_keyword)

            return new_str

        try:
            # check description
            try:
                social_links = str(offchain_meta['metadata'])

            except KeyError:
                social_links = ""
            links = extract_links(social_links)

            # if desc is empty, check extensions
            if links == {'others': [], 'telegram': [], 'twitter': []}:
                try:
                    social_links = str(offchain_meta['metadata']['extensions'])
                except KeyError:
                    social_links = ""
                links = extract_links(social_links)

                # if extensions are empty, raise an error and check legacy metadata
                if links == {'others': [], 'telegram': [], 'twitter': []}:
                    raise TypeError

        except TypeError:
            try:
                social_links = str(legacy_meta['extensions'])
                links = extract_links(social_links)

            except TypeError:
                links = {'others': [], 'telegram': [], 'twitter': []}

        return links

    socials = get_social_links()

    twitter = socials['twitter']

    if twitter:
        twitter = twitter[0]
    else:
        twitter = ''

    telegram = socials['telegram']
    if telegram:
        telegram = telegram[0]
    else:
        telegram = ''

    other_links = socials['others']
    other_links = ', '.join(other_links)

    return {
        'token_mint': mint,
        'symbol': symbol,
        'name': name,
        'img_url': img_url,
        'twitter': twitter,
        'telegram': telegram,
        'other_links': other_links
    }


async def get_num_holders(mint='', helius_key=helius_api_key):
    url = f'https://mainnet.helius-rpc.com/?api-key={helius_key}'
    url = 'https://lia-gf6xva-fast-mainnet.helius-rpc.com'
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


async def get_top_holder_percentages(mint: str = "", helius_key: str = helius_api_key):
    """
    Asynchronously fetches the 20 largest accounts for a specified SPL Token mint.

    Args:
    - mint_address (str): The base-58 encoded public key of the SPL Token Mint.
    - cluster_url (str, optional): URL of the Solana cluster to query. Defaults to mainnet-beta.

    Returns:
    - list: A list of dictionaries containing the 20 largest accounts for the specified SPL Token.
    """

    supply = await get_just_supply(mint)

    # Initialize the Solana RPC client
    cluster_url = f'https://mainnet.helius-rpc.com/?api-key={helius_key}'
    cluster_url = 'https://lia-gf6xva-fast-mainnet.helius-rpc.com'

    client = Client(cluster_url)

    # Convert the mint address string to a Pubkey object
    mint_pubkey = Pubkey.from_string(mint)

    # Fetch the 20 largest accounts
    try:
        data = ast.literal_eval(client.get_token_largest_accounts(mint_pubkey).to_json())['result']['value']
    except Exception as e:
        await asyncio.sleep(3)
        data = ast.literal_eval(client.get_token_largest_accounts(mint_pubkey).to_json())['result']['value']
        print(e)

    data = [{k: v for k, v in d.items() if k == 'uiAmount'} for d in data]
    data = [d['uiAmount'] for d in data]
    try:
        top_10 = round((sum(data[0:10]) / supply * 100), 2)
        top_20 = round((sum(data) / supply * 100), 2)
    except TypeError:
        top_10 = 100.0
        top_20 = 100.0
    return {
        'top_10': top_10,
        'top_20': top_20
    }
