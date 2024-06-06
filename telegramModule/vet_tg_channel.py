import datetime, asyncio, re, aiohttp, json, time

from telethon.tl.functions.messages import GetHistoryRequest

from dbs.db_operations import get_id_from_channel
from telegramModule.tg_client_pooling import TelegramClientPool
from metadataAndSecurityModule.metadataUtils import get_data_from_helius
from priceDataModule.price_utils import is_win_trade, token_prices_to_db

try:
    with open('config.json', 'r') as file:
        config = json.load(file)
except:
    print("config.json required")
    exit()

api_id = config["api_id"]
api_hash = config["api_hash"]
blocksight_api = config["blockSightApi"]
dex_api = config["dexApi"]
blocksight_db_url = config["blockSightDB"]


async def insert_address_time_into_db(addressTimeData: dict = None, channelId=None, pool=None):
    records_to_insert = []
    for k, v in addressTimeData.items():
        records_to_insert.append((k, channelId, v,))

    conn = await pool.acquire()

    # pprint(records_to_insert)

    try:
        async with conn.transaction():
            await conn.execute('''
                    CREATE TEMP TABLE temp_tg_calls AS TABLE tg_calls WITH NO DATA;
                ''')
            await conn.copy_records_to_table(
                'temp_tg_calls',
                columns=['token_mint', 'channel_id', 'timestamp'],
                records=records_to_insert
            )
            await conn.execute('''
                    INSERT INTO tg_calls (token_mint, channel_id, timestamp)
                    SELECT token_mint, channel_id, timestamp FROM temp_tg_calls
                    ON CONFLICT (token_mint, channel_id, timestamp) DO UPDATE
                    SET timestamp = EXCLUDED.timestamp;
                ''')
            await conn.execute('''DROP TABLE temp_tg_calls;''')
        # print("Records upserted successfully")
    except Exception as e:
        print(f"Error {e} while upserting records")
        raise e
    finally:
        await pool.release(conn)


async def extract_address_time_data(messages) -> dict:
    # This function processes a list of TG messages and returns
    addressTimeData = {}
    potentialAddresses = {}

    dexIDs = []
    # Compile dexscreener links and solana addresses and add them to their lists/dicts
    for message in messages:
        dexMatches = re.findall(r'(?:https://)?dexscreener.com/solana/\w+', message.message)
        if dexMatches:
            for link in dexMatches:
                # add scheme if absent
                if 'https://' not in link:
                    link = 'https://' + link
                # remove query params
                c = link.find('?')
                if c != -1:
                    link = link[:c]
                dexID = link[31:]
                if dexID not in dexIDs:
                    dexIDs.append(dexID)

        # print(message.message)

        matches = re.findall(r'[1-9A-HJ-NP-Za-km-z]{32,44}', message.message)
        if matches:
            for match in matches:
                if match not in potentialAddresses.keys():
                    potentialAddresses[match] = int(message.date.timestamp())

    checkDexSemaphore = asyncio.Semaphore(2)

    async def dex_id_to_token(dex_id):
        token_found = False
        async with checkDexSemaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{dex_api}/tokens/{dex_id}") as response:
                        result = await response.json()
                        if response.status == 200 and result and result.get("pairs"):
                            token = result["pairs"][0]["baseToken"]["address"]
                            if token and len(token) > 0:
                                token_found = True
                                addressTimeData[token] = int(message.date.timestamp())
            except Exception as error:
                print(f"Error {error} while trying to find token for id {dex_id} (dex - token). {response.status}")

            if not token_found:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                                f"{dex_api}/pairs/solana/{dex_id}") as response:
                            result = await response.json()
                            if response.status == 200 and result and result.get("pairs"):
                                token = result["pairs"][0]["baseToken"]["address"]
                                if token and len(token) > 0:
                                    addressTimeData[token] = int(message.date.timestamp())
                except Exception as error:
                    print(
                        f"Error {error} while trying to find token for id {dex_id} (dex - lp api). "
                        f"Status code: {response.status}")

    if len(dexIDs) > 0:
        tasks = [dex_id_to_token(dex_id) for dex_id in dexIDs]
        await asyncio.gather(*tasks)

    verifySemaphore = asyncio.Semaphore(20)
    verified_mints = dict()

    async def verify_token_mint(address, source='potential'):
        async with verifySemaphore:
            result = await get_data_from_helius(address)
            if "'interface': 'FungibleToken'" in str(result):
                if source == 'potential':
                    verified_mints[address] = potentialAddresses[address]
                elif source == 'dxs':
                    verified_mints[address] = addressTimeData[address]

    if len(potentialAddresses.keys()) > 0:
        tasks = [verify_token_mint(address) for address in potentialAddresses.keys()]
        await asyncio.gather(*tasks)

    tasks = [verify_token_mint(address, source='dxs') for address in addressTimeData.keys()]
    await asyncio.gather(*tasks)

    # print(f"{len(addressTimeData)} valid trades found.")

    return verified_mints


async def is_outdated_channel(channel_id: int, pl=None):
    """
    Connect to DB, Check if the channel ID exists and if it is too old
    Returns a bool depending on the result
    """
    qry = "SELECT last_updated FROM channel_stats WHERE channel_id = $1"

    try:
        cnn = await pl.acquire()
        last_seen = await cnn.fetchval(qry, channel_id)
        await pl.release(cnn)
    except Exception as out_error:
        print(f"Error {out_error} while fetching channel last seen from blockSight's db")
        raise out_error

    if not last_seen:
        return True
    elif int(time.time()) - last_seen < 24 * 60 * 60:
        return False
    else:
        return True


async def vetChannel(channel='', window=30, tg_pool=None, pool=None):

    print(f"Vetting  channel {channel}")

    channel_id = await get_id_from_channel(channel_name=channel, pool=pool)
    conn = None
    try:
        # Fetch new messages if the channel is outdated (>1 day since last update)
        if await is_outdated_channel(channel_id, pl=pool):

            # TODO FIX API HASH/ID
            tg_pool = tg_pool or TelegramClientPool(api_hash='841396171d9b111fa191dcdce768d223', api_id=21348081)

            client = await tg_pool.acquire()

            # Get channel object with TG Client
            try:
                channel_entity = await client.get_input_entity(channel)
                channel_id = channel_entity.channel_id

            except ValueError as e:
                print(f"Given channel {channel} can't be found")
                raise e
            except Exception as e:
                print(f"An unexpected error occurred while trying to resolve channel name, {e}")
                raise e

            if 'channel_id' not in channel_entity.__dict__:
                return

            conn = await pool.acquire()

            try:
                query = "SELECT MAX(timestamp) FROM tg_calls WHERE channel_id = $1"
                last_db_tx_timestamp = await conn.fetchval(query, channel_id)

                days_of_data_to_fetch = 0

                if not last_db_tx_timestamp:
                    time_since_last_update = 31 * 24 * 60 * 60
                else:
                    time_since_last_update = int(time.time()) - last_db_tx_timestamp

                if time_since_last_update > 24 * 60 * 60:  # seconds in one day
                    days_of_data_to_fetch = round(time_since_last_update / (24 * 60 * 60))

                offset_date = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
                breakoff_point = offset_date - datetime.timedelta(days=days_of_data_to_fetch)

                messages = []

                thirty_days_ago = int(time.time()) - 30 * 24 * 60 * 60

                # Fetch new messages
                if days_of_data_to_fetch > 0:

                    while True:
                        old_len = len(messages)
                        messages.extend(
                            (await client(GetHistoryRequest(peer=channel_entity, limit=3000, offset_date=offset_date,
                                                            offset_id=0, max_id=0, min_id=0, add_offset=0,
                                                            hash=0))).messages)

                        if len(messages) == old_len:
                            break
                        elif messages[-1].id == messages[-2].id:
                            break

                        if messages[-1].date <= breakoff_point.replace(tzinfo=datetime.timezone.utc):
                            break
                        else:
                            offset_date = messages[-1].date

                    # print(f"{len(messages)} messages fetched.")
                    # filter for text messages
                    messages = [message for message in messages if message.message and message.message != "" and int(
                        message.date.timestamp()) >= thirty_days_ago]

                    addressTimeData = await extract_address_time_data(messages)

                    await insert_address_time_into_db(addressTimeData=addressTimeData,
                                                      channelId=channel_entity.channel_id, pool=pool)
            finally:
                await tg_pool.release(client)

            query = "SELECT token_mint, timestamp FROM tg_calls WHERE channel_id = $1 and timestamp >= $2"

            # Fetch calls and calculate win_rate
            tg_calls = await conn.fetch(query, channel_entity.channel_id, (int(time.time()) - (window * 24 * 60 * 60)))

            tmts = [[record['token_mint'], record['timestamp']]for record in tg_calls]

            sem = asyncio.BoundedSemaphore(10)

            async def limited_task(tmt):
                async with sem:
                    await token_prices_to_db(tmt[0], tmt[1], int(time.time()), pool=pool)

            # Fetch token info up front and at once
            tasks = [limited_task(tmt) for tmt in tmts]

            # Execute tasks concurrently with a limit of 100 tasks at a time
            await asyncio.gather(*tasks)

            tasks = [is_win_trade(address, time_, pool=pool) for address, time_ in tg_calls]
            results = await asyncio.gather(*tasks)

            if len(results) != 0:
                win_count = results.count(True)
                win_rate = round((win_count / len(results) * 100), 2)
            else:
                win_rate = 0.0

            upsert_query = """
                INSERT INTO channel_stats (channel_id, win_rate, trades_count, last_updated, channel_name, window_value)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (channel_id)
                DO UPDATE SET win_rate = EXCLUDED.win_rate, last_updated = EXCLUDED.last_updated, 
                trades_count = EXCLUDED.trades_count, channel_name = EXCLUDED.channel_name, 
                window_value = EXCLUDED.window_value;
            """

            try:
                await conn.execute(upsert_query, channel_entity.channel_id, win_rate, len(results), int(time.time()),
                                   channel, f"{window}d".zfill(3))
                print(f"{channel}'s data updated")
            except Exception as e:
                print(f"Error {e} while upserting {channel}'s data to db")
                raise e

            if window == 30:
                await vetChannel(channel, window=7, pool=pool)
            if window == 7:
                await vetChannel(channel, window=3, pool=pool)

            return {"win_rate": win_rate, "trade_count": len(results), "time_window": window,
                    "last_updated": int(time.time()), "channel_name": channel, "channel_id": channel_id}

        else:
            conn = await pool.acquire()

            query = f"SELECT * FROM channel_stats WHERE channel_id = $1 and window_value = $2"

            row = await conn.fetchrow(query, channel_id, f"{window}d".zfill(3))

            data = {}
            if row:
                # Map the row to a dictionary. asyncpg returns a Record which can be accessed by keys.
                return {
                    'channel_name': row['channel_name'],
                    'channel_id': row['channel_id'],
                    'trade_count': row['trades_count'],
                    'win_rate': row['win_rate'],
                    'last_updated': row['last_updated'],
                    'time_window': row['window_value']
                }

    finally:
        if conn:
            await pool.release(conn)

if __name__ == "__main__":
    asyncio.run(vetChannel())
