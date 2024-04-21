import datetime, asyncio, re, aiohttp, json, asyncpg, time
from pprint import pprint

from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest

config = {
}

try:
    with open('config.json', 'r') as file:
        config = json.load(file)
except:
    print("config.json required")
    exit()

api_id = config["api_id"]
api_hash = config["api_hash"]
tg_channel = "MadarasGambles"
blocksight_api = config["blockSightApi"]
dex_api = config["dexApi"]
blocksight_db_url = config["blockSightDB"]


async def insert_address_time_into_db(db_url=blocksight_db_url, addressTimeData: dict = None, channelId=None):
    records_to_insert = []
    for k, v in addressTimeData.items():
        records_to_insert.append((k, channelId, v,))

    try:
        conn = await asyncpg.connect(dsn=db_url)
        print("Connected to the database")
    except Exception as e:
        print(f"Error {e} while connecting to the database")
        raise e

    try:
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
        print("Records upserted successfully")
    except Exception as e:
        print(f"Error {e} while upserting records")
        raise e
    finally:
        await conn.close()
        print("Connection closed")

    print("tg calls inserted")


async def extract_address_time_data(messages) -> dict:
    addressTimeData = {}
    potentialAddresses = {}
    dexIDs = []

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
                    print(f"Calling DexScreener For: {dex_id}")
                    async with session.get(f"{dex_api}/tokens/{dex_id}") as response:
                        result = await response.json()
                        if response.status == 200 and result and result.get("pairs"):
                            token = result["pairs"][0]["baseToken"]["address"]
                            if token and len(token) > 0:
                                token_found = True
                                addressTimeData[token] = int(message.date.timestamp())
            except Exception as error:
                print(
                    f"Error {error} while trying to find token for id {dex_id} (dex - token). "
                    f"Status code: {response.status}")

            if not token_found:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                                f"{dex_api}/pairs/solana/{dex_id}") as response:
                            print("making dex call")
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

    verifySemaphore = asyncio.Semaphore(2)

    async def verify_token_mint(address):
        async with verifySemaphore:
            async with aiohttp.ClientSession() as session:
                print("Making Helius Call")
                async with session.get(f"{blocksight_api}/core/verify-token-mint/{address}") as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get("result") != "valid":
                            del potentialAddresses[address]

                    else:
                        print(f"Failed to verify token {address} (helius). Status code:", response.status)

    if len(potentialAddresses.keys()) > 0:
        tasks = [verify_token_mint(address) for address in potentialAddresses.keys()]
        await asyncio.gather(*tasks)
        addressTimeData = {**potentialAddresses, **addressTimeData}
        print(f"{len(addressTimeData)} trades found")
    return addressTimeData


async def vetChannel(channel=tg_channel, db_url=blocksight_db_url, window=30, tg_client=None):
    print(f"Vetting  channel {channel}")
    async def is_outdated_channel(channel_id, db_url=blocksight_db_url):
        """
        Connect to DB, Check if the channel ID exists and if it is too old
        Returns a bool depending on the result
        """
        query = "SELECT last_updated FROM channel_stats WHERE channel_id = $1"

        try:
            conn = await asyncpg.connect(dsn=db_url)
            last_seen = await conn.fetchval(query, channel_id)
        except Exception as e:
            print(f"Error {e} while fetching channel last seen from blockSight's db")
            raise e

        if not last_seen:
            return True
        elif int(time.time()) - last_seen < 24 * 60 * 60:
            return False
        else:
            return True

    if not tg_client:
        async with TelegramClient('anon', api_id, api_hash) as client:
            await client.start()
            try:
                channel_entity = await client.get_input_entity(channel)
            except ValueError as e:
                print("Given channel can't be found")
                raise e
            except Exception as e:
                print(f"An unexpected error occurred, {e}")
                raise e

            try:
                conn = await asyncpg.connect(dsn=db_url)
            except Exception as e:
                print(f"Error {e} while connecting to blockSight's db")
                raise e

            if await is_outdated_channel(channel_entity.channel_id):
                query = "SELECT MAX(timestamp) FROM tg_calls WHERE channel_id = $1"
                last_db_tx_timestamp = await conn.fetchval(query, channel_entity.channel_id)

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
                if days_of_data_to_fetch > 0:
                    print("fetching new messages")
                    while True:
                        old_len = len(messages)
                        messages.extend((await client(GetHistoryRequest(
                            peer=channel_entity,
                            limit=3000,  # Anything more than this causes slowdowns
                            offset_date=offset_date,
                            offset_id=0,
                            max_id=0,
                            min_id=0,
                            add_offset=0,
                            hash=0
                        ))).messages)

                        if len(messages) == old_len:
                            break
                        elif messages[-1].id == messages[-2].id:
                            break

                        if messages[-1].date <= breakoff_point.replace(tzinfo=datetime.timezone.utc):
                            break
                        else:
                            offset_date = messages[-1].date

                    await client.disconnect()
                    print(f"{len(messages)} messages fetched")
                    # filter for text messages
                    messages = [message for message in messages if message.message and message.message != "" and int(
                        message.date.timestamp()) >= thirty_days_ago]

                    addressTimeData = await extract_address_time_data(messages)

                    await insert_address_time_into_db(addressTimeData=addressTimeData,
                                                      channelId=channel_entity.channel_id)
    else:
        client = tg_client
        await client.start()
        try:
            channel_entity = await client.get_input_entity(channel)
        except ValueError as e:
            print("Given channel can't be found")
            raise e
        except Exception as e:
            print(f"An unexpected error occurred, {e}")
            raise e

        try:
            conn = await asyncpg.connect(dsn=db_url)
        except Exception as e:
            print(f"Error {e} while connecting to blockSight's db")
            raise e

        if await is_outdated_channel(channel_entity.channel_id):
            query = "SELECT MAX(timestamp) FROM tg_calls WHERE channel_id = $1"
            last_db_tx_timestamp = await conn.fetchval(query, channel_entity.channel_id)

            days_of_data_to_fetch = 0

            if not last_db_tx_timestamp:
                time_since_last_update = 31 * 24 * 60 * 60
            else:
                time_since_last_update = int(time.time()) - last_db_tx_timestamp

            if time_since_last_update > 24 * 60 * 60:  # seconds in one day
                days_of_data_to_fetch = round(time_since_last_update / (24 * 60 * 60))

            print(f"Vetting channel {channel}")
            offset_date = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())
            breakoff_point = offset_date - datetime.timedelta(days=days_of_data_to_fetch)

            messages = []

            thirty_days_ago = int(time.time()) - 30 * 24 * 60 * 60
            if days_of_data_to_fetch > 0:
                print("fetching new messages")
                while True:
                    old_len = len(messages)
                    messages.extend((await client(GetHistoryRequest(
                        peer=channel_entity,
                        limit=3000,  # Anything more than this causes slowdowns
                        offset_date=offset_date,
                        offset_id=0,
                        max_id=0,
                        min_id=0,
                        add_offset=0,
                        hash=0
                    ))).messages)

                    if len(messages) == old_len:
                        break

                    if messages[-1].date <= breakoff_point.replace(tzinfo=datetime.timezone.utc):
                        break
                    else:
                        offset_date = messages[-1].date

                await client.disconnect()
                print(f"{len(messages)} messages fetched")
                # filter for text messages
                messages = [message for message in messages if message.message and message.message != "" and int(
                    message.date.timestamp()) >= thirty_days_ago]

                addressTimeData = await extract_address_time_data(messages)

                await insert_address_time_into_db(addressTimeData=addressTimeData,
                                                  channelId=channel_entity.channel_id)

    query = "SELECT token_mint, timestamp FROM tg_calls WHERE channel_id = $1 and timestamp >= $2"
    conn = await asyncpg.connect(dsn=db_url)
    tg_calls = await conn.fetch(query, channel_entity.channel_id, (int(time.time()) - window * 24 * 60 * 60))
    winCheckSemaphore = asyncio.Semaphore(30)

    async def check_if_win(address, timestamp):
        async with winCheckSemaphore:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{blocksight_api}/core/is-win-trade",
                                       params={"token": address, "timestamp": str(timestamp)}) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result.get("result")

                    else:
                        print(f"Failed to check trade {[address, timestamp]}. Status code: {response.status}")

    tasks = [check_if_win(address, time_) for address, time_ in tg_calls]
    results = await asyncio.gather(*tasks)

    if len(results) != 0:
        win_count = results.count(True)
        win_rate = round((win_count / len(results) * 100), 2)
        print(f"Channel {channel}'s win rate is {win_rate}%")
    else:
        win_rate = 0.0

    upsert_query = """
        INSERT INTO channel_stats (channel_id, win_rate, trades_count, last_updated, channel_name)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (channel_id)
        DO UPDATE SET win_rate = EXCLUDED.win_rate, last_updated = EXCLUDED.last_updated, 
        trades_count = EXCLUDED.trades_count, channel_name = EXCLUDED.channel_name;
    """

    try:
        await conn.execute(upsert_query, channel_entity.channel_id, win_rate, len(results), int(time.time()),
                           channel)
        print(f"{channel}'s data upserted")
    except Exception as e:
        print(f"Error {e} while upserting {channel}'s data to db")
        raise e
    finally:
        await conn.close()

    return win_rate, len(results)


if __name__ == "__main__":
    asyncio.run(vetChannel())
