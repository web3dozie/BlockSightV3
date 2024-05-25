import asyncio
import datetime
import json
from pprint import pprint

import asyncpg
from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest, GetChannelsRequest
from telethon.tl.types import RpcError, InputPeerChannel, InputChannel
from telethon.types import Channel

from dbs.db_operations import pg_db_url, useful_channels
from telegramModule.vet_tg_channel import extract_address_time_data, insert_address_time_into_db

config = {}

try:
    with open('../telegramModule/config.json', 'r') as file:
        config = json.load(file)
except:
    print("config.json required")
    exit()

api_id = config["api_id"]
api_hash = config["api_hash"]

tg_id = 21348081
tg_hash = '841396171d9b111fa191dcdce768d223'

# smart_channels = [-1002130372878, -1002157259383]
smart_channels = []


async def listen_for_calls(pool=None):
    global smart_channels
    smart_channels += await useful_channels(pool=pool)
    # print(smart_channels)

    tg_names = [value for d in smart_channels for value in d.values()]
    tg_ids = [value for d in smart_channels for value in d]

    print(tg_names)
    # print(tg_ids)

    async with TelegramClient('trial', tg_id, tg_hash) as client:

        @client.on(events.NewMessage(chats=tg_ids, incoming=True))
        async def handle_new_message(event):
            source_channel = await event.message.get_sender()
            print(f'Received a new message from: {source_channel.title} at {datetime.datetime.now().strftime("%I:%M %p")}')

            messages = [event.message]
            addressTimeData = await extract_address_time_data(messages)

            # print(f'ADDRESS TIME DATA ----> {addressTimeData}')

            if len(addressTimeData.keys()) > 0:
                await insert_address_time_into_db(addressTimeData=addressTimeData,
                                                  channelId=source_channel.id, pool=pool)

        await client.start()
        await client.run_until_disconnected()


async def tg_listener():
    pool = await asyncpg.create_pool(dsn=pg_db_url)
    print(f'POOL CREATED at: {datetime.datetime.now().strftime("%I:%M %p")}')
    await listen_for_calls(pool=pool)


# asyncio.run(main())
