import json
from telethon import TelegramClient, events
from telethon.types import Channel
from vet_tg_channel import extract_address_time_data, insert_address_time_into_db

config = {}

try:
    with open('config.json', 'r') as file:
        config = json.load(file)
except:
    print("config.json required")
    exit()

api_id = config["api_id"]
api_hash = config["api_hash"]


async def listen_for_calls():
    async with TelegramClient('anon', api_id, api_hash) as client:
        @client.on(events.NewMessage(chats=Channel))
        async def handle_new_message(event):
            print('Received a new message:', event.message.text)
            messages = [event.message]
            addressTimeData = await extract_address_time_data(messages)

            if len(addressTimeData.keys()) > 0:
                await insert_address_time_into_db(addressTimeData=addressTimeData, channelId=event.message.peer_id)

        client.start()
        client.run_until_disconnected()
