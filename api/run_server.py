import asyncpg, json
from aiohttp import ClientSession

from dbs.db_operations import pg_db_url
from telegramModule.tg_client_pooling import TelegramClientPool
from telegram import telegram_blueprint
from web import web_blueprint
from core import core_blueprint

from quart import Quart, request

app = Quart(__name__)
app.register_blueprint(telegram_blueprint, url_prefix="/telegram")
app.register_blueprint(web_blueprint, url_prefix='/web')
app.register_blueprint(core_blueprint, url_prefix='/core')


@app.before_serving
async def create_pool_and_config():
    config = {}

    try:
        with open('config.json', 'r') as file:
            config = json.load(file)
            app.bs_config = config
    except:
        print("config.json required")
        return "Server Error", 500

    # Create Pool
    app.pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=300, max_size=800, max_inactive_connection_lifetime=1000, command_timeout=500)

    '''
    # Create Discord Client
    intents = discord.Intents.default()
    intents.messages = True
    intents.guilds = True
    intents.members = True
    intents.dm_messages = True
    intents.message_content = True
    app.discord_client = discord.Client(intents=intents)
    await app.discord_client.start(BOT_TOKEN)
    '''

    # Create TG Client Pool
    app.tg_pool = TelegramClientPool(api_hash='841396171d9b111fa191dcdce768d223', api_id=21348081)

    # Create AIOHTTP session # TODO Modify Params
    app.session = ClientSession()


@app.after_request
def add_cors_headers(response):
    # TODO CHANGE THIS IN PRODUCTION!
    white = ['http://localhost:3000','https://block-sight.vercel.app']
    r = request.headers.get('Origin')
    if r in white:
        response.headers.add('Access-Control-Allow-Origin', r)
        response.headers.add('Access-Control-Allow-Credentials', 'true')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        response.headers.add('Access-Control-Allow-Headers', 'Cache-Control')
        # response.headers.add('Access-Control-Allow-Headers', 'X-Requested-With')
        response.headers.add('Access-Control-Allow-Headers', 'Access-Token')
        response.headers.add('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE')
    
    return response


if __name__ == '__main__':
    app.run(debug=True)
