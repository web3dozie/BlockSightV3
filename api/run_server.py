import asyncpg, json

from dbs.db_operations import pg_db_url
from telegram import telegram_blueprint
from web import web_blueprint
from core import core_blueprint

from quart import Quart

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
    
    app.pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=300, max_size=800, max_inactive_connection_lifetime=1000, command_timeout=500)


@app.after_request
def add_cors_headers(response):
    # TODO CHANGE THIS IN PRODUCTION!
    response.headers['Access-Control-Allow-Origin'] = 'http://localhost:3000'
    response.headers['Access-Control-Allow-Credentials'] = 'true'
    return response


if __name__ == '__main__':
    app.run(debug=True)
