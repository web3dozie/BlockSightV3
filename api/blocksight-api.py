import asyncpg

from dbs.db_operations import pg_db_url
from walletVettingModule.wallet_vetting_utils import process_wallet, determine_wallet_grade, determine_tg_grade, \
    generate_trader_message, is_valid_wallet
from metadataAndSecurityModule.metadataUtils import get_data_from_helius
from priceDataModule.price_utils import is_win_trade
from telegramModule.vet_tg_channel import vetChannel
from usersModule.user_utils import add_user_to_db
from telegram import telegram_blueprint
import aiohttp, json

# from flask import Flask, request, jsonify, make_response
from quart import Quart, request, jsonify, make_response

app = Quart(__name__)
app.register_blueprint(telegram_blueprint, url_prefix="/telegram")


@app.before_serving
async def create_pool():
    app.pool = await asyncpg.create_pool(dsn=pg_db_url)


@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


@app.route("/core/analyse-wallet/<wallet_address>")
async def analyse_wallet(wallet_address):
    window = request.args.get("window", default=30, type=int)
    format = request.args.get("format", default=False, type=bool)
    if not is_valid_wallet(wallet_address):
        return "Invalid wallet", 400
    try:
        wallet_data = await process_wallet(wallet_address, window, pool=app.pool)
        if not format:
            return wallet_data

        grades = determine_wallet_grade(wallet_data['trades'], float(wallet_data['win_rate']),
                                        float(wallet_data['avg_size']), float(wallet_data['pnl']),
                                        window=window)

        trader_message = generate_trader_message(grades)
        return {"stats": wallet_data, "grades": grades, "msg": trader_message}
    except Exception as e:
        return f"Internal Server Error: {str(e)}", 5000


@app.route("/core/verify-token-mint/<token_mint>")
async def verify_token_mint(token_mint):
    # might add some validation logic here
    if not token_mint:
        return "bad request", 400
    try:
        token_data = await get_data_from_helius(token_mint)
        if token_data:
            return {"result": "valid"}
        else:
            return {"result": "invalid"}
    except Exception:
        return "Internal Server Error", 500


@app.route("/core/is-win-trade")
async def api_is_win_trade():
    token = request.args.get("token")
    timestamp = request.args.get("timestamp")

    if not token or not timestamp:
        return "Missing parameter", 400

    try:
        retv = await is_win_trade(token_mint=token, timestamp=int(timestamp))
        return {"result": retv}
    except Exception as e:
        print(f"Error while checking trade {e}")
        return "Internal Server Error", 500


@app.route("/core/vet-tg-channel/<tg_channel>")
async def vet_channel(tg_channel):
    print('API STARTED')
    window = request.args.get("window", default=30, type=int)
    format = request.args.get("format", default=False, type=bool)

    if not tg_channel:
        return "bad request", 400

    print('VET STARTED')
    try:
        retv = await vetChannel(tg_channel, window=window)
        print('VET SUCCEEDED')
        if not format:
            return retv

        grades = determine_tg_grade(retv["trade_count"], retv["win_rate"], retv["time_window"])
        return {"stats": retv, "grades": grades}
    except Exception as e:
        print('VET FAILED')
        print(f"Error {e} while vetting channel {tg_channel}")
        return make_response(jsonify({"status": "Internal Server Error", "message": str(e)}), 500)


@app.route("/core/discord-redirect")
async def handle_discord_redirect():
    config = {}

    try:
        with open('config.json', 'r') as file:
            config = json.load(file)
    except:
        print("config.json required")
        return "Server Error", 500

    code = request.args.get("code")
    tg_id = request.args.get("state")

    if not code or not tg_id:
        return "Missing parameter", 400

    data = {
        "client_id": config["discord_app_id"],
        "client_secret": config["discord_secret"],
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': "http://localhost:5000/core/discord-redirect"
    }

    data = aiohttp.FormData(data)

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    access_token = None

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://discord.com/api/oauth2/token", headers=headers, data=data) as response:
                if response.status != 200:
                    print(response.url, response.content_type, await response.text())
                    # print(f"Discord returned an error {response} with status {response.status}")
                    return "Internal Server Error", 500
                response = await response.json()
                # print("finally", response)
                access_token = response.get("access_token")
    except Exception as e:
        print(f"Exception {e} while getting access token")
        return "Internal Server Error", 500

    user_info = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://discord.com/api/v10/users/@me",
                                   headers={"Authorization": f"Bearer {access_token}"}) as response:
                if response.status != 200:
                    print("Discord returned an error", response, response.status)
                    return "Internal Server Error", 500

                response = await response.json()
                print(response)
                user_info = response
    except Exception as e:
        print(f"Exception {e} while getting user info")
        return "Internal Server Error", 500

    try:
        await add_user_to_db(username=user_info["username"], user_id=int(user_info["id"]), tg_id=int(tg_id))
    except Exception as e:
        print(f"Exception {e} while adding user info to db")
        return "Internal Server Error", 500

    return ("<p>You've signed up successfully! <a href='https://discord.gg/blocksight'>Join the discord server</a>"
            " and verify to use the Telegram bot</p>")


if __name__ == '__main__':
    app.run(debug=True)
