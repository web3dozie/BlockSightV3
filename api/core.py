import re

from dbs.db_operations import get_tx_list
from walletVettingModule.wallet_vetting_utils import process_wallet, determine_wallet_grade, determine_tg_grade, \
    generate_trader_message, is_valid_wallet, fetch_wallet_leaderboard, is_valid_channel
from metadataAndSecurityModule.metadataUtils import get_data_from_helius
from priceDataModule.price_utils import is_win_trade
from telegramModule.vet_tg_channel import vetChannel
from usersModule.user_utils import add_user_to_db
import aiohttp, json

from quart import request, jsonify, make_response, Blueprint, current_app

core_blueprint = Blueprint('core', __name__)


@core_blueprint.route("/analyse-wallet/<wallet_address>")
async def analyse_wallet(wallet_address):
    window = request.args.get("window", default=30, type=int)
    fmt = request.args.get("format", default=False, type=bool)
    include_txs = request.args.get("include_txs", default=False, type=bool)

    if not is_valid_wallet(wallet_address):
        return "Invalid wallet", 400

    try:
        wallet_data = await process_wallet(wallet_address, window, pool=current_app.pool)

        # Optionally fetch the transaction list
        if include_txs:
            tx_list = await get_tx_list(wallet_address, pool=current_app.pool)
            wallet_data['tx_list'] = tx_list

        if not fmt:
            return wallet_data

        grades = determine_wallet_grade(wallet_data['trades'], float(wallet_data['win_rate']),
                                        float(wallet_data['avg_size']), float(wallet_data['pnl']),
                                        window=window)

        trader_message = generate_trader_message(grades)
        return {"stats": wallet_data, "grades": grades, "msg": trader_message,
                "tx_list": wallet_data.get('tx_list', [])}
    except Exception as e:
        current_app.logger.error(e, stack_info=True)
        return f"Internal Server Error: {str(e)}", 500


@core_blueprint.route("/verify-token-mint/<token_mint>")
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
    except Exception as e:
        return f"Internal Server Error: {e}", 500


@core_blueprint.route("/is-win-trade")
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


@core_blueprint.route("/vet-tg-channel/<tg_channel>")
async def vet_channel(tg_channel):
    print('API STARTED')
    window = request.args.get("window", default=30, type=int)
    fmt = request.args.get("format", default=False, type=bool)

    if not is_valid_channel(tg_channel):
        return "Invalid wallet", 400

    match = re.search(r't\.me/([^/]+)', tg_channel)
    tg_channel = match.group(1) if match else tg_channel

    try:
        retv = await vetChannel(tg_channel, window=window, pool=current_app.pool)

        if not fmt:
            return retv

        grades = determine_tg_grade(retv["trade_count"], retv["win_rate"], retv["time_window"])
        return {"stats": retv, "grades": grades}
    except Exception as e:
        print(f"Error {e} while vetting channel: {tg_channel} via api")
        return make_response(jsonify({"status": "Internal Server Error", "message": str(e)}), 500)


@core_blueprint.route("/discord-redirect")
async def handle_discord_redirect():

    try:
        with open('config.json', 'r') as file:
            config = json.load(file)
    except Exception as e:
        print("config.json required")
        return f"Server Error: {e}", 500

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


@core_blueprint.route("/get-wallets-leaderboard/<window>")
async def wallets_leaderboard(window):
    return await fetch_wallet_leaderboard(current_app.pool, window)
