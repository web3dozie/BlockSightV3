from quart import Blueprint, request, make_response, jsonify, current_app

# from dbs.db_operations import fetch_wallet_leaderboard
from utils import token_required
from usersModule.user_utils import add_user_to_db, get_user_data, update_user_avatar, edit_user_data
from core import analyse_wallet, fetch_wallet_leaderboard
import json, aiohttp, jwt
from time import time

web_blueprint = Blueprint('web', __name__)


@web_blueprint.route("/discord-redirect")
async def handle_web_discord_redirect():
    try:
        with open('config.json', 'r') as file:
            config = json.load(file)
    except:
        print("config.json required")
        return "Server Error", 500

    code = request.args.get("code")

    if not code:
        return "Missing parameter", 400

    data = {
        "client_id": config["discord_app_id"],
        "client_secret": config["discord_secret"],
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': "http://localhost:3000/redirect"  # TODO Need to change this
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
                    return "Internal Server Error", 500
                response = await response.json()
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

    token = request.cookies.get('access-token')
    if token:
        decoded_token = jwt.decode(token, key=config["blockSight_secret"], algorithms=["HS256"])
        if decoded_token["user_name"] == user_info["username"]:
            return {"message": "Success"}

    try:
        await add_user_to_db(username=user_info["username"], user_id=int(user_info["id"]), pool=current_app.pool)
        await update_user_avatar(username=user_info["username"], avatar=user_info["avatar"], pool=current_app.pool)
    except Exception as e:
        print(f"Exception {e} while adding user info to db")
        return "Internal Server Error", 500
    
    payload = {"user_name": user_info["username"], "user_id": user_info["id"], "created_at":int(time())}
    encoded_jwt = jwt.encode(payload, config["blockSight_secret"], algorithm="HS256")
    data = {"message":"Success", "access-token":encoded_jwt}
    resp = await make_response(jsonify(data))
    # resp.set_cookie(key='access-token', value=encoded_jwt, httponly=True, samesite="None") # does what it should, but doesn't work for some reason and I'm tired

    return resp

@web_blueprint.route("/get-user-info/")
@token_required
async def web_get_user_info():
    token = request.cookies.get('access-token')
    try:
        decoded_token = jwt.decode(token, key=current_app.bs_config["blockSight_secret"], algorithms=["HS256"])
    except:
        return "Unauthorized", 401
    
    user_name = decoded_token["user_name"] #continue
    try:
        user_data = await get_user_data(username=user_name)
        print(user_data)
        return user_data
    except Exception as e:
        current_app.logger.error(e)
        return "Internal Server Error", 500



@web_blueprint.route("/analyse-wallet/<wallet_address>")
@token_required
async def web_analyse_wallet(wallet_address):
    return await analyse_wallet(wallet_address)


@web_blueprint.route("/get-wallets-leaderboard")
@token_required
async def web_wallets_leaderboard():
    try:
        retv = dict()
        retv["30d"] = await fetch_wallet_leaderboard(current_app.pool, '30d')
        retv["7d"] = await fetch_wallet_leaderboard(current_app.pool, '7d')
        retv["3d"] = await fetch_wallet_leaderboard(current_app.pool, '3d')

        return retv
    except Exception as e:
        current_app.logger.error(e, stack_info=True)
        return f"Internal Server Error while fetching wallet leaderboard", 500
