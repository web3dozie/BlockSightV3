from quart import Blueprint, request, make_response, jsonify
from utils import token_required
from usersModule.user_utils import add_user_to_db
from core import analyse_wallet
import json, aiohttp, jwt

web_blueprint = Blueprint('web', __name__)


@web_blueprint.route("/discord-redirect")
async def handle_web_discord_redirect():
    config = {}

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
        if decoded_token["user_id"] == user_info["username"]:
            return {"message": "Success"}

    try:
        await add_user_to_db(username=user_info["username"], user_id=int(user_info["id"]))
    except Exception as e:
        print(f"Exception {e} while adding user info to db")
        return "Internal Server Error", 500
    
    payload = {"user_id": user_info["username"]}
    encoded_jwt = jwt.encode(payload, config["blockSight_secret"], algorithm="HS256")
    data = {"message":"Success", "access-token":encoded_jwt}
    resp = await make_response(jsonify(data))
    # resp.set_cookie(key='access-token', value=encoded_jwt, httponly=True, samesite="None") # does what it should, but doesn't work for some reason and I'm tired

    return resp

# @web_blueprint.route("/get-user-info/")
# @token_required
# async def web_get_user_info():
#     config = {}

#     try:
#         with open('config.json', 'r') as file:
#             config = json.load(file)
#     except:
#         print("config.json required")
#         return "Server Error", 500
#     token = request.cookies.get('access-token')
#     # unnecessary
#     # if not token:
#     #     resp = await make_response(jsonify({"message": "Missing user token"}))
#     #     resp.status ="401"
#     #     return resp

#     decoded_token = jwt.decode(token, key=config["blockSight_secret"], algorithms=["HS256"])
#     user_id = decoded_token["user_id"] #continue


@web_blueprint.route("/analyse-wallet/<wallet_address>")
@token_required
async def web_analyse_wallet(wallet_address):
    return await analyse_wallet(wallet_address)


@web_blueprint.route("/get-wallets-leaderboard")
@token_required
async def web_wallets_leaderboard(wallet_address):
    return await fetch_wallet_leaderboard()