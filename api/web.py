from quart import Blueprint, request, make_response, jsonify, current_app

# from dbs.db_operations import fetch_wallet_leaderboard
from utils import token_required
from usersModule.user_utils import add_user_to_db, get_user_data, update_user_avatar, edit_user_data, create_referral_code, get_all_users
from walletVettingModule.wallet_vetting_utils import is_valid_wallet, fetch_tg_leaderboard, fetch_wallet_leaderboard
from core import analyse_wallet, vet_channel
import json, aiohttp, jwt, math
from time import time
web_blueprint = Blueprint('web', __name__)


# return {is-signed-up: bool, redirect_to: string}
@web_blueprint.route("/is-signed-up")
@token_required
async def handle_is_signed_up():
    token = request.headers.get('Access-Token')
    try:
        decoded_token = jwt.decode(token, key=current_app.bs_config["blockSight_secret"], algorithms=["HS256"])
    except:
        return "Unauthorized", 401
    
    user_data = {}
    try:
        user_data = await get_user_data(username=decoded_token['user_name'])
    except Exception as e:
        current_app.logger.error(f"error {e} while getting user data in handle_is_signed_up", f"request data: {request.url}, {request.args}")
        return "Internal Server Error", 500

    if not user_data['referral_used']:
        return {"is-signed-up": False, "redirect-to": "submit-ref-code"}
    elif not user_data['wallet']:
        return {"is-signed-up": False, "redirect-to": "wallet"}
    elif not user_data['email']:
        return {"is-signed-up": False, "redirect-to": "email"}
    else:
        return {"is-signed-up": True, "redirect-to": "dashboard"}

@web_blueprint.route("/discord-redirect")
async def handle_web_discord_redirect():
    r_uri = request.args.get("r-uri", type=str, default='http://localhost:3000/redirect')
    r_uri = r_uri.split("?")[0]
    
    try:
        with open('config.json', 'r') as file:
            config = json.load(file)
    except:
        current_app.logger.error("config.json required")
        return "Server Error", 500

    code = request.args.get("code")

    if not code:
        return "Missing parameter", 400

    data = {
        "client_id": config["discord_app_id"],
        "client_secret": config["discord_secret"],
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': r_uri
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
                    current_app.logger.error("error from discord: ", response.url, await response.text(), f"request data: {request.url}, {request.args}")
                    return "Internal Server Error", 500
                response = await response.json()
                access_token = response.get("access_token")
    except Exception as e:
        current_app.logger.error(f"Exception {e} while getting access token", f"request data: {request.url}, {request.args}")
        return "Internal Server Error", 500

    user_info = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://discord.com/api/v10/users/@me",
                                   headers={"Authorization": f"Bearer {access_token}"}) as response:
                if response.status != 200:
                    current_app.logger.error("Discord returned an error", response, response.status, f"request data: {request.url}, {request.args}")
                    return "Internal Server Error", 500

                response = await response.json()
                user_info = response
    except Exception as e:
        current_app.logger.error(f"Exception {e} while getting user info", f"request data: {request.url}, {request.args}")
        return "Internal Server Error", 500

    token = request.headers.get('Access-Token')
    if token:
        decoded_token = jwt.decode(token, key=config["blockSight_secret"], algorithms=["HS256"])
        if decoded_token["username"] == user_info["username"]:
            return {"message": "Success"}

    try:
        await add_user_to_db(username=user_info["username"], user_id=int(user_info["id"]), pool=current_app.pool)
        await update_user_avatar(username=user_info["username"], avatar=user_info["avatar"], pool=current_app.pool)
    except Exception as e:
        current_app.logger.error(f"Exception {e} while adding user info to db", f"request data: {request.url}, {request.args}")
        return "Internal Server Error", 500
    
    payload = {"username": user_info["username"], "user_id": user_info["id"], "created_at":int(time())}
    encoded_jwt = jwt.encode(payload, config["blockSight_secret"], algorithm="HS256")
    data = {"message":"Success", "access-token":encoded_jwt}
    resp = await make_response(jsonify(data))
    # resp.set_cookie(key='access-token', value=encoded_jwt, httponly=True, samesite="None") # does what it should, but doesn't work for some reason and I'm tired

    return resp

@web_blueprint.route("/get-user-info")
@token_required
async def web_get_user_info():
    token = request.headers.get('Access-Token')
    try:
        decoded_token = jwt.decode(token, key=current_app.bs_config["blockSight_secret"], algorithms=["HS256"])
    except:
        return "Unauthorized", 401
    
    user_name = decoded_token["username"] #continue
    try:
        user_data = await get_user_data(username=user_name)
        return user_data
    except Exception as e:
        current_app.logger.error(e, f"request data: {request.url}, {request.args}")
        return "Internal Server Error", 500

@web_blueprint.route("/update-user-data")
@token_required
async def web_update_user_data():
    col_name = request.args.get("col", type=str)
    data = request.args.get("data")
    if not col_name or not data:
        return "Bad Request: Missing required query param(s)", 400
    
    token = request.headers.get('Access-Token')
    try:
        decoded_token = jwt.decode(token, key=current_app.bs_config["blockSight_secret"], algorithms=["HS256"])
    except:
        return "Unauthorized", 401
    
    if col_name == 'wallet' and not is_valid_wallet(data):
        return "Invalid wallet submitted", 400

    try:
        if await edit_user_data(decoded_token["username"], data, col_name=col_name):
            return {"msg": "Success"}
        else:
            current_app.logger.error(f"Failed to edit {col_name} user data for {decoded_token["username"]}", f"request data: {request.url}, {request.args}")
            return "Invalid data submitted", 400
    except Exception as e:
        current_app.logger.error(f"Error {e} in web/update user data", f"request data: {request.url}, {request.args}", stack_info=True)
        return f"Internal Server Error while updating user data", 500


@web_blueprint.route("/create-ref-code")
@token_required
async def create_ref_code():
    code = request.args.get("code")

    if not code:
        return "Bad Request: Missing required query param - code", 400
    
    token = request.headers.get('Access-Token')
    try:
        decoded_token = jwt.decode(token, key=current_app.bs_config["blockSight_secret"], algorithms=["HS256"])
    except:
        return "Unauthorized", 401

    try:
        if await create_referral_code(decoded_token["username"], code):
            return {"msg": "Success"}
        else:
            raise Exception("create referral_code failed")
    except Exception as e:
        current_app.logger.error(f"Error {e} in web/update user data", f"request data: {request.url}, {request.args}", stack_info=True)
        return f"Internal Server Error while updating user data", 500


@web_blueprint.route("/analyse-wallet/<wallet_address>")
@token_required
async def web_analyse_wallet(wallet_address):
    return await analyse_wallet(wallet_address)

@web_blueprint.route("/analyse-tgt/<channel>")
@token_required
async def web_analyse_tg(channel):
    return await vet_channel(channel)


@web_blueprint.route("/get-wallets-leaderboard/<window>")
@token_required
async def web_wallets_leaderboard(window):
    page = request.args.get("page", default=1, type=int)

    try:
        sort_by = request.args.get("sort", type=str, default='win_rate')
        direction = request.args.get("direction", type=str, default='desc')
        total_pages = request.args.get("total_pages", type=int, default=15)
        ld_data = await fetch_wallet_leaderboard(current_app.pool, window, sort_by=sort_by, direction=direction)

        rows_per_page = math.floor(len(ld_data) / total_pages)

        page_data = ld_data[rows_per_page * (page-1) : rows_per_page * page]

        if page > 1:
            prev = f"/get-wallets-leaderboard/{window}?page={page-1}&total_pages={total_pages}"
        else:
            prev = "None"

        if page < total_pages:
            next = f"/get-wallets-leaderboard/{window}?page={page+1}&total_pages={total_pages}"
        else:
            next = "None"

        retv = {"page-data": page_data, "prev": prev, "next": next}


        return retv
    except Exception as e:
        current_app.logger.error(e, f"request data: {request.url}, {request.args}", stack_info=True)
        return f"Internal Server Error while fetching wallet leaderboard", 500


@web_blueprint.route("/get-tg-leaderboard/<window>")
# @token_required
async def tg_leaderboard(window):
    page = request.args.get("page", default=1, type=int)
    try:
        sort_by = request.args.get("sort", type=str, default='win_rate')
        direction = request.args.get("direction", type=str, default='desc')
        total_pages = request.args.get("total_pages", type=int, default=15)

        ld_data = await fetch_tg_leaderboard(current_app.pool, window, sort_by=sort_by, direction=direction)
        
        rows_per_page = math.floor(len(ld_data) / total_pages)

        page_data = ld_data[rows_per_page * (page-1) : rows_per_page * page]

        if page > 1:
            prev = f"/get-tg-leaderboard/{window}?page={page-1}&total_pages={total_pages}"
        else:
            prev = "None"

        if page < total_pages:
            next = f"/get-tg-leaderboard/{window}?page={page+1}&total_pages={total_pages}"
        else:
            next = "None"

        retv = {"page-data": page_data, "prev": prev, "next": next}
        return retv
    except Exception as e:
        current_app.logger.error(f"Error {e} while fetching tg leaderboard page {page}", f"request data: {request.url}, {request.args}")
        return "Internal Server Error", 500

@web_blueprint.route("/get-user-leaderboard")
@token_required
async def user_leaderboard():
    page = request.args.get("page", default=1, type=int)
    try:
        total_pages = request.args.get("total_pages", type=int, default=15)
        
        ld_data = await get_all_users(pool=current_app.pool)
        rows_per_page = math.floor(len(ld_data) / total_pages)

        if rows_per_page <= 0:
            return {"page_data": ld_data, "prev":None, "next":None}

        page_data = ld_data[rows_per_page * (page-1) : rows_per_page * page]

        if page > 1:
            prev = f"/get-user-leaderboard?page={page-1}&total_pages={total_pages}"
        else:
            prev = "None"

        if page < total_pages:
            next = f"/get-user-leaderboard?page={page+1}&total_pages={total_pages}"
        else:
            next = "None"

        retv = {"page-data": page_data, "prev": prev, "next": next}
        return retv
    except Exception as e:
        current_app.logger.error(f"Error {e} while fetching user leaderboard {page}", f"request data: {request.url}, {request.args}")
        return "Internal Server Error", 500