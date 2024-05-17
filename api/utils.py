from functools import wraps
from quart import request, jsonify
import jwt, json
from telegramModule.tg_utils import is_user_verified

config = {}

try:
    with open('config.json', 'r') as file:
        config = json.load(file)
except:
    print("config.json required")
    exit()


def token_required(f):
    @wraps(f)
    async def decorated(*args, **kwargs):
        token = None

        token = request.cookies.get('access-token')

        if not token:
            return jsonify({"msg": "Token is missing"}), 401

        try:
            jwt.decode(token, config['blockSight_secret'], algorithms=["HS256"])
        except:
            return jsonify({"msg": "Token is invalid"}), 401

        return await f(*args, **kwargs)

    return decorated

def token_and_verification_required(f):
    @wraps(f)
    async def decorated(*args, **kwargs):
        token = None

        token = request.cookies.get('access-token')

        if not token:
            return jsonify({"msg": "Token is missing"}), 401
        
        user_info = None
        try:
            user_info = jwt.decode(token, config['blockSight_secret'], algorithms=["HS256"])
        except:
            return jsonify({"msg": "Token is invalid"}), 401
        
        try:
            verified = await is_user_verified(user_info["user_id"])
            if not verified:
                return jsonify({"msg": "User unauthorized. Please verify on the Discord Server"}), 401
        except Exception as e:
            print(e)
            return jsonify({"msg": "Error while checking if user verified"}), 500
        


        return await f(*args, **kwargs)

    return decorated
