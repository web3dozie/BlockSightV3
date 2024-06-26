from functools import wraps
from quart import request, jsonify
import jwt, json, datetime
from usersModule.user_utils import is_user_verified

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

        token = request.headers.get('Access-Token')

        if not token:
            return jsonify({"msg": "Access Token is missing"}), 401

        decoded_jwt = None
        try:
            decoded_jwt = jwt.decode(token, config['blockSight_secret'], algorithms=["HS256"])
        except:
            return jsonify({"msg": "Access Token is invalid"}), 401
        
        token_dt = datetime.datetime.fromtimestamp(decoded_jwt["created_at"])

        now = datetime.datetime.now()

        if (now - token_dt).days > 7:
            return jsonify({"msg": "Token expired"}), 401


        return await f(*args, **kwargs)

    return decorated

def token_and_verification_required(f):
    @wraps(f)
    async def decorated(*args, **kwargs):
        token = None

        token = request.headers.get('Access-Token')

        if not token:
            return jsonify({"msg": "Access Token is missing"}), 401
        
        decoded_jwt = None
        try:
            decoded_jwt = jwt.decode(token, config['blockSight_secret'], algorithms=["HS256"])
        except:
            return jsonify({"msg": "Access Token is invalid"}), 401
        
        token_dt = datetime.datetime.fromtimestamp(decoded_jwt["created_at"])

        now = datetime.datetime.now()

        if (now - token_dt).days > 7:
            return jsonify({"msg": "Access Token expired"}), 401

        
        try:
            verified = await is_user_verified(decoded_jwt["user_id"])
            if not verified:
                return jsonify({"msg": "User unauthorized. Please verify on the Discord Server"}), 401
        except Exception as e:
            print(e)
            return jsonify({"msg": "Error while checking if user verified"}), 500
        


        return await f(*args, **kwargs)

    return decorated
