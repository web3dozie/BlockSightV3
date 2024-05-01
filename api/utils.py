from functools import wraps
from quart import request, jsonify
import jwt, json

config = {}

try:
    with open('config.json', 'r') as file:
        config = json.load(file)
except:
    print("config.json required")
    exit()


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        token = request.cookies.get('access-token')

        if not token:
            return jsonify({"msg": "Token is missing"}), 401

        try:
            jwt.decode(token, config['blockSight_secret'], algorithms=["HS256"])
        except:
            return jsonify({"msg": "Token is invalid"}), 401

        return f(*args, **kwargs)

    return decorated
