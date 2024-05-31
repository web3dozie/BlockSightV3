# from flask import Blueprint
from quart import Blueprint
from telegramModule.tg_utils import get_userid_from_tg_id
from usersModule.user_utils import is_user_verified

telegram_blueprint = Blueprint('telegram', __name__)

"""
Should return a dict like this: {
  verified: True,
  registered: True
}
"""


@telegram_blueprint.route('/check-id/<telegram_id>')
async def check_id(telegram_id):
    if not telegram_id:
        return "Bad request", 400

    try:
        userid = await get_userid_from_tg_id(telegram_id)
        if not userid:
            return {"registered": False, "verified": False}

        verified = await is_user_verified(userid)

        return {"registered": True, "verified": verified}

    except Exception as e:
        print(f"Error {e} while checking if id {telegram_id} is registered/verified")
        return "Internal Server Error", 500


@telegram_blueprint.route('/check-id/<telegram_id>')
async def check_id(telegram_id):
    if not telegram_id:
        return "Bad request", 400

    try:
        userid = await get_userid_from_tg_id(telegram_id)
        if not userid:
            return {"registered": False, "verified": False}

        verified = await is_user_verified(userid)

        return {"registered": True, "verified": verified}

    except Exception as e:
        print(f"Error {e} while checking if id {telegram_id} is registered/verified")
        return "Internal Server Error", 500


