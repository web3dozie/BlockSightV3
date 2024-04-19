from flask import Blueprint
from telegramModule.tg_utils import get_username_from_tg_id

telegram_blueprint = Blueprint('telegram', __name__)

@telegram_blueprint.route('/is-id-registered/<telegram-id>')
async def check_id(telegram_id):
  if not telegram_id:
    return "Bad request", 400
  
  retv = False
  try:
    username = await get_username_from_tg_id(telegram_id)
    if not username:
      return {"result": False}
    else:
      return {"result": True}
  except Exception as e:
    return f"Error {e} while checking if id {telegram_id} is registered"