import json, asyncpg, discord, asyncio
from usersModule.discord_bot_dms import BLOCKSIGHT_SERVER_ID, VERIFIED_ROLE, BOT_TOKEN

config = {}

try:
    with open('config.json', 'r') as file:
        config = json.load(file)
except:
    print("config.json required")
    exit()

dex_api = config["dexApi"]
blocksight_db_url = config["blockSightDB"]


async def get_userid_from_tg_id(tg_id: int, db_url: str = blocksight_db_url):
    query = "select user_id from users where telegram_id = $1"

    try:
        conn = await asyncpg.connect(dsn=db_url)
        userid = await conn.fetchval(query, int(tg_id))
    except Exception as e:
        print(f"Error {e} while getting username for tg id {tg_id}")
        raise e
    finally:
        await conn.close()

    if not userid:
        return None
    else:
        return userid



