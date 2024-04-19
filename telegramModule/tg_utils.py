import json, asyncpg

config = {
}

try:
    with open('config.json', 'r') as file:
        config = json.load(file)
except:
    print("config.json required")
    exit()

dex_api = config["dexApi"]
blocksight_db_url = config["blockSightDB"]

async def get_username_from_tg_id(tg_id, db_url=blocksight_db_url) -> str|None:
    query = "select username from users where telegram_id = $1"

    try:
        conn = await asyncpg.connect(dsn=blocksight_db_url)
        username = await conn.fetchval(query, tg_id)
    except Exception as e:
        print(f"Error {e} while getting username for tg id {tg_id}")
        raise e
    
    if not username:
        return None
    else:
        return username