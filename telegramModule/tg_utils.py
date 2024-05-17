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


async def is_user_verified(userid: int, db_url: str = blocksight_db_url) -> bool:
    query = "SELECT is_verified from users where user_id = $1"

    verified = False
    conn = None
    try:
        conn = await asyncpg.connect(dsn=db_url)
    except Exception as e:
        print(f"Error {e} while connecting to db {db_url}")
        raise e
    
    try:
        verified = await conn.fetchval(query, int(userid))
    except Exception as e:
        print(f"Error {e} while checking is_verified from db for user {userid}")
        raise e
    if verified:
        await conn.close()
        return True
    
    intents = discord.Intents.default()
    intents.guilds = True
    intents.members = True
    client = discord.Client(intents=intents)

    try:
        await client.login(BOT_TOKEN)
        guild = await client.fetch_guild(BLOCKSIGHT_SERVER_ID)
        member = await guild.fetch_member(userid)
        if not member:
            return False
        verified_role = guild.get_role(VERIFIED_ROLE)

        if verified_role in member.roles:
            query = "UPDATE users SET is_verified = True WHERE user_id = $1"
            await conn.execute(query, userid)
            return True
        else:
            return False
    finally:
        await client.close()
        await conn.close()


if __name__ == "__main__":
    print(asyncio.run(is_user_verified(394387083189288961)))
