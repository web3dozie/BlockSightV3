"""
PRIMARY LOGIN METHOD WILL BE VIA A DISCORD ACCOUNT
"""
import random, re, asyncpg, discord
import time

from pprint import pprint

import aiohttp
from discord import Embed
from datetime import datetime

from dbs.db_operations import wallet_exists, channel_exists
from walletVettingModule.wallet_vetting_utils import is_valid_wallet, determine_wallet_grade, generate_trader_message, \
    determine_tg_grade, generate_tg_message, get_sol_price

import json, asyncpg, discord, asyncio

BOT_TOKEN = 'MTIwNDgyMDc3MjM1OTc2NjA2Ng.GRhhu0.3zkLNrB7dzcPcou_309mKRQ0WWDFbFqwg2pjlg'
BLOCKSIGHT_SERVER_ID = 1101255371210887258
VERIFIED_ROLE = 1200907085320294460

BETA_ROLE = 1184124534366933143
BETA_PRIME_ROLE = 1192053087586762803
HONORARY_ROLE = 1184125377124257842

blocksight_api = "http://localhost:5000"
pg_db_url = 'postgresql://bmaster:BlockSight%23Master@109.205.180.184:6432/blocksight'


async def login(user: discord.User, db_path=pg_db_url):
    # check if they are in db
    # if they are proceed
    # else (call signup)
    pass


async def get_max_role(client: discord.Client, user: discord.User):
    guild = client.get_guild(BLOCKSIGHT_SERVER_ID)
    member = await guild.fetch_member(user.id)

    role_verified = guild.get_role(VERIFIED_ROLE)
    role_beta = guild.get_role(BETA_ROLE)
    role_beta_prime = guild.get_role(BETA_PRIME_ROLE)  # Corrected variable
    role_honorary = guild.get_role(HONORARY_ROLE)  # Corrected variable

    # Define a list of roles in the order of their priority
    priority_roles = [role_honorary, role_beta_prime, role_beta, role_verified]

    # Find the highest priority role the member has
    max_role = next((role for role in priority_roles if role in member.roles), None)

    return max_role.name if max_role else None


async def update_max_role(username: str, role: str, db_path: str = pg_db_url, pool=None):
    conn = await asyncpg.connect(db_path)
    try:
        await conn.execute(
            """
            INSERT INTO users (username, max_role) VALUES ($1, $2)
            ON CONFLICT (username) DO UPDATE
            SET max_role = EXCLUDED.max_role
            """,
            username, role
        )
    finally:
        # Ensure the connection is always closed
        await conn.close()


async def assign_role(client, user: discord.User, role_id: int):
    """
    Assigns a role to a user in a specified guild.

    Parameters:
    - client: The discord_blueprints.py client or bot instance.
    - user: discord.User object representing the user to whom the role will be assigned.
    - role_id: The ID of the role to assign to the user.
    """

    guild_id = 1101255371210887258

    # Get the guild object using the guild ID
    guild = client.get_guild(guild_id)
    if guild is None:
        print(f"Guild with ID {guild_id} not found.")
        return 0

    # Get the member object from the user ID
    member = guild.get_member(user.id)
    if member is None:
        print(f"User {user} is not a member of guild {guild_id}.")
        return 0

    # Get the role object using the role ID
    role = guild.get_role(role_id)
    if role is None:
        print(f"Role with ID {role_id} not found in guild {guild_id}.")
        return 0

    # Assign the role to the user
    try:
        await member.add_roles(role)
        print(f"Successfully assigned role {role.name} to user {user.name}.")
        return 1
    except discord.HTTPException as e:
        print(f"Failed to assign role: {e}")
        return 0


async def discord_command_executor(text: str, user, client: discord.Client, message: discord.Message, pool=None):
    """
        This function executes commands and returns responses for sending
        :param pool:
        :param message:
        :param client:
        :param user:
        :param text:
        :return: content, embed
    """
    # .help command
    if text == '.help'.lower():

        embed = Embed(title='Command List', color=0xc8a2c8, timestamp=datetime.now(),
                      description='Contains information on supported bot commands', )

        embed.set_footer(text=f"BlockSight | Made with 💜 by @web3dozie",
                         icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                  "1189235897288372244/BSL_Gradient.png")

        embed.add_field(name=".help", value='Shows the command list', inline=False)
        embed.add_field(name="", value='', inline=True)
        embed.add_field(name=".my_info", value='Shows your information', inline=False)
        embed.add_field(name="", value='', inline=False)
        embed.add_field(name=".link <data to link> <value to link>", value='Link your data to your account\n'
                                                                           'BE CAREFUL! ENTRIES cAN NOT BE EDITED!\n\n'
                                                                           'Examples:\n\n'
                                                                           '> .link email blocksight@gmail.com\n'
                                                                           '> .link telegram @BlockSight\n'
                                                                           '> .link twitter @BlockSightData\n'
                                                                           '> .link wallet 6RTJyh89djPm...\n\n',
                        inline=True)
        embed.add_field(name="", value='', inline=False)
        embed.add_field(name=".use_code <CODE>", value='Use a referral code', inline=False)
        embed.add_field(name="", value='', inline=False)
        embed.add_field(name=".create_code <CODE>", value='Create a referral code',
                        inline=False)
        embed.add_field(name="", value='', inline=False)
        embed.add_field(name=".scan <wallet_address> || .scan <@public_tg>",
                        value='Scan any wallet or Public Telegram Caller to see its detailed stats and ratings.\n'
                              'Use this to find smart wallets, check your personal stats or rate a caller.\n'
                              'This is a very computationally intensive task, so allow up to 10 minutes per scan. '
                              'Scans consume credits.',
                        inline=False)

        content = ''

        return content, embed

        # ??my_info command

    elif text.startswith('.my_info'):
        info = await get_user_data(user.name, pool=pool)

        embed = Embed(title=f"{user.name}'s Info.", color=0xc8a2c8)
        embed.set_footer(text=f"BlockSight",
                         icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                  "1189235897288372244/BSL_Gradient.png")

        max_role = await get_max_role(client, user)

        await update_max_role(user.name, max_role, pool=pool)
        embed.set_thumbnail(url=user.avatar)

        embed.add_field(name=f'Wallet:', value=info['wallet'])
        embed.add_field(name=f'Max Role:', value=info['max_role'])
        embed.add_field(name=f'Email:', value=info['email'])
        embed.add_field(name=f'Twitter:', value=info['twitter'])
        embed.add_field(name=f'Telegram:', value=info['telegram'])
        embed.add_field(name="", value='', inline=False)
        embed.add_field(name=f'Points:', value=f'{info['points']}')
        embed.add_field(name=f'Referral Code:', value=info['referral_code'])
        embed.add_field(name=f'Referrals:', value=info['referrals'])
        embed.add_field(name="", value='', inline=False)
        embed.add_field(name=f'Credit Balance:', value=info['credits'])
        embed.add_field(name="", value='', inline=False)
        embed.add_field(name=f'Current Tier:', value=info['current_plan'])
        embed.add_field(name=f'Tier Expires In:', value=f'<t:{info['plan_end_date']}:R>')

        return '', embed

    # ??link_wallet command
    elif text.startswith('.link '):
        def invalid_embed(inv_embed):
            inv_embed.title = 'Invalid Format'
            inv_embed.set_footer(text=f"BlockSight",
                                 icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                          "1189235897288372244/BSL_Gradient.png")
            inv_embed.add_field(name=".link <data to link> <value to link>",
                                value='Use the right format\n'
                                      'BE CAREFUL! ENTRIES CAN NOT BE EDITED!\n\n'
                                      'Examples:\n\n'
                                      '> .link email blocksight@gmail.com\n'
                                      '> .link telegram @BlockSight\n'
                                      '> .link twitter @BlockSightData\n'
                                      '> .link wallet 6RTJyh89djPm...\n\n',
                                inline=True)

            return inv_embed

        def valid_embed(val_embed, col):
            val_embed.title = f'Successfully linked your {col}'
            val_embed.set_footer(text=f"BlockSight",
                                 icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                          "1189235897288372244/BSL_Gradient.png")
            return val_embed

        def no_edits_embed(val_embed, col):
            val_embed.title = f'No Edits Allowed'
            val_embed.set_footer(text=f"BlockSight",
                                 icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                          "1189235897288372244/BSL_Gradient.png")
            val_embed.add_field(name=f"You already linked a {col_name}",
                                value='',
                                inline=True)
            return val_embed

        link_embed = Embed(color=0xc8a2c8)
        split_text = text.split()

        if len(split_text) != 3:
            link_embed = invalid_embed(link_embed)
            return '', link_embed

        col_name = split_text[1]
        col_data = text.split()[2]

        def check_data(column_name, column_data):
            regex_map = {
                'email': r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
                'telegram': r"@[a-zA-Z_][a-zA-Z0-9_]*",
                'twitter': r"@[a-zA-Z_][a-zA-Z0-9_]*",
                'wallet': r'[1-9A-HJ-NP-Za-km-z]{32,44}'
            }

            # Check if the column name is a key in the regex_map
            if column_name in regex_map:
                # Check if the column data matches the regex for that column
                if re.match(regex_map[column_name], column_data):
                    return True

            return False

        valid_data = check_data(col_name, col_data)

        if valid_data:
            updated = await edit_user_data(user.name, col_data, col_name, pool=pool)

            if updated:
                link_embed = valid_embed(link_embed, col_name)
                await adjust_points(user.name, 5, pool=pool)
                return '', link_embed
            else:
                link_embed = no_edits_embed(link_embed, col_name)
                return '', link_embed

        else:
            link_embed = invalid_embed(link_embed)
            return '', link_embed

    # ??use_code command
    elif text.startswith('.use_code '):
        code = text.split()[1].upper()

        def success_embed(val_embed, code_, valid):
            if valid:
                val_embed.title = f'Successfully used code: {code_}'
                val_embed.description = f'You now have full beta access to BlockSight\'s tools'
                val_embed.set_footer(text=f"BlockSight",
                                     icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                              "1189235897288372244/BSL_Gradient.png")
            else:
                val_embed.title = f'Failed to use code: {code_}'
                val_embed.set_footer(text=f"BlockSight",
                                     icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                              "1189235897288372244/BSL_Gradient.png")
            return val_embed

        use_code_embed = Embed(color=0xc8a2c8)

        successful = await use_referral_code(user, code, client, pool=pool)

        use_code_embed = success_embed(use_code_embed, code, successful)

        return '', use_code_embed

    # ??create_code command
    elif text.startswith('.create_code '):
        code = text.split()[1].upper()

        def success_embed(val_embed, code_, valid):
            if valid:
                val_embed.title = f'Successfully created code: {code_}'
                val_embed.set_footer(text=f"BlockSight",
                                     icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                              "1189235897288372244/BSL_Gradient.png")
            else:
                val_embed.title = f'Failed to create code: {code_}'
                val_embed.description = 'Codes must be unique and should be 3 - 15 characters long.'
                val_embed.set_footer(text=f"BlockSight",
                                     icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                              "1189235897288372244/BSL_Gradient.png")
            return val_embed

        create_code_embed = Embed(color=0xc8a2c8)

        successful = await create_referral_code(user.name, code, pool=pool)

        create_code_embed = success_embed(create_code_embed, code, successful)

        return '', create_code_embed

    elif text.startswith('.scan '):
        split_text = text.split()

        if len(split_text) == 2:
            data_to_scan = split_text[1]
            window = 30
        else:
            data_to_scan = split_text[2]
            window = split_text[1]

        scan_embed = Embed(color=0xc8a2c8, title=f'Starting scan for {data_to_scan[0:5]}...',
                           description='Please be patient')
        scan_embed.set_footer(text=f"BlockSight",
                              icon_url="https://cdn.discordapp.com/attachments/"
                                       "1184131101782970398/1189235897288372244/BSL_Gradient.png")

        await adjust_credits(user.name, 5, pool=pool)  # subtract 5 credits

        if is_valid_wallet(data_to_scan):
            scan_message = await message.channel.send(content='', embed=scan_embed)

            def make_wallet_scan_embed(wallet_data):
                grades = determine_wallet_grade(wallet_data['trades'], float(wallet_data['win_rate']),
                                                float(wallet_data['avg_size']), float(wallet_data['pnl']),
                                                window=window)

                wallet_scan_embed = Embed(color=0xc8a2c8, title=f"{wallet_data['wallet'][0:7]}...'s  {window}D Summary",
                                          description='In-Depth Breakdown')
                try:
                    wallet_scan_embed.add_field(name=f'Overall Rank: {grades['overall_grade']}',
                                                value=f'{generate_trader_message(grades)}')
                    wallet_scan_embed.add_field(name="", value='', inline=False)
                except Exception as e:
                    print(e)
                    raise e

                wallet_scan_embed.add_field(name=f'Trading Frequency: ({grades['trades_grade']})',
                                            value=f'{round((wallet_data['trades'] / window), 2)} trades per day')

                wallet_scan_embed.add_field(name=f'Win Rate: {grades['win_rate_grade']}',
                                            value=f'{wallet_data['win_rate']}% '
                                                  f'(hit 2.5x in 4 days or less)')

                try:
                    wallet_scan_embed.add_field(name=f'Average Size: {grades['size_grade']}',
                                                value=f'Apes {round((float(wallet_data['avg_size'])), 2)}$ per trade ')
                except Exception as e:
                    print(e)
                    raise e

                wallet_scan_embed.add_field(name=f'PnL: {grades['pnl_grade']}',
                                            value=f'Realized {wallet_data['pnl']}$ in the last {window} days')

                wallet_scan_embed.add_field(name=f'Last updated:',
                                            value=f'{f'<t:{wallet_data['last_checked']}:R>'}')

                wallet_scan_embed.set_footer(text=f"BlockSight",
                                             icon_url="https://cdn.discordapp.com/attachments/"
                                                      "1184131101782970398/1189235897288372244/BSL_Gradient.png")

                return wallet_scan_embed

            if not await wallet_exists(data_to_scan, pool=pool):
                await adjust_points(user.name, 20, pool=pool)

            summary = {}
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{blocksight_api}/core/analyse-wallet/{data_to_scan}",
                                       params={'window': window}) as response:
                    if response.status == 200:
                        summary = await response.json()

                        scan_embed = make_wallet_scan_embed(summary)
                        await scan_message.edit(embed=scan_embed)

                    elif not summary.get('wallet'):
                        scan_embed = Embed(color=0xc8a2c8, title=f'Scan failed for {data_to_scan[0:7]}...',
                                           description='Please try another wallet')

                        scan_embed.set_footer(text=f"BlockSight",
                                              icon_url="https://cdn.discordapp.com/attachments/"
                                                       "1184131101782970398/1189235897288372244/BSL_Gradient.png")

                        await scan_message.edit(embed=scan_embed)

                    else:
                        scan_embed = Embed(color=0xc8a2c8, title=f'Scan failed for {data_to_scan[0:7]}...',
                                           description='Please try another wallet')

                        scan_embed.set_footer(text=f"BlockSight",
                                              icon_url="https://cdn.discordapp.com/attachments/"
                                                       "1184131101782970398/1189235897288372244/BSL_Gradient.png")

                        await scan_message.edit(embed=scan_embed)

        elif data_to_scan.startswith('@') or data_to_scan.startswith('https://t.me/'):
            print('TG SCAN STARTS')
            if data_to_scan.startswith('@'):
                data_to_scan = data_to_scan[1:]
            else:
                data_to_scan = data_to_scan[14:]

            print(f'DATA TO SCAN IS: {data_to_scan}')

            scan_embed = Embed(color=0xc8a2c8, title=f'Starting scan for {data_to_scan[0:5]}...',
                               description='Please be patient')

            print('DEFAULT EMBED MADE')

            scan_embed.set_footer(text=f"BlockSight",
                                  icon_url="https://cdn.discordapp.com/attachments/"
                                           "1184131101782970398/1189235897288372244/BSL_Gradient.png")

            # scan_message = await message.channel.send(content='', embed=scan_embed)

            async def make_tg_embed(data_to_use):

                data_window = 30  # TODO -> data['window']
                win_rate = data_to_use.get('win_rate')
                trade_count = data_to_use.get('trade_count')
                channel = data_to_use.get('channel')

                grades = determine_tg_grade(trade_count, win_rate)

                tg_embed = Embed(color=0xc8a2c8, title=f"{channel}'s  {data_window}D Summary",
                                 description='In-Depth Breakdown')
                tg_embed.set_footer(text=f"BlockSight",
                                    icon_url="https://cdn.discordapp.com/attachments/"
                                             "1184131101782970398/1189235897288372244/BSL_Gradient.png")

                try:
                    tg_embed.add_field(name=f'Overall Rank: {grades['overall_grade']}',
                                       value=f'{generate_tg_message(grades)}')
                    tg_embed.add_field(name="", value='', inline=False)

                except Exception as exc:
                    print(exc)
                    raise exc

                print('EMBED HEADER MADE')

                tg_embed.add_field(name=f'Calling Frequency: ({grades['trades_grade']})',
                                   value=f'{round((data_to_use['trade_count'] / window), 2)} calls per day')

                tg_embed.add_field(name=f'Win Rate: {grades['win_rate_grade']}',
                                   value=f'{data_to_use['win_rate']}% '
                                         f'(hit 2.5x in 4 days or less)')

                try:
                    tg_embed.add_field(name=f'Simulated PnL: {grades['pnl_grade']}',
                                       value=f'${round(((grades['pnl'] / window) * await get_sol_price()), 2)}'
                                             f' in daily profits with 1 SOL per trade.')

                    tg_embed.add_field(name="", value='', inline=False)

                    tg_embed.add_field(name="",
                                       value=f'[Join Channel](https://t.me/f{channel})',
                                       inline=False)
                except Exception as e:
                    print(e)
                    raise e

                tg_embed.add_field(name=f'Last updated:', value=f'{f'<t:{int(time.time())}:R>'}')
                # TODO value=f'{f'<t:{data_to_use['last_checked']}:R>'}') -> Replace once API fixed

                return tg_embed

            if not await channel_exists(data_to_scan):
                print('CHANNEL IS NEW')
                await adjust_points(user.name, 20)

            print('API ABOUT TO START')
            data = {}
            async with aiohttp.ClientSession() as session:
                print('SESSION MADE')
                try:
                    async with session.get(f"{blocksight_api}/core/vet-tg-channel/{data_to_scan}") as response:
                        print('RESPONSE GOTTEN')
                        if response.status == 200:
                            data = await response.json()
                            print('API RESPONSE GOTTEN')
                            pprint(data)
                            data['channel'] = data_to_scan
                except Exception as e:
                    print(e)
                    raise e

            if data.get('win_rate'):
                print('WIN RATE EXISTS')
                scan_message = await message.channel.send(content='', embed=scan_embed)
                print('MESSAGE SENT')
                scan_embed = await make_tg_embed(data)
                print('EMBED MADE')

                await scan_message.edit(embed=scan_embed)
                print('MESSAGE EDITED')

            else:
                scan_embed = Embed(color=0xc8a2c8, title=f'Scan failed for {data_to_scan}...',
                                   description='Please try another channel')

                scan_embed.set_footer(text=f"BlockSight",
                                      icon_url="https://cdn.discordapp.com/attachments/"
                                               "1184131101782970398/1189235897288372244/BSL_Gradient.png")

                return '', scan_embed

        else:
            scan_embed = Embed(color=0xc8a2c8, title=f'Invalid Input',
                               description='Please use the right format')

            scan_embed.set_footer(text=f"BlockSight",
                                  icon_url="https://cdn.discordapp.com/attachments/"
                                           "1184131101782970398/1189235897288372244/BSL_Gradient.png")

            return '', scan_embed

    else:
        bad_command_embed = Embed(color=0xc8a2c8, title='Invalid Command',
                                  description='Use .help to see the command list')
        bad_command_embed.set_footer(text=f"BlockSight",
                                     icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                              "1189235897288372244/BSL_Gradient.png")
        return '', bad_command_embed


async def add_user_to_db(username: str, user_id: int | None, current_plan='FREE', plan_end_date=9999999999,
                         db_path=pg_db_url, tg_id: int | None = None, pool=None):

    if pool:
        async with pool.acquire() as conn:  # Use a connection from the pool
            try:
                async with conn.transaction():
                    await conn.execute(
                        """
                        INSERT INTO users (username, current_plan, plan_end_date, points, credits, user_id, telegram_id) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (username) DO UPDATE
                    SET telegram_id = EXCLUDED.telegram_id WHERE users.telegram_id IS NULL;
                        """,
                        username, current_plan, plan_end_date, 0, 1000, user_id, tg_id
                    )
            except Exception as e:
                # Add error handling or logging here
                print(f"An error occurred while adding user: {e}")
                raise e
            finally:
                await conn.close()

    else:
        conn = await asyncpg.connect(db_path)
        try:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO users (username, current_plan, plan_end_date, points, credits, user_id, telegram_id) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (username) DO UPDATE
                SET telegram_id = EXCLUDED.telegram_id;
                    """,
                    username, current_plan, plan_end_date, 0, 1000, user_id, tg_id
                )
        except Exception as e:
            # Add error handling or logging here
            print(f"An error occurred while adding user: {e}")
            raise e
        finally:
            await conn.close()

async def update_user_avatar(username:str, avatar:str, pool = None, db_path=pg_db_url,):
    query = "UPDATE users set discord_avatar_hash = $1 WHERE username = $2;"

    if pool:
        async with pool.acquire() as conn:  # Use a connection from the pool
            try:
                async with conn.transaction():
                    await conn.execute(
                       query,avatar, username)
            except Exception as e:
                # Add error handling or logging here
                print(f"An error {e} occurred while updating avatar of user {username}")
                raise e
            finally:
                await conn.close()

    else:
        conn = await asyncpg.connect(db_path)
        try:
            async with conn.transaction():
                await conn.execute(query,avatar, username)
        except Exception as e:
            # Add error handling or logging here
            print(f"An error {e} occurred while updating avatar of user {username}")
            raise e
        finally:
            await conn.close()


async def get_user_data(username: str, db_path=pg_db_url, pool=None):
    if pool:
        async with pool.acquire() as conn:  # Use a connection from the pool
            try:
                # Retrieve the user's record
                user_query = 'SELECT * FROM users WHERE username = $1;'
                user_record = await conn.fetchrow(user_query, username)

                if user_record:
                    # Count referrals based on the user's referral code
                    referrals_query = 'SELECT COUNT(*) FROM users WHERE referral_used = $1;'
                    referrals_count = await conn.fetchval(referrals_query, user_record['referral_code'])

                    # Combine user data and referrals count
                    data = dict(user_record)
                    data['referrals'] = referrals_count
                    data['avatar_link'] = f"https://cdn.discordapp.com/avatars/{data["user_id"]}/{data["discord_avatar_hash"]}.webp"
                    pprint(data)
                    return data
                else:
                    return {}
            finally:
                await conn.close()
    else:
        conn = await asyncpg.connect(db_path)  # Connect to the database using the provided database URL
        try:
            # Retrieve the user's record
            user_query = 'SELECT * FROM users WHERE username = $1;'
            user_record = await conn.fetchrow(user_query, username)

            if user_record:
                # Count referrals based on the user's referral code
                referrals_query = 'SELECT COUNT(*) FROM users WHERE referral_used = $1;'
                referrals_count = await conn.fetchval(referrals_query, user_record['referral_code'])

                # Combine user data and referrals count
                data = dict(user_record)
                data['referrals'] = referrals_count
                data['avatar_link'] = f"https://cdn.discordapp.com/avatars/{data["user_id"]}/{data["discord_avatar_hash"]}.webp"

                pprint(data)
                return data
            else:
                return {}
        finally:
            await conn.close()


async def get_all_users(db_path=pg_db_url):
    conn = await asyncpg.connect(db_path)
    try:
        # Execute the query to fetch all users
        query = 'SELECT * FROM users;'
        records = await conn.fetch(query)
        # Convert each record to a dictionary and return a list of dictionaries
        return [dict(record) for record in records]
    finally:
        await conn.close()


async def adjust_credits(username, amount, db_path=pg_db_url, spending=True, pool=None):
    # Returns a bool,
    # that will be used to show if a credit consuming operation is valid or not
    if pool:
        async with pool.acquire() as conn:
            try:
                async with conn.transaction():
                    # Fetch current credits and adjust in one step using a conditional update
                    if spending:
                        update_query = '''
                        UPDATE users SET credits = credits - $1
                        WHERE username = $2 AND credits >= $1
                        RETURNING credits;
                        '''
                    else:
                        update_query = '''
                        UPDATE users SET credits = credits + $1
                        WHERE username = $2
                        RETURNING credits;
                        '''

                    new_credits = await conn.fetchval(update_query, amount, username)

                    if new_credits is None:
                        if spending:
                            return False  # Insufficient funds
                        else:
                            return False  # Invalid user

                    return True  # Credits were spent/topped-up successfully
            finally:
                await conn.close()

    else:
        conn = await asyncpg.connect(db_path)
        try:
            async with conn.transaction():
                # Fetch current credits and adjust in one step using a conditional update
                if spending:
                    update_query = '''
                    UPDATE users SET credits = credits - $1
                    WHERE username = $2 AND credits >= $1
                    RETURNING credits;
                    '''
                else:
                    update_query = '''
                    UPDATE users SET credits = credits + $1
                    WHERE username = $2
                    RETURNING credits;
                    '''

                new_credits = await conn.fetchval(update_query, amount, username)

                if new_credits is None:
                    if spending:
                        return False  # Insufficient funds
                    else:
                        return False  # Invalid user

                return True  # Credits were spent/topped-up successfully
        finally:
            await conn.close()


async def adjust_points(username, amount, multiplier=1, db_path=pg_db_url, pool=None):
    # Randomised points increment, returns a bool to indicate if the operation was successful
    if pool:
        async with pool.acquire() as conn:
            try:
                # Calculate the lower and upper bounds of the range
                lb = int(amount * 0.75)
                ub = int(amount * 1.25)

                # Select a random integer from this range
                random_points = random.choice(range(lb, ub + 1)) * multiplier

                # Start a transaction
                async with conn.transaction():
                    # Update the user's points directly and return the new value
                    update_query = '''
                    UPDATE users
                    SET points = points + $1
                    WHERE username = $2
                    RETURNING points;
                    '''

                    new_points = await conn.fetchval(update_query, random_points, username)

                    if new_points is None:
                        return False  # User not found or update failed

                    return True  # Points were incremented successfully
            finally:
                await conn.close()
    else:
        conn = await asyncpg.connect(db_path)  # Connect to the database
        try:
            # Calculate the lower and upper bounds of the range
            lb = int(amount * 0.75)
            ub = int(amount * 1.25)

            # Select a random integer from this range
            random_points = random.choice(range(lb, ub + 1)) * multiplier

            # Start a transaction
            async with conn.transaction():
                # Update the user's points directly and return the new value
                update_query = '''
                UPDATE users
                SET points = points + $1
                WHERE username = $2
                RETURNING points;
                '''

                new_points = await conn.fetchval(update_query, random_points, username)

                if new_points is None:
                    return False  # User not found or update failed

                return True  # Points were incremented successfully
        finally:
            await conn.close()


async def edit_user_data(username, new_data, col_name='', db_path=pg_db_url, pool=None):
    # Define a dictionary mapping valid column names to their SQL-safe names
    valid_columns = {
        'twitter': 'twitter',
        'telegram': 'telegram',
        'wallet': 'wallet',
        'email': 'email',
        'ref-used': 'referral_used',
    }

    if col_name not in valid_columns:
        return False  # Invalid column name provided

    if col_name == 'wallet' and not is_valid_wallet(new_data):
        return False  # Not a SOL wallet

    # Get the SQL-safe column name from the dictionary
    safe_col_name = valid_columns[col_name]

    # Connect to the database and perform the update
    if pool:
        async with pool.acquire() as conn:
            try:
                # Prepare the query to update the specified column using the safe column name
                query = f"UPDATE users SET {safe_col_name} = $1 WHERE username = $2 AND {safe_col_name} IS NULL;"
                result = await conn.execute(query, new_data, username)

                # Check if the update was successful
                return result == 'UPDATE 1'  # This checks if exactly one row was updated
            finally:
                await conn.close()

    else:
        conn = await asyncpg.connect(db_path)
        try:
            # Prepare the query to update the specified column using the safe column name
            query = f"UPDATE users SET {safe_col_name} = $1 WHERE username = $2 AND {safe_col_name} IS NULL;"
            result = await conn.execute(query, new_data, username)

            # Check if the update was successful
            return result == 'UPDATE 1'  # This checks if exactly one row was updated
        finally:
            await conn.close()


async def create_referral_code(username, code, db_path=pg_db_url, pool=None):
    # Check the length of the code
    if len(code) < 3 or len(code) > 15:
        return False  # Return False - Code is too short or too long

    code = code.upper()

    if pool:
        async with pool.acquire() as conn:
            try:
                # Prepare the query to update the referral_code column only if it is currently NULL
                query = '''
                UPDATE users SET referral_code = $1
                WHERE username = $2 AND referral_code IS NULL
                RETURNING referral_code;
                '''

                # Execute the query
                updated_code = await conn.fetchval(query, code, username)

                # Check if the code was updated successfully
                if updated_code == code:
                    await adjust_points(username, 30)
                    return True
                else:
                    return False
            finally:
                await conn.close()
    else:
        conn = await asyncpg.connect(db_path)  # Connect to the database
        try:
            # Prepare the query to update the referral_code column only if it is currently NULL
            query = '''
            UPDATE users SET referral_code = $1
            WHERE username = $2 AND referral_code IS NULL
            RETURNING referral_code;
            '''

            # Execute the query
            updated_code = await conn.fetchval(query, code, username)

            # Check if the code was updated successfully
            if updated_code == code:
                await adjust_points(username, 30)
                return True
            else:
                return False
        finally:
            await conn.close()

async def use_referral_code(user: discord.User, code, client: discord.Client, db_path=pg_db_url, pool=None):
    if pool:
        async with pool.acquire() as conn:
            try:
                # Start a transaction
                async with conn.transaction():
                    # Check if the code exists and the referral_code_used is NULL, then update it
                    update_query = '''
                    UPDATE users
                    SET referral_code_used = $1
                    WHERE username = $2 AND referral_code_used IS NULL
                    AND EXISTS (
                        SELECT 1 FROM users WHERE referral_code = $1
                    )
                    RETURNING username;
                    '''

                    result = await conn.fetchval(update_query, code, user.name)

                    # If the referral code was used successfully
                    if result:
                        # Fetch the username of the referrer
                        referrer_query = 'SELECT username FROM users WHERE referral_code = $1;'
                        referrer_username = await conn.fetchval(referrer_query, code)

                        # Adjust points for both the referrer and the new user
                        if referrer_username:
                            await adjust_points(referrer_username, 300)  # Referrer gets 300 points
                            await adjust_points(user.name, 75)  # New user gets 75 points

                            await assign_role(client, user, BETA_ROLE)

                            return True

                    return False  # If the operation was not successful
            finally:
                await conn.close()
    else:
        conn = await asyncpg.connect(db_path)
        try:
            # Start a transaction
            async with conn.transaction():
                # Check if the code exists and the referral_code_used is NULL, then update it
                update_query = '''
                UPDATE users
                SET referral_code_used = $1
                WHERE username = $2 AND referral_code_used IS NULL
                AND EXISTS (
                    SELECT 1 FROM users WHERE referral_code = $1
                )
                RETURNING username;
                '''

                result = await conn.fetchval(update_query, code, user.name)

                # If the referral code was used successfully
                if result:
                    # Fetch the username of the referrer
                    referrer_query = 'SELECT username FROM users WHERE referral_code = $1;'
                    referrer_username = await conn.fetchval(referrer_query, code)

                    # Adjust points for both the referrer and the new user
                    if referrer_username:
                        await adjust_points(referrer_username, 300)  # Referrer gets 300 points
                        await adjust_points(user.name, 75)  # New user gets 75 points

                        await assign_role(client, user, BETA_ROLE)

                        return True

                return False  # If the operation was not successful
        finally:
            await conn.close()

async def is_user_verified(userid: int, db_url: str = pg_db_url) -> bool:
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
        beta_role = guild.get_role(BETA_ROLE)

        if verified_role in member.roles:
            query = "UPDATE users SET is_verified = True WHERE user_id = $1"
            await conn.execute(query, userid)
            return True
        else:
            return False
    finally:
        await client.close()
        await conn.close()

