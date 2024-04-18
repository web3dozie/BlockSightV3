"""
PRIMARY LOGIN METHOD WILL BE VIA A DISCORD ACCOUNT
"""
import random
import time
from datetime import datetime
from pprint import pprint

import asyncpg
import discord

from discord import Embed
from walletVettingModule.wallet_vetting_utils import is_valid_wallet


BLOCKSIGHT_SERVER_ID = 1101255371210887258
VERIFIED_ROLE = 1200907085320294460
BETA_ROLE = 1184124534366933143
BETA_PRIME_ROLE = 1192053087586762803
HONORARY_ROLE = 1184125377124257842

pg_db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight'


async def sign_up(username, db_path=pg_db_url):
    await add_user_to_db(username)


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


async def update_max_role(username: str, role: str, db_path: str = pg_db_url):
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


async def assign_role():
    pass


async def discord_command_executor(text: str, user: discord.User, client: discord.Client):
    """
        This function executes commands and returns responses for sending
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
        info = await get_user_data(user.name)

        embed = Embed(title=f"{user.name}'s Info.", color=0xc8a2c8)
        embed.set_footer(text=f"BlockSight",
                         icon_url="https://cdn.discordapp.com/attachments/1184131101782970398/"
                                  "1189235897288372244/BSL_Gradient.png")

        max_role = await get_max_role(client, user)

        await update_max_role(user.name, max_role)

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


async def add_user_to_db(username: str, db_path=pg_db_url):
    conn = await asyncpg.connect(db_path)
    try:
        # You could add more fields by modifying the query and the values passed
        await conn.execute(
            """
            INSERT INTO users (username, current_plan) VALUES ($1, $2)
            ON CONFLICT (username) DO NOTHING;
            """,
            username, 'FREE'
        )
    finally:
        await conn.close()


async def get_user_data(username: str, db_path=pg_db_url):
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


async def adjust_credits(username, amount, db_path='pg_db_url', spending=True):
    # Returns a bool,
    # that will be used to show if a credit consuming operation is valid or not

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


async def adjust_points(username, amount, multiplier=1, db_path='pg_db_url'):
    # Randomised points increment, returns a bool to indicate if the operation was successful

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


async def edit_user_data(username, new_data, col_name='', db_path='pg_db_url'):
    # Define a dictionary mapping valid column names to their SQL-safe names
    valid_columns = {
        'twitter': 'twitter',
        'telegram': 'telegram',
        'wallet': 'wallet'
    }

    if col_name not in valid_columns:
        return False  # Invalid column name provided

    if col_name == 'wallet' and not is_valid_wallet(new_data):
        return False  # Not a SOL wallet

    # Get the SQL-safe column name from the dictionary
    safe_col_name = valid_columns[col_name]

    # Connect to the database and perform the update
    conn = await asyncpg.connect(db_path)
    try:
        # Prepare the query to update the specified column using the safe column name
        query = f'UPDATE users SET {safe_col_name} = $1 WHERE username = $2;'
        result = await conn.execute(query, new_data, username)

        # Check if the update was successful
        return result == 'UPDATE 1'  # This checks if exactly one row was updated
    finally:
        await conn.close()


async def create_referral_code(username, code, db_path='pg_db_url'):
    # Check the length of the code
    if len(code) < 3 or len(code) > 10:
        return False  # Return False - Code is too short or too long

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
        return updated_code == code
    finally:
        await conn.close()


async def use_referral_code(username, code, db_path='pg_db_url'):
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

            result = await conn.fetchval(update_query, code, username)

            # If the referral code was used successfully
            if result:
                # Fetch the username of the referrer
                referrer_query = 'SELECT username FROM users WHERE referral_code = $1;'
                referrer_username = await conn.fetchval(referrer_query, code)

                # Adjust points for both the referrer and the new user
                if referrer_username:
                    await adjust_points(referrer_username, 300)  # Referrer gets 300 points
                    await adjust_points(username, 75)  # New user gets 75 points
                    return True

            return False  # If the operation was not successful
    finally:
        await conn.close()
