"""
PRIMARY LOGIN METHOD WILL BE VIA A DISCORD ACCOUNT
"""
import random
import asyncpg
import discord

from walletVettingModule.wallet_vetting_utils import is_valid_wallet

pg_db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight'


async def sign_up(user: discord.User, db_path=pg_db_url):
    # login via discord

    # check if they are in the server and verified
    # if they are add user to db return true
    # else return false
    pass


async def login(user: discord.User, db_path=pg_db_url):
    # check if they are in db
    # if they are proceed
    # else (call signup)
    pass


async def add_user_to_db(username: str, db_path=pg_db_url):
    conn = await asyncpg.connect(db_path)
    try:
        # You could add more fields by modifying the query and the values passed
        await conn.execute(
            """
            INSERT INTO users (username) VALUES ($1)
            ON CONFLICT (username) DO NOTHING;
            """,
            username
        )
    finally:
        await conn.close()


async def get_user_data(username: str, db_path=pg_db_url):
    conn = await asyncpg.connect(db_path)  # Connect to the database using the provided database URL
    try:
        query = 'SELECT * FROM users WHERE username = $1;'
        user_record = await conn.fetchrow(query, username)
        if user_record:
            return dict(user_record)
        else:
            return {}
    finally:
        await conn.close()  # Ensure the connection is closed after the operation


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
                    await adjust_points(username, 75)           # New user gets 75 points
                    return True

            return False  # If the operation was not successful
    finally:
        await conn.close()
