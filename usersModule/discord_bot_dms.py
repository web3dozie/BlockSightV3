from pprint import pprint

import asyncpg
import discord, asyncio
from discord import Embed

from dbs.db_operations import user_exists, pg_db_url
from usersModule.user_utils import discord_command_executor, add_user_to_db

BOT_TOKEN = 'MTIwNDgyMDc3MjM1OTc2NjA2Ng.GRhhu0.3zkLNrB7dzcPcou_309mKRQ0WWDFbFqwg2pjlg'
BLOCKSIGHT_SERVER_ID = 1101255371210887258
VERIFIED_ROLE = 1200907085320294460
BETA_ROLE = 1184124534366933143
BETA_PRIME_ROLE = 1192053087586762803
HONORARY_ROLE = 1184125377124257842


class CompanionBot:
    def __init__(self):
        intents = discord.Intents.default()
        intents.messages = True
        intents.guilds = True
        intents.members = True
        intents.dm_messages = True
        intents.message_content = True

        self.client = discord.Client(intents=intents)
        self.token = BOT_TOKEN

        # SERVER AND ROLE ID'S
        self.blocksight_server_id = BLOCKSIGHT_SERVER_ID
        self.verified_role = VERIFIED_ROLE
        self.beta_role = BETA_ROLE
        self.beta_prime_role = BETA_PRIME_ROLE
        self.honorary_role = HONORARY_ROLE

        self.guild = self.client.get_guild(self.blocksight_server_id)
        self.pool = None

        @self.client.event
        async def on_ready():
            print(f'We have logged in as {self.client.user}')
            self.guild = self.client.get_guild(self.blocksight_server_id)
            self.pool = await asyncpg.create_pool(dsn=pg_db_url)

        @self.client.event
        async def on_message(message):
            # Ignore messages sent by the bot
            if message.author == self.client.user:
                return

            # Check if the message is a DM
            if isinstance(message.channel, discord.DMChannel):
                user = message.author

                text = message.content

                # Check if the user is in the DB. If not, prompt to sign-up
                if not await user_exists(user.name, pool=self.pool):
                    await add_user_to_db(user.name, user.id, pool=self.pool)

                if self.guild:
                    member = self.guild.get_member(user.id)
                    if member:
                        # Check if the member has the beta role by ID
                        role_beta = self.guild.get_role(self.beta_role)
                        role_verified = self.guild.get_role(self.verified_role)

                        if role_verified in member.roles:
                            if text.startswith('.help') or text.startswith('.use_code'):
                                content, embed = await discord_command_executor(text, user, client=self.client,
                                                                                message=message)
                                await message.channel.send(content=content, embed=embed)

                            elif role_beta in member.roles:
                                if text.startswith('.'):
                                    # COMMAND PROCESSING CODE HERE
                                    content, embed = await discord_command_executor(text, user, client=self.client,
                                                                                    message=message)
                                    await message.channel.send(content=content, embed=embed)

                                else:
                                    invalid_input_embed = (Embed(color=0xc8a2c8)
                                                           .add_field(name='Invalid Input', value='Use .help to see'
                                                                                     ' the command list'))
                                    await message.channel.send(embed=invalid_input_embed)
                            else:
                                not_verified_embed = Embed(color=0xc8a2c8)
                                not_verified_embed.add_field(name='',
                                                             value='You must have the beta role or higher '
                                                                   'to use this tool properly\n')
                                not_verified_embed.add_field(name='',
                                                             value='Type ".use_code <CODE>" to use a referral code'
                                                                   ' and activate Beta.\n'
                                                                   'This will also unlock more channels in the server')
                                await message.channel.send(embed=not_verified_embed)

                        else:
                            not_verified_embed = Embed(color=0xc8a2c8)
                            not_verified_embed.add_field(name='', value='You must be a verified BlockSight member '
                                                                        'to use this tool.\n'
                                                                        'You are already in the server.\n'
                                                                        'Make sure you are properly verified ✅')
                            not_verified_embed.add_field(name='', value='Get Verified Here: '
                                                                        'https://discord.com/channels'
                                                                        '/1101255371210887258/1200907088990306365')
                            await message.channel.send(embed=not_verified_embed)

                    else:
                        not_in_server_embed = Embed(color=0xc8a2c8)
                        not_in_server_embed.add_field(name='', value='You must be a verified member of the'
                                                                     ' BlockSight Discord Server to use this tool.')
                        not_in_server_embed.add_field(name='', value='Join Using: https://discord.gg/blocksight')
                        await message.channel.send(embed=not_in_server_embed)
                else:
                    print('Server does not exist.')

            # Check if the message is in a specific channel

            # TODO use this part to reward with points and other stuff

        # TODO use the client to make a function that checks different channels at intervals and award random points

    def run(self):
        self.client.run(self.token)


async def main():
    await CompanionBot().client.start(BOT_TOKEN)

if __name__ == "__main__":
    asyncio.run(main())
