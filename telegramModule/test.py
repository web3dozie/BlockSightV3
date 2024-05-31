import asyncio
import random

import asyncpg


from dbs.db_operations import pg_db_url
from listeners.telegram_pools.tg_client_pooling import TelegramClientPool
from vet_tg_channel import vetChannel

with open('channels.txt') as file:
    lines = file.readlines()


telegram_channels = ['TriColourCrypto', 'TwinkleCalls', 'InfinityGainCalls', 'nowisdeep', 'totaloneth', 'shrimpycall', 'anonymouzjournal', 'odglug69420069', 'snowstormcalls', 'ShinChanCalls99X', 'bullishorbearishgambles', 'bollish', 'pumpwithshadow', 'TradersViewByZeppelin', 'jamiescave', 'travel_diary_gamble', 'curlycurrycalls', 'BROTHERSMARKETINGCHANNEL1', 'CraftyGems', 'MooCalls', 'FVKcalls', 'CryptoPioneersHub', 'leopardcalls', 'Thatguyscrypto', 'MineGems', 'WillOfDgems', 'Oliversafucalls', 'AvastarCalls', 'marucallss', 'theapedgemscalls', 'NdranghetaETHGems', 'spyongems', 'TheSolitairePrestige', 'caesars_gambles', 'sidschadcalls', 'butroncalls', 'BiDaoPD', 'Crizalcallsx100', 'susplays', 'Cryptodynast', 'jkplays', 'ntmexpress', 'petergambles', 'earlycallsby0xEly', 'thedailymememag', 'InsidersLoungeAnnouncement', 'MarkGems', 'MonkeyTreasury', 'lowtaxbsc', 'ZerolixCalls', 'johnwickmemesafari', 'ZLaunchBotOfficial', 'roshambocalls', 'MemecoinLounge', 'killereyesCP', 'OmegaApes', 'Bane_calls', 'tastycalls', 'Pika_Microcap', 'stubbornplays', 'Joe420Calls', 'BiggBoggCalls', 'KingDegens', 'andyshoutout', 'FurkzCallz', 'millionsordustcalls', 'quantmask', 'wingsinsider', 'chosengemcalls', 'badapescalls', 'Zorrogems', 'VirusCalls', 'DegensGeneration', 'feihuziben', 'SpyDefi', 'actcalls', 'SapphireCalls', 'DogsRockDaily', 'One_Vk', 'MakeMoneyWithMattTg', 'thorshammergems', 'SpotLightCaLLs', 'antirug_calls', 'PowsGemCalls', 'TheCorleoneEmpire', 'Green_Apes_Calls', 'OssyCalls', 'EmiLyscrypto', 'civilianinvestors', 'CoinLaunchAMA', 'TopGCall', 'Showtimekols', 'GodOfApesCalls', 'RyuCryptoGems', 'erics_calls', 'BlockChainBrothersGambles', 'believe93club', 'WhoDis6964', 'themanagercalling', 'degen1hub', 'vinci_gambles', 'ZeroGenerations', 'WenCaleb', 'MidnightCallss', 'PythonPlays', 'HellsingCalls', 'dr100xlaboratory', 'touhaody', 'jackcrypto_calls', 'CobraGems', 'travladdsafureviews', 'Flashsriskyprintors', 'THE_GEMSZ', 'shyroshigambles', 'ViperDegens', 'buyingthis', 'KnightsRoyalVentures', 'ToshisAlphaCalls', 'tanjicalls', 'FightClubERC', 'mackcalls', 'TheBlockchainGods']
print(telegram_channels)
print(len(telegram_channels))


async def vetChannelLimited(semaphore, channel, tg_pool, pool=None):
    try:
        async with semaphore:
            await asyncio.sleep(10)
            return await vetChannel(channel=channel, tg_pool=tg_pool, pool=pool)
    except Exception as e:
        print(f"Exception in vetChannelLimited {e} while vetting channel  {channel}")

d = +2348162921144


async def main_func():
    semaphore = asyncio.Semaphore(2)  # Limits the number of concurrent tasks to
    tg_pool = TelegramClientPool(api_hash='841396171d9b111fa191dcdce768d223', api_id=21348081)
    pool = await asyncpg.create_pool(dsn=pg_db_url, min_size=50, max_size=450, command_timeout=360)

    tasks = [vetChannelLimited(semaphore, channel, tg_pool=tg_pool, pool=pool) for channel in telegram_channels]
    results = await asyncio.gather(*tasks)

    return results

if __name__ == "__main__":
    asyncio.run(main_func())
