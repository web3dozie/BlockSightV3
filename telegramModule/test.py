import asyncio
from vet_tg_channel import vetChannel

telegram_channels = [
    "NiksGambles",
    "LOLLISIGMAL",
    "spidersjournal",
    "jakefam",
    "apezone",
    "thorshammergems",
    "Zorrogems",
    "MhotCallsErc",
    "mad_apes_gambles",
    "Maestrosdegen",
    "NagatoGemCalls",
    "DegenSeals",
    "TradersViewByZeppelin",
    "HeliosGem",
    "MarkDegens",
    "CharlesCalls",
    "PEYOSDEGENHUB",
    "crypt0coc0",
    "gubbinscalls",
    "LionCALL",
    "doctoregems",
    "SolanaHunter_Channel",
    "Cryptic_Maestro",
    "GodsCryptoReviews",
    "cryptoprophetcalls",
    "SIGcalls",
    "EsPlays",
    "NeoCallss",
    "AlphaArtemissCall",
    "PEPEgambleETH",
    "JFRJ0x1",
    "Chad_Crypto",
    "HighRiskCall",
    "AchillesGambles",
    "Emilianscalls",
    "leoclub168c",
    "Bullish_Signalz",
    "mooneagle_call",
    "spacemandifferentchaincallz",
    "EthGambles",
    "MadarasGambles",
    "Archerrgambles",
    "SafePlayOnly_4AM",
    "degensgems",
    "sugarydick",
    "WhoDis6964",
    "EZMoneyCalls",
    "GabbensCalls",
    "HellsingGamble",
    "PapasCall",
    "SapphireCalls",
    "ralverogems",
    "AlphaArbitrageCalls",
    "CryptoFrogsGems",
    "bagcalls",
    "maythouscalls"
]


async def vetChannelLimited(semaphore, channel):
    async with semaphore:
        return await vetChannel(channel=channel)


async def main_func():
    semaphore = asyncio.Semaphore(1)  # Limits the number of concurrent tasks to 5
    tasks = [vetChannelLimited(semaphore, channel) for channel in telegram_channels]
    results = await asyncio.gather(*tasks)
    return results

# If this script is the main program and it's not being imported, run the asyncio event loop
if __name__ == "__main__":
    asyncio.run(main_func())
