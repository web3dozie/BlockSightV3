from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio, cProfile

async def main():
  print(await process_wallet("HhQAJ3j95hVNr6C4iWiYvYau2jXVQ6KPfHJstsV6kCbr"))

def wrapper():
  asyncio.run(main())


profiler = cProfile.Profile()
profiler.enable()
wrapper()
profiler.disable()
profiler.print_stats()