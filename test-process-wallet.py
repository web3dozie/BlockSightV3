from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio, cProfile

async def main():
  print(await process_wallet("CnsMpVXrzP3V6eCnE3hojnrMW9x6oWwGBWJ1sN5qdWkn"))

def wrapper():
  asyncio.run(main())


profiler = cProfile.Profile()
profiler.enable()
wrapper()
profiler.disable()
profiler.print_stats()