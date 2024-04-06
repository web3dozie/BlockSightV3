from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio, cProfile

async def main():
  print(await process_wallet("6RJTHCa7k1HTkRopp44JU1UjFftYpdAC2QZLVjvK5vWT"))

def wrapper():
  asyncio.run(main())


profiler = cProfile.Profile()
profiler.enable()
wrapper()
profiler.disable()
profiler.print_stats()