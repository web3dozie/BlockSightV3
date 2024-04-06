from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio, cProfile

async def main():
  print(await process_wallet("DEBsFVDp9qmjkTPHVDo3jWGw4kBgbtVzCLwEx3B61Kr4"))

def wrapper():
  asyncio.run(main())


profiler = cProfile.Profile()
profiler.enable()
wrapper()
profiler.disable()
profiler.print_stats