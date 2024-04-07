from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio, cProfile

async def main():
  print(await process_wallet("27bbscgShL5QCa2dZ4oKcaaSUFLqww3tfph6iYeN4Wh5"))

def wrapper():
  asyncio.run(main())


profiler = cProfile.Profile()
profiler.enable()
wrapper()
profiler.disable()
profiler.print_stats()