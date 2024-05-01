from walletVettingModule.wallet_vetting_utils import process_wallet
import asyncio, cProfile, speedscope

async def main():
  print(await process_wallet("6aqgmDym6Fr6AxjmQJ2BSaA39VDLrMLYGBPQzYBrEqkW"))

def wrapper():
  asyncio.run(main())

with speedscope.track('profile.json'):
  wrapper()

# profiler = cProfile.Profile()
# profiler.enable()
# wrapper()
# profiler.disable()
# profiler.print_stats()