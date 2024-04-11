import unittest
from walletVettingModule.wallet_vetting_utils import process_wallet
from decimal import Decimal

db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight_test' #path to test db

class TestWalletVetting(unittest.IsolatedAsyncioTestCase):
  async def test_process_wallet(self):
    # run process wallet
    wallet = await process_wallet("FWgJk47hKYFswsnKDm4Y6kWR3ujyfEcunwNyFaWrfmtB", end_time=1712755836, db_url=db_url)
    # assert results
    self.assertEqual(wallet, {'wallet': 'FWgJk47hKYFswsnKDm4Y6kWR3ujyfEcunwNyFaWrfmtB', 'trades': 713, 'win_rate': Decimal('30.58'), 'avg_size': Decimal('128.06'), 'last_checked': 1712788781, 'pnl': Decimal('8132.73'), 'window_value': '30d'})

if __name__ == '__main__':
  unittest.main()
