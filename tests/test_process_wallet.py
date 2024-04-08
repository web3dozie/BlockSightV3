import unittest
from walletVettingModule.wallet_vetting_utils import process_wallet
from decimal import Decimal

db_url = 'postgresql://bmaster:BlockSight%23Master@173.212.244.101/blocksight_test' #path to test db

class TestWalletVetting(unittest.IsolatedAsyncioTestCase):
  async def test_process_wallet(self):
    # run process wallet
    wallet = await process_wallet("CnsMpVXrzP3V6eCnE3hojnrMW9x6oWwGBWJ1sN5qdWkn", db_url=db_url)
    # assert results
    self.assertEqual(wallet, {'avg_size': Decimal('289.50'), 'last_checked': 1712520557, 'pnl': Decimal('1173.27'), 'trades': 629, 'wallet': 'CnsMpVXrzP3V6eCnE3hojnrMW9x6oWwGBWJ1sN5qdWkn', 'win_rate': Decimal('22.73'), 'window_value': '30d'})

if __name__ == '__main__':
  unittest.main()
