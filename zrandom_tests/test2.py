def calculate_for_tokens(daily_revenue, workers_share, workers_number):
    total_workers_share = (workers_share / 100) * daily_revenue
    earnings_per_worker = total_workers_share / workers_number
    return earnings_per_worker


def calculate_for_keys(daily_revenue, workers_share, workers_number):
    total_workers_share = (workers_share / 100) * daily_revenue
    earnings_per_worker = total_workers_share / workers_number
    return earnings_per_worker


num_keys = 412
worker_pge = 30
num_workers = 13.7

print(f'{calculate_for_tokens(num_keys, worker_pge, num_workers):.2f}%')

'''
Great Pool = 13.49%
God Bless China = 12.9%
Arbitrum = 12.16%
CryptoTelugu = 10.61%
XAI END GAME = 10%
Keyholders = 9.5%
XMG CAP = 9.52%
XAIBORG = 9.23%
elon musk = 9.21%
unity cap = 9.05%
Aquagems = 9.02%
XAIverse = 8.39%
'''