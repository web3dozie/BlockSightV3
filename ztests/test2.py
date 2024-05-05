def calculate_for_tokens(daily_revenue, workers_share, workers_number):
    total_workers_share = (workers_share / 100) * daily_revenue
    earnings_per_worker = total_workers_share / workers_number
    return earnings_per_worker


def calculate_for_keys(daily_revenue, workers_share, workers_number):
    total_workers_share = (workers_share / 100) * daily_revenue
    earnings_per_worker = total_workers_share / workers_number
    return earnings_per_worker


rev = 265
worker_pge = 49
num_workers = 18.9

print(f'{calculate_for_tokens(rev, worker_pge, num_workers):.2f}%')

'''
XAIverse = 15.09%
Arbitrum = 14.52%
Aquagems = 14.21%
God Bless China = 12.9%
CryptoTelugu = 12.23%
unity cap = 10.47%
ShanyXAI = 10.39%
XAI END GAME = 10%
Keyholders = 9.5%
XMG CAP = 8.85%
XAIBORG = 8.33%
'''