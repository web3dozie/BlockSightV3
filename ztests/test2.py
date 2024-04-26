def calculate_for_tokens(daily_revenue, workers_share, workers_number):
    total_workers_share = (workers_share / 100) * daily_revenue
    earnings_per_worker = total_workers_share / workers_number
    return earnings_per_worker


def calculate_for_keys(daily_revenue, workers_share, workers_number):
    total_workers_share = (workers_share / 100) * daily_revenue
    earnings_per_worker = total_workers_share / workers_number
    return earnings_per_worker


rev = 700
worker_pge = 50
num_workers = 7

print(f'{calculate_for_tokens(rev, worker_pge, num_workers):.2f}%')
