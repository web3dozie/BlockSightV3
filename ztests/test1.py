def get_points(value, thresholds):
    if value >= thresholds['S']:
        return 25
    elif value >= thresholds['A']:
        return 15
    elif value >= thresholds['B']:
        return 10
    elif value >= thresholds['C']:
        return 5
    else:
        return 0


win_rate = 0.00

win_rate_thresholds = {'S': 25, 'A': 20, 'B': 15, 'C': 10, 'F': 10}

win_rate_points = get_points(win_rate, win_rate_thresholds) * 2  # Double points for win rate

print(win_rate_points)