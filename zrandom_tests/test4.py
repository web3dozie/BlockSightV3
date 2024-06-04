import math

# Constants
initial_tokens = 25000
initial_price = 0.73  # in dollars

# when farming token balance increases by 1% daily (compounding)
daily_farming_return = 0.8 / 100

# price drops by 1.5% every day (compounding)
daily_price_decrease = 0.5 / 100  # converting percentage to a decimal


# Function to calculate the price after n days
def future_price(days):
    if days >= 60:
        answer = initial_price * math.pow(1 - daily_price_decrease, 60)  # regular price decrease
        answer = answer * math.pow(1 - daily_price_decrease * 2, days - 60)
    else:
        answer = initial_price * math.pow(1 - daily_price_decrease, days)

    if days >= 60:
        answer *= 0.85

    if days >= 100:
        answer *= 0.65

    return answer


# Function to calculate the token balance after n days
def farming_returns(days, tokens):
    return tokens * math.pow(1 + daily_farming_return, days)


# Function to calculate tokens left after unstaking
def tokens_after_unstaking(days, tokens):
    if days == 15:
        return tokens * 0.25
    elif days == 90:
        return tokens * 0.625
    elif days == 180:
        return tokens
    else:
        return 0  # No valid period was provided


def cash_after(farming_days, redemption_days, start_tokens=initial_tokens):
    price_after_everything = future_price(farming_days + redemption_days)

    print(f'Price after: {price_after_everything: .3f}\n'
          f'With: {farming_days} Farming days AND {redemption_days} Unstaking days')

    tokens_after_farming = farming_returns(farming_days, start_tokens)

    tokens_after_redemption = tokens_after_unstaking(redemption_days, tokens_after_farming)

    print(f'Cash after: ${round(tokens_after_redemption * price_after_everything, 2)}\n')


periods = [15, 90, 180]

for period in periods:
    times = [i for i in range(0, 100, 5)]

    for i in times:
        cash_after(i, period)

    print('*********************************************************************************\n\n')

