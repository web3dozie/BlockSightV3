starting_lot = 0.02
num_attempts = 14

with_1_point_4_x = [starting_lot, starting_lot] + [round((starting_lot * (1.4 ** num)), 2) for num in range(1, num_attempts)]
with_1_point_45_x = [starting_lot, starting_lot] + [round((starting_lot * (1.45 ** num)), 2) for num in range(1, num_attempts)]
with_1_point_5_x = [starting_lot, starting_lot] + [round((starting_lot * (1.5 ** num)), 2) for num in range(1, num_attempts)]
with_1_point_55_x = [starting_lot, starting_lot] + [round((starting_lot * (1.55 ** num)), 2) for num in range(1, num_attempts)]
with_1_point_6_x = [starting_lot, starting_lot] + [round((starting_lot * (1.6 ** num)), 2) for num in range(1, num_attempts)]

print(f'Lots for 1.4x: {with_1_point_4_x}')
print(f'Lots for 1.45x: {with_1_point_45_x}')
print(f'Lots for 1.5x: {with_1_point_5_x}')
print(f'Lots for 1.55x: {with_1_point_55_x}')
print(f'Lots for 1.6x: {with_1_point_6_x}')

print(len(with_1_point_6_x))
