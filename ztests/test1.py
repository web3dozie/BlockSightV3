import msvcrt
import os

'''
7 A DONE DONE
7 B DONE DONE
9 DONE DONE
10 DONE DONE
11 DONE
12 DONE
'''

n = 0
while True:
    if msvcrt.kbhit():
        key = msvcrt.getch()
        if key == b'\r':
            n += 1
        else:
            n = 0

        os.system('cls')
        print(f'{n} answers in a row.')



