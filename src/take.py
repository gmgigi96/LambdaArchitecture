from sys import stdin, stdout, argv, exit
from time import sleep

if len(argv) != 3:
    print(f"Usage: {argv[0]} <sleep_time> <n. lines>")
    exit(-1)

sleep_time, lines = list(map(int, argv[1:]))

for i, line in enumerate(stdin):
    if i % lines == 0:
        sleep(sleep_time)
    stdout.write(line)