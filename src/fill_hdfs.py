from pydoop import hdfs
from sys import stdin, argv

if len(argv) != 3:
    print(f"Usage: {argv[0]} <file> <buffer>")
    exit(-1)

file = argv[1]
buf = int(argv[2])


f = hdfs.open(file, "w")
for i, line in enumerate(stdin):
    if i % buf == 0:
        f.close()
        f = hdfs.open(file, "a")

    f.write(line.encode('utf-8'))
    #print("Scritto:", line)
