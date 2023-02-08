import os

for i in range(600):
    print(i)
    os.system(" ~/.gopath/bin/etcdctl put a%s b%s" % (i, i))
