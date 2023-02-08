import os


def get_files(path):
    for item in os.listdir(path):
        dir_file = os.path.join(path, item)
        if os.path.isfile(dir_file):
            yield dir_file
        else:
            for xx in get_files(dir_file):
                yield xx


main_set = set()
for file in get_files('.'):
    if file.endswith('py'):
        continue
    try:
        flag = False
        # data = ''
        # with open(file,'r', encoding='utf8')as f:
        #     data = f.read()
        #     if 'clientv3' in data and 'clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"' not in data:
        #         data = data.replace('import (', 'import (\n	clientv3 "github.com/ls-2018/etcd_cn/client_sdk/v3"')
        #         flag = True
        #
        #         # print(file)
        # if flag:
        #     with open(file, 'w', encoding='utf8', )as f:
        #         f.write(data)
        with open(file, 'r', encoding='utf8') as f:
            if 'package main' in f.read():
                main_set.add(os.path.dirname(file))

    except Exception:
        pass

for item in main_set:
    # print(item)
    print('cd %s ; go build . ; cd -' % item.replace('\\', '/'))

for item in main_set:
    # print(item)
    print('rm %s' % os.path.join(item, item.split('/')[-1]))
