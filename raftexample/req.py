import json

import requests

for i in range(100000):
    print(requests.put('http://127.0.0.1:9121/a%s' % i, data=json.dumps({"a": i})).text)
    print(i)
