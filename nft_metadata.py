import concurrent.futures
import requests
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import json
import sys
import pandas
import os
import openpyxl

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

if not sys.argv[1]:
    sys.exit("Missing project's name. EX: creepz")
else:
    _project_name = sys.argv[1]
if not sys.argv[2]:
    sys.exit("Missing project's URL")
else:
    _url = sys.argv[2]
    if _url[-1] != '/':
        _url += '/'
if not sys.argv[3]:
    sys.exit("Missing project's NFT amount. EX : 1000")
else:
    _range = int(sys.argv[3])
_connections = 100
if sys.argv[4]:
    _connections = int(sys.argv[4])


def load_url(url):
    ans = session.get(url)
    d = ans.json()

    # Put id in data
    d['id'] = url.replace(_url, '')

    return d


def output_csv(_out):
    for entry in _out:
        for attribute in entry['attributes']:
            entry[attribute['trait_type']] = attribute['value']
    pandas.DataFrame.from_dict(_out).to_excel(f"{_project_name}/metadata.xlsx")


if __name__ == '__main__':
    print(f"Project name : {_project_name} \n"
          f"Project url : {_url} \n"
          f"Number of nfts : {_range} \n"
          f"Number of concurrent connections : {_connections} \n")
    out = []
    CONNECTIONS = _connections
    urls = [f'{_url}{x}' for x in range(_range)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_url = (executor.submit(load_url, url) for url in urls)
        time1 = time.time()
        for future in concurrent.futures.as_completed(future_to_url):
            try:
                data = future.result()
            except Exception as exc:
                data = str(type(exc))
            finally:
                out.append(data)

    if not os.path.exists(_project_name):
        os.makedirs(_project_name)

    out_sorted = sorted(out, key=lambda d: d['id'])

    with open(f"{_project_name}/metadata.json", 'w') as f:
        json.dump(out_sorted, f)

    output_csv(out_sorted)
    session.close()
    time2 = time.time()

    print(f'Took {time2 - time1:.2f} s')
