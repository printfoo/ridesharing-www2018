#!/usr/bin/env python

import sys, os
import json, time
import requesocks as requests
import multiprocessing as mp
from start_simulation import generate_grid, load_accounts, trim_grid
import pdb

def start_uber_api(account_path):
    tokens = ['P048oVbIPEsVXWRxokCOvAIeIeuHwYeV_tNyFbAE',
    'PDzCBtMa1BFu_XOTZOLde58R3jygJ_hLp7zt77bv']
    accounts = load_accounts(account_path)

    grid = generate_grid(-74.00003712345876, 40.77174202324564,
    msize = 10, interval = 0.0041)
    lon_lat = trim_grid(grid, city = 'nyc')

    token_index, req_cnt = 0, 23
    cur_epoch = str(int(time.time()))

    while True:
        sys.stderr.write("Round: %d\t%d\n" %(token_index, req_cnt))
        cur_token = tokens[token_index]

        jobs = []

        for (user, email, password, port), (lon, lat) in zip(accounts, lon_lat):
            cur_dir = os.path.join('api_reply', user)
            if not os.path.isdir(cur_dir):
                os.makedirs(cur_dir)

            cur_fpath = os.path.join(cur_dir, cur_epoch + '.txt')
            p = mp.Process(target = call_uber_api,
            args = (port, cur_token, lat, lon, cur_fpath))
            jobs.append(p)
            p.start()

        for p in jobs:
            p.join()

        time.sleep(90)

        req_cnt -= 1
        if req_cnt == 0:
            req_cnt = 23

            token_index += 1
            if token_index >= 2:
                token_index = 0
                cur_epoch = str(int(time.time()))
        
def call_uber_api(port, token, lat, lon, fpath):
    print 'Calling api for ' + fpath, port
    """Build a socks session."""
    proxy_addr = 'socks5://127.0.0.1:%s' %port
    session = requests.session()
    session.proxies = {'http': proxy_addr, 'https': proxy_addr}

    url = 'https://api.uber.com/v1/estimates/price'

    parameters = {
    'server_token': token,
    'start_latitude': lat,
    'start_longitude': lon,
    'end_latitude': lat,
    'end_longitude': lon
    }

    with open(fpath, 'a') as f:
        while True:
            try:
                response = session.get(url = url, params = parameters)
                res_json = json.loads(response.content)
            except Exception, e:
                sys.stderr.write(fpath + ': ' + str(e) + '\n')
                break

            f.write(str(int(time.time() * 1000)) + ':::::')
            json.dump(res_json, f)
            f.write('\n')

            break

if __name__ == '__main__':
    start_uber_api(sys.argv[1])
