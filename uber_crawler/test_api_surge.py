#!/usr/bin/env python

"""
./collect_border_data.py ../raw_data/uber_accounts.txt sim 2> log.txt
"""

import sys, os, json
import multiprocessing as mp
from datetime import datetime
import time, csv
import requesocks as requests
import pdb

"""
kristina_fuller -73.9836371235 40.7676420232
erma_ford -73.9631371235 40.7594420232
irene_sandoval -73.9877371235 40.7553420232
marco_ramsey -73.9836371235 40.7471420232
"""

def start_collecting(account_path, mode):
    tokens = \
    ['P048oVbIPEsVXWRxokCOvAIeIeuHwYeV_tNyFbAE',
    'PDzCBtMa1BFu_XOTZOLde58R3jygJ_hLp7zt77bv',
    'iXzDTF4MLZ8eZVPz_fN1D3bV-MfnVAIjwzRcZfTD',
    '7jk35PRJSU6gXgiZqUwWATx9U7RLHkXEXEsKzXH1',
    'Moiw3oaen5GacCCDZHnvkerOmrrFs5VySPKKDyeD',
    'FwJ7rtuN8wu1xh7kpx5t011kapmM3KzA1k1yO9so']

    accounts = list(load_accounts(account_path))
    groups = [[
    (-73.9836371235, 40.7676420232, 0, 0), 
    (-73.9631371235, 40.7594420232, 0, 1),
    (-73.9877371235, 40.7553420232, 1, 0),
    (-73.9836371235, 40.7471420232, 1, 1)]]

    account_group = []
    account_index, account_len = 0, len(accounts)
    token_index, token_len = 0, len(tokens)

    for group in groups:
        cur_group = []

        for entry in group:
            cur_account = accounts[account_index]
            cur_token = tokens[token_index]

            cur_group.append((cur_account, entry, cur_token))

            account_index += 1
            if account_index >= account_len:
                account_index = 0

            token_index += 1
            if token_index >= token_len:
                token_index = 0

        account_group.append(cur_group)

    for group_index, cur_group in enumerate(account_group):
        req_cnt = 0

        while True:
            cur_dir = os.path.join('test_api_surge', str(group_index))
            if not os.path.isdir(cur_dir):
                os.makedirs(cur_dir)

            jobs = []

            for (user, email, password, port), (lon, lat, x, y), \
            cur_token in cur_group:
                cur_fpath = os.path.join(cur_dir, '%d-%d.txt' %(x, y))

                p = mp.Process(target = call_uber_api,
                args = (port, cur_token, lat, lon, cur_fpath))
                jobs.append(p)
                p.start()

            for p in jobs:
                p.join()

            time.sleep(5)
            
            req_cnt += 1

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

def group_measuring_points(lon_lat):
    groups, prev_row, cur_group = [], [], []

    for cur_row in lon_lat:
        cur_row_index = 0

        for lon, lat, (x, y) in cur_row:
            if len(cur_group) >= 500:
                groups.append(cur_group)
                
                """Add border measuring points."""
                cut_lon, cur_lat, (cur_x, cur_y) = cur_row[cur_row_index]
                last_lon, last_lat, (last_x, last_y) = cur_row[-1]

                cur_group = [(lon, lat, x, y) for lon, lat, (x, y) in cur_row]
                cur_group += [(lon, lat, x, y) for lon, lat, (x, y)
                in prev_row if y >= cur_y and y <= last_y]

                break
            else:
                cur_group.append((lon, lat, x, y))
                cur_row_index += 1

        prev_row = cur_row

    groups.append(cur_group)

    return groups

def trim_grid(grid, total = 43, city = 'sf'):
    lon_lat = []

    if city == 'manhattan':
        lon_lat.append(grid[0][22:25])
        lon_lat.append(grid[1][21:27])
        lon_lat.append(grid[2][21:27])
        lon_lat.append(grid[3][21:26])
        lon_lat.append(grid[4][20:25])
        lon_lat.append(grid[5][19:25])
        lon_lat.append(grid[6][18:24])
        lon_lat.append(grid[7][18:23])
        lon_lat.append(grid[8][18:23])
        lon_lat.append(grid[9][17:22])
        lon_lat.append(grid[10][17:22])
        lon_lat.append(grid[11][16:22])
        lon_lat.append(grid[12][15:22])
        lon_lat.append(grid[13][14:22])
        lon_lat.append(grid[14][14:22])
        lon_lat.append(grid[15][13:22])
        lon_lat.append(grid[16][13:22])
        lon_lat.append(grid[17][12:22])
        lon_lat.append(grid[18][11:22])
        lon_lat.append(grid[19][10:23])
        lon_lat.append(grid[20][10:23])
        lon_lat.append(grid[21][9:21])
        lon_lat.append(grid[22][9:21])
        lon_lat.append(grid[23][7:19])
        lon_lat.append(grid[24][7:19])
        lon_lat.append(grid[25][6:20])
        lon_lat.append(grid[26][5:19])
        lon_lat.append(grid[27][4:18])
        lon_lat.append(grid[28][4:16])
        lon_lat.append(grid[29][3:16])
        lon_lat.append(grid[30][3:15])
        lon_lat.append(grid[31][2:14])
        lon_lat.append(grid[32][2:13])
        lon_lat.append(grid[33][2:12])
        lon_lat.append(grid[34][2:12])
        lon_lat.append(grid[35][2:12])
        lon_lat.append(grid[36][1:12])
        lon_lat.append(grid[37][2:12])
        lon_lat.append(grid[38][1:12])
        lon_lat.append(grid[39][0:12])
        lon_lat.append(grid[40][0:11])
        lon_lat.append(grid[41][0:6])
        lon_lat.append(grid[42][0:4])
    else:
        pdb.set_trace()
    
    return lon_lat

def generate_grid(start_lon, start_lat, msize = 8, interval = 0.0078):
    grid = []

    for i in xrange(msize):
        grid.append([(start_lon, start_lat - (i * interval), (i, 0))])

    for row_index, row in enumerate(grid):
        s_lon, s_lat, s_index = row[0]
        for i in xrange(1, msize):
            row.append((s_lon + (i * interval), s_lat, (row_index, i)))

    return grid

def load_accounts(account_path):
    accounts = set()

    with open(account_path, 'r') as f:
        acc_reader = csv.reader(f, delimiter = '\t')
        header = acc_reader.next()
        field_index = {v:k for (k, v) in enumerate(header)}

        for row in acc_reader:
            accounts.add((row[field_index['name']],
                row[field_index['email']],
                row[field_index['password']],
                row[field_index['port']]))

        """Remove the last one because there are not enough ports."""
        accounts.remove((row[field_index['name']],
            row[field_index['email']],
            row[field_index['password']],
            row[field_index['port']]))
    
    return accounts

if __name__ == '__main__':
    start_collecting(sys.argv[1], sys.argv[2])
