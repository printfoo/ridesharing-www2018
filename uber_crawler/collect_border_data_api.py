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

def start_collecting(account_path):
    tokens = \
    ['P048oVbIPEsVXWRxokCOvAIeIeuHwYeV_tNyFbAE',
    'PDzCBtMa1BFu_XOTZOLde58R3jygJ_hLp7zt77bv',
    'iXzDTF4MLZ8eZVPz_fN1D3bV-MfnVAIjwzRcZfTD',
    '7jk35PRJSU6gXgiZqUwWATx9U7RLHkXEXEsKzXH1',
    'Moiw3oaen5GacCCDZHnvkerOmrrFs5VySPKKDyeD',
    'FwJ7rtuN8wu1xh7kpx5t011kapmM3KzA1k1yO9so',
    'qbBhYqCDjkx4Lkt6r1xEWdTFcgbwVxokflSrjxMS']

    accounts = list(load_accounts(account_path))

#    """Manhattan Island."""
#    grid = generate_grid(-74.02121512345876, 40.876199202324564,
#    msize = 43, interval = 0.0041)
#    lon_lat = trim_grid(grid, city = 'manhattan')

#    """SF"""
#    grid = generate_grid(-122.51124312345876, 37.804643202324564,
#    msize = 20, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'sf')

#    """DC"""
#    grid = generate_grid(-77.12670312345876, 39.000000202324564,
#    msize = 30, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'dc')

#    """London"""
#    grid = generate_grid(-0.24305412345876, 51.559985202324564,
#    msize = 35, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'london')

#    """LA"""
#    grid = generate_grid(-118.66490812345876, 34.326328202324564,
#    msize = 200, interval = 0.01)
#    lon_lat = trim_grid(grid, city = 'la')

#    """Boston"""
#    grid = generate_grid(-71.19496412345876, 42.441319202324564,
#    msize = 30, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'boston')

#    """Chicago"""
#    grid = generate_grid(-87.94111412345876, 42.022238202324564,
#    msize = 50, interval = 0.01)
#    lon_lat = trim_grid(grid, city = 'chicago')

#    """Miami"""
#    grid = generate_grid(-80.37052812345876, 25.856610202324564,
#    msize = 30, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'miami')

#    """Paris"""
#    grid = generate_grid(2.25573612345876, 48.896228202324564,
#    msize = 45, interval = 0.0041)
#    lon_lat = trim_grid(grid, city = 'paris')

#    """Seattle"""
#    grid = generate_grid(-122.43546912345876, 47.735057202324564,
#    msize = 50, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'seattle')

#    """Bay Area"""
#    grid = generate_grid(-122.58102012345876, 38.007937202324564,
#    msize = 200, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'bay_area')

#    """nyc_complete"""
#    grid = generate_grid(-74.27850112345876, 40.934632202324564,
#    msize = 200, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'nyc_complete')

#    """Copenhagen"""
#    grid = generate_grid(12.46865712345876, 55.723605202324564,
#    msize = 100, interval = 0.0078)
#    lon_lat = trim_grid(grid, city = 'copenhagen')

#    """Houston"""
#    grid = generate_grid(-95.764511202324564, 29.87383812345876,
#    msize = 100, interval = 0.01)
#    lon_lat = trim_grid(grid, city = 'houston')

    """Houston"""
    grid = generate_grid(116.224543202324564, 40.01800212345876,
    msize = 100, interval = 0.01)
    lon_lat = trim_grid(grid, city = 'beijing')

    groups = group_measuring_points(lon_lat)

#    for gindex, group in enumerate(groups):
#        for lon, lat, x, y in group: print lat, lon, gindex, x, y
#        for lon, lat, x, y in group: print lat, ',', lon
#    pdb.set_trace()

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
        if group_index < 1:
            continue
        req_cnt = 0

        """Collecting 3 days of data."""
        while req_cnt < 864:
            cur_dir = os.path.join('mapping', str(group_index))
            if not os.path.isdir(cur_dir):
                os.makedirs(cur_dir)

            while True:
                cur_dt = datetime.now()
                if (cur_dt.minute - 4) % 5 == 0 and (cur_dt.second == 0):
                    break

                time.sleep(0.1)

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

            time.sleep(1)
            
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
                response = session.get(url = url, params = parameters,
                timeout = 10)
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
            if len(cur_group) >= 570:
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
    elif city == 'sf':
        lon_lat.append(grid[0][4:15])
        lon_lat.append(grid[1][4:16])
        lon_lat.append(grid[2][4:17])
        lon_lat.append(grid[3][0:17])
        lon_lat.append(grid[4][0:17])
        lon_lat.append(grid[5][0:17])
        lon_lat.append(grid[6][0:17])
        lon_lat.append(grid[7][1:18])
        lon_lat.append(grid[8][1:19])
        lon_lat.append(grid[9][1:18])
        lon_lat.append(grid[10][1:20])
        lon_lat.append(grid[11][1:20])
        lon_lat.append(grid[12][1:17])
        lon_lat.append(grid[13][1:16])
    elif city == 'dc':
        lon_lat.append(grid[1][9:12])
        lon_lat.append(grid[2][8:14])
        lon_lat.append(grid[3][7:15])
        lon_lat.append(grid[4][6:16])
        lon_lat.append(grid[5][5:17])
        lon_lat.append(grid[6][4:18])
        lon_lat.append(grid[7][3:19])
        lon_lat.append(grid[8][2:20])
        lon_lat.append(grid[9][1:22])
        lon_lat.append(grid[10][1:23])
        lon_lat.append(grid[11][3:24])
        lon_lat.append(grid[12][4:26])
        lon_lat.append(grid[13][8:27])
        lon_lat.append(grid[14][8:28])
        lon_lat.append(grid[15][10:29])
        lon_lat.append(grid[16][11:27])
        lon_lat.append(grid[17][13:26])
        lon_lat.append(grid[18][13:25])
        lon_lat.append(grid[19][14:24])
        lon_lat.append(grid[20][14:23])
        lon_lat.append(grid[21][13:22])
        lon_lat.append(grid[22][13:21])
        lon_lat.append(grid[23][13:20])
        lon_lat.append(grid[24][13:19])
        lon_lat.append(grid[25][13:18])
        lon_lat.append(grid[26][13:17])
        lon_lat.append(grid[27][13:16])
        lon_lat.append(grid[28][13:15])
    elif city == 'london':
        lon_lat.append(grid[0][:])
        lon_lat.append(grid[1][:])
        lon_lat.append(grid[2][:])
        lon_lat.append(grid[3][:])
        lon_lat.append(grid[4][:])
        lon_lat.append(grid[5][:])
        lon_lat.append(grid[6][:])
        lon_lat.append(grid[7][:])
        lon_lat.append(grid[8][:])
        lon_lat.append(grid[9][:])
        lon_lat.append(grid[10][:])
        lon_lat.append(grid[11][:])
        lon_lat.append(grid[12][:])
        lon_lat.append(grid[13][:])
        lon_lat.append(grid[14][:])
        lon_lat.append(grid[15][:])
    elif city == 'la':
        lon_lat.append(grid[0][13:25])
        lon_lat.append(grid[1][13:26])
        lon_lat.append(grid[2][13:26])
        lon_lat.append(grid[3][7:26])
        lon_lat.append(grid[4][6:26])
        lon_lat.append(grid[5][6:26])
        lon_lat.append(grid[6][5:40])
        lon_lat.append(grid[7][5:40])
        lon_lat.append(grid[8][5:40])
        lon_lat.append(grid[9][5:40])
        lon_lat.append(grid[10][5:40])
        lon_lat.append(grid[11][5:32])
        lon_lat.append(grid[12][5:35])
        lon_lat.append(grid[13][2:36])
        lon_lat.append(grid[14][:38])
        lon_lat.append(grid[15][:38])
        lon_lat.append(grid[16][:38])
        lon_lat.append(grid[17][:38])
        lon_lat.append(grid[18][:38])
        lon_lat.append(grid[19][19:52])
        lon_lat.append(grid[20][19:52])
        lon_lat.append(grid[21][19:52])
        lon_lat.append(grid[22][19:52])
        lon_lat.append(grid[23][19:52])
        lon_lat.append(grid[24][19:52])
        lon_lat.append(grid[25][11:52])
        lon_lat.append(grid[26][11:52])
        lon_lat.append(grid[27][11:52])
        lon_lat.append(grid[28][11:52])
        lon_lat.append(grid[29][11:52])
        lon_lat.append(grid[30][14:52])
        lon_lat.append(grid[31][15:52])
        lon_lat.append(grid[32][16:52])
        lon_lat.append(grid[33][17:52])
        lon_lat.append(grid[34][18:52])
        lon_lat.append(grid[35][19:52])
        lon_lat.append(grid[36][20:52])
        lon_lat.append(grid[37][21:52])
        lon_lat.append(grid[38][22:52])
        lon_lat.append(grid[39][23:52])
        lon_lat.append(grid[40][23:52])
        lon_lat.append(grid[41][23:52])
        lon_lat.append(grid[42][24:52])
        lon_lat.append(grid[43][24:52])
        lon_lat.append(grid[44][25:52])
        lon_lat.append(grid[45][25:52])
        lon_lat.append(grid[46][26:52])
        lon_lat.append(grid[47][26:52])
        lon_lat.append(grid[48][27:52])
        lon_lat.append(grid[49][27:52])
        lon_lat.append(grid[50][28:52])
        lon_lat.append(grid[51][28:52])
        lon_lat.append(grid[52][28:52])
        lon_lat.append(grid[53][28:52])
        lon_lat.append(grid[54][27:52])
        lon_lat.append(grid[55][26:52])
        lon_lat.append(grid[56][25:52])
        lon_lat.append(grid[57][25:48])
        lon_lat.append(grid[58][26:48])
        lon_lat.append(grid[59][26:48])
        lon_lat.append(grid[60][31:43])
        lon_lat.append(grid[61][33:39])
        lon_lat.append(grid[62][35:38])
    elif city == 'boston':
        lon_lat.append(grid[0][:])
        lon_lat.append(grid[1][:-1])
        lon_lat.append(grid[2][:-2])
        lon_lat.append(grid[3][:-3])
        lon_lat.append(grid[4][:-3])
        lon_lat.append(grid[5][:-3])
        lon_lat.append(grid[6][:-2])
        lon_lat.append(grid[7][:-1])
        lon_lat.append(grid[8][:-1])
        lon_lat.append(grid[9][:-1])
        lon_lat.append(grid[10][:-4])
        lon_lat.append(grid[11][:-3])
        lon_lat.append(grid[12][:-5])
        lon_lat.append(grid[13][:-6])
        lon_lat.append(grid[14][:-7])
        lon_lat.append(grid[15][:-10])
        lon_lat.append(grid[16][:-10])
        lon_lat.append(grid[17][:-9])
        lon_lat.append(grid[18][:-10])
        lon_lat.append(grid[19][:-10])
        lon_lat.append(grid[20][:-7])
        lon_lat.append(grid[21][:-7])
    elif city == 'chicago':
        lon_lat.append(grid[0][:-22])
        lon_lat.append(grid[1][:-22])
        lon_lat.append(grid[2][:-21])
        lon_lat.append(grid[3][:-21])
        lon_lat.append(grid[4][:-21])
        lon_lat.append(grid[5][:-20])
        lon_lat.append(grid[6][:-20])
        lon_lat.append(grid[7][:-20])
        lon_lat.append(grid[8][7:-20])
        lon_lat.append(grid[9][7:-19])
        lon_lat.append(grid[10][10:-19])
        lon_lat.append(grid[11][10:-18])
        lon_lat.append(grid[12][10:-18])
        lon_lat.append(grid[13][13:-17])
        lon_lat.append(grid[14][13:-17])
        lon_lat.append(grid[15][13:-17])
        lon_lat.append(grid[16][13:-17])
        lon_lat.append(grid[17][16:-17])
        lon_lat.append(grid[18][16:-17])
        lon_lat.append(grid[19][16:-16])
        lon_lat.append(grid[20][16:-15])
        lon_lat.append(grid[21][16:-14])
        lon_lat.append(grid[22][16:-14])
        lon_lat.append(grid[23][14:-13])
        lon_lat.append(grid[24][14:-12])
        lon_lat.append(grid[25][14:-12])
        lon_lat.append(grid[26][20:-11])
        lon_lat.append(grid[27][20:-10])
        lon_lat.append(grid[27][20:-10])
        lon_lat.append(grid[28][20:-8])
        lon_lat.append(grid[29][20:-8])
        lon_lat.append(grid[30][20:-8])
        lon_lat.append(grid[31][20:-8])
        lon_lat.append(grid[32][20:-8])
        lon_lat.append(grid[33][20:-6])
        lon_lat.append(grid[34][20:-5])
        lon_lat.append(grid[35][20:-5])
        lon_lat.append(grid[36][20:-5])
        lon_lat.append(grid[37][20:-5])
        lon_lat.append(grid[38][20:-5])
        lon_lat.append(grid[39][20:-5])
    elif city == 'miami':
        lon_lat.append(grid[0][:-4])
        lon_lat.append(grid[1][:-4])
        lon_lat.append(grid[2][:-4])
        lon_lat.append(grid[3][:-4])
        lon_lat.append(grid[4][:-4])
        lon_lat.append(grid[5][:-4])
        lon_lat.append(grid[6][:-5])
        lon_lat.append(grid[7][:-5])
        lon_lat.append(grid[8][:-5])
        lon_lat.append(grid[9][:-5])
        lon_lat.append(grid[10][:-6])
        lon_lat.append(grid[11][:-6])
        lon_lat.append(grid[12][:-6])
        lon_lat.append(grid[13][:-7])
        lon_lat.append(grid[14][:-7])
        lon_lat.append(grid[15][:-9])
        lon_lat.append(grid[16][:-10])
        lon_lat.append(grid[17][:-11])
        lon_lat.append(grid[18][:-12])
        lon_lat.append(grid[19][:-13])
        lon_lat.append(grid[20][:-13])
        lon_lat.append(grid[21][:-13])
        lon_lat.append(grid[22][:-14])
        lon_lat.append(grid[23][:-14])
        lon_lat.append(grid[24][:-15])
    elif city == 'paris':
        lon_lat.append(grid[0][14:-11])
        lon_lat.append(grid[1][12:-11])
        lon_lat.append(grid[2][11:-11])
        lon_lat.append(grid[3][9:-10])
        lon_lat.append(grid[4][8:-9])
        lon_lat.append(grid[5][7:-7])
        lon_lat.append(grid[6][5:-6])
        lon_lat.append(grid[7][4:-6])
        lon_lat.append(grid[8][3:-6])
        lon_lat.append(grid[9][2:-6])
        lon_lat.append(grid[10][:-6])
        lon_lat.append(grid[11][:-6])
        lon_lat.append(grid[12][:-6])
        lon_lat.append(grid[13][:-6])
        lon_lat.append(grid[14][:-6])
        lon_lat.append(grid[15][:-6])
        lon_lat.append(grid[16][5:-6])
        lon_lat.append(grid[17][8:-8])
        lon_lat.append(grid[18][11:-12])
        lon_lat.append(grid[19][16:-14])
        lon_lat.append(grid[20][18:-16])
    elif city == 'seattle':
        lon_lat.append(grid[0][8:])
        lon_lat.append(grid[1][8:])
        lon_lat.append(grid[2][8:])
        lon_lat.append(grid[3][8:])
        lon_lat.append(grid[4][7:])
        lon_lat.append(grid[5][5:])
        lon_lat.append(grid[6][4:])
        lon_lat.append(grid[7][4:])
        lon_lat.append(grid[8][4:])
        lon_lat.append(grid[9][2:])
        lon_lat.append(grid[10][1:])
        lon_lat.append(grid[11][3:])
        lon_lat.append(grid[12][3:])
        lon_lat.append(grid[13][4:])
        lon_lat.append(grid[14][7:])
        lon_lat.append(grid[15][10:])
        lon_lat.append(grid[16][12:])
        lon_lat.append(grid[17][12:])
        lon_lat.append(grid[18][13:])
        lon_lat.append(grid[19][6:])
        lon_lat.append(grid[20][4:])
        lon_lat.append(grid[21][3:])
        lon_lat.append(grid[22][4:])
        lon_lat.append(grid[23][5:])
        lon_lat.append(grid[24][5:])
        lon_lat.append(grid[25][5:])
        lon_lat.append(grid[26][5:])
        lon_lat.append(grid[27][6:])
        lon_lat.append(grid[28][5:])
        lon_lat.append(grid[29][6:])
        lon_lat.append(grid[30][7:])
        lon_lat.append(grid[31][9:])
        lon_lat.append(grid[32][10:])
        lon_lat.append(grid[33][10:])
        lon_lat.append(grid[34][9:])
        lon_lat.append(grid[35][9:])
        lon_lat.append(grid[36][9:])
        lon_lat.append(grid[37][8:])
        lon_lat.append(grid[38][9:])
        lon_lat.append(grid[39][11:])
        lon_lat.append(grid[40][11:])
        lon_lat.append(grid[41][11:])
        lon_lat.append(grid[42][12:])
        lon_lat.append(grid[33][14:])
    elif city == 'bay_area':
        lon_lat.append(grid[1][28:40])
        lon_lat.append(grid[2][29:40])
        lon_lat.append(grid[3][29:40])
        lon_lat.append(grid[4][28:40])
        lon_lat.append(grid[5][26:40])
        lon_lat.append(grid[6][24:40])
        lon_lat.append(grid[7][21:40])
        lon_lat.append(grid[8][21:35])
        lon_lat.append(grid[9][22:36])
        lon_lat.append(grid[10][23:37])
        lon_lat.append(grid[11][25:38])
        lon_lat.append(grid[12][25:39])
        lon_lat.append(grid[13][25:40])
        lon_lat.append(grid[14][35:41])
        lon_lat.append(grid[15][35:42])
        lon_lat.append(grid[16][34:43])
        lon_lat.append(grid[17][35:44])
        lon_lat.append(grid[18][35:46])
        lon_lat.append(grid[19][36:46])
        lon_lat.append(grid[20][36:47])
        lon_lat.append(grid[21][36:48])
        lon_lat.append(grid[22][36:49])
        lon_lat.append(grid[23][37:50])
        lon_lat.append(grid[24][35:51])
        lon_lat.append(grid[25][34:52])
        lon_lat.append(grid[26][31:53])
        lon_lat.append(grid[27][13:25])
        lon_lat.append(grid[27][33:54])
        lon_lat.append(grid[28][13:25])
        lon_lat.append(grid[28][33:55])
        lon_lat.append(grid[29][9:25])
        lon_lat.append(grid[29][32:56])
        lon_lat.append(grid[30][9:25])
        lon_lat.append(grid[30][37:57])
        lon_lat.append(grid[31][9:25])
        lon_lat.append(grid[31][38:58])
        lon_lat.append(grid[32][9:26])
        lon_lat.append(grid[32][39:58])
        lon_lat.append(grid[33][9:27])
        lon_lat.append(grid[33][40:59])
        lon_lat.append(grid[34][10:27])
        lon_lat.append(grid[34][42:59])
        lon_lat.append(grid[35][10:27])
        lon_lat.append(grid[35][42:60])
        lon_lat.append(grid[36][10:27])
        lon_lat.append(grid[36][43:60])
        lon_lat.append(grid[37][10:29])
        lon_lat.append(grid[37][43:60])
        lon_lat.append(grid[38][10:28])
        lon_lat.append(grid[38][44:60])
        lon_lat.append(grid[39][11:25])
        lon_lat.append(grid[39][45:68])
        lon_lat.append(grid[40][11:25])
        lon_lat.append(grid[40][50:69])
        lon_lat.append(grid[41][11:25])
        lon_lat.append(grid[41][51:69])
        lon_lat.append(grid[42][11:25])
        lon_lat.append(grid[42][52:68])
        lon_lat.append(grid[43][12:26])
        lon_lat.append(grid[43][53:65])
        lon_lat.append(grid[44][12:26])
        lon_lat.append(grid[44][55:66])
        lon_lat.append(grid[45][12:26])
        lon_lat.append(grid[45][55:67])
        lon_lat.append(grid[46][12:26])
        lon_lat.append(grid[46][55:68])
        lon_lat.append(grid[47][12:26])
        lon_lat.append(grid[47][55:69])
        lon_lat.append(grid[48][12:26])
        lon_lat.append(grid[48][56:70])
        lon_lat.append(grid[49][12:28])
        lon_lat.append(grid[49][56:71])
        lon_lat.append(grid[50][12:28])
        lon_lat.append(grid[50][56:72])
        lon_lat.append(grid[51][12:29])
        lon_lat.append(grid[51][56:73])
        lon_lat.append(grid[52][12:27])
        lon_lat.append(grid[52][56:74])
        lon_lat.append(grid[53][10:29])
        lon_lat.append(grid[53][56:75])
        lon_lat.append(grid[54][10:33])
        lon_lat.append(grid[54][56:76])
        lon_lat.append(grid[55][10:35])
        lon_lat.append(grid[55][57:77])
        lon_lat.append(grid[56][24:36])
        lon_lat.append(grid[56][57:80])
        lon_lat.append(grid[57][25:42])
        lon_lat.append(grid[57][58:80])
        lon_lat.append(grid[58][26:44])
        lon_lat.append(grid[58][59:81])
        lon_lat.append(grid[59][27:43])
        lon_lat.append(grid[59][59:82])
        lon_lat.append(grid[60][27:47])
        lon_lat.append(grid[60][60:83])
        lon_lat.append(grid[61][28:50])
        lon_lat.append(grid[61][61:84])
        lon_lat.append(grid[62][29:49])
        lon_lat.append(grid[62][61:85])
        lon_lat.append(grid[63][30:52])
        lon_lat.append(grid[63][61:86])
        lon_lat.append(grid[64][31:52])
        lon_lat.append(grid[64][61:86])
        lon_lat.append(grid[65][32:54])
        lon_lat.append(grid[65][61:86])
        lon_lat.append(grid[66][33:58])
        lon_lat.append(grid[66][68:86])
        lon_lat.append(grid[67][34:58])
        lon_lat.append(grid[67][68:86])
        lon_lat.append(grid[68][35:59])
        lon_lat.append(grid[68][69:86])
        lon_lat.append(grid[69][36:60])
        lon_lat.append(grid[69][69:86])
        lon_lat.append(grid[70][37:61])
        lon_lat.append(grid[70][69:87])
        lon_lat.append(grid[71][38:62])
        lon_lat.append(grid[71][72:88])
        lon_lat.append(grid[72][39:89])
        lon_lat.append(grid[73][40:90])
        lon_lat.append(grid[74][41:91])
        lon_lat.append(grid[75][51:92])
        lon_lat.append(grid[76][52:93])
        lon_lat.append(grid[77][53:94])
        lon_lat.append(grid[78][54:95])
        lon_lat.append(grid[79][55:96])
        lon_lat.append(grid[80][56:97])
        lon_lat.append(grid[81][57:98])
        lon_lat.append(grid[82][58:99])
        lon_lat.append(grid[83][59:100])
        lon_lat.append(grid[84][60:101])
        lon_lat.append(grid[85][61:102])
        lon_lat.append(grid[86][62:103])
        lon_lat.append(grid[87][63:104])
        lon_lat.append(grid[88][64:105])
        lon_lat.append(grid[89][65:106])
        lon_lat.append(grid[90][66:107])
        lon_lat.append(grid[91][67:108])
        lon_lat.append(grid[92][68:109])
        lon_lat.append(grid[93][69:110])
        lon_lat.append(grid[94][69:100])
        lon_lat.append(grid[95][69:101])
        lon_lat.append(grid[96][69:102])
        lon_lat.append(grid[97][69:103])
        lon_lat.append(grid[98][70:104])
        lon_lat.append(grid[99][71:105])
        lon_lat.append(grid[100][72:106])
        lon_lat.append(grid[101][75:106])
        lon_lat.append(grid[102][75:95])
        lon_lat.append(grid[103][90:96])
        lon_lat.append(grid[104][91:97])
    elif city == 'nyc_complete':
        lon_lat.append(grid[0][48:72])
        lon_lat.append(grid[1][48:72])
        lon_lat.append(grid[2][48:71])
        lon_lat.append(grid[3][47:70])
        lon_lat.append(grid[4][47:69])
        lon_lat.append(grid[5][47:68])
        lon_lat.append(grid[6][46:67])
        lon_lat.append(grid[7][46:66])
        lon_lat.append(grid[8][45:65])
        lon_lat.append(grid[9][45:64])
        lon_lat.append(grid[10][44:63])
        lon_lat.append(grid[11][43:62])
        lon_lat.append(grid[12][43:61])
        lon_lat.append(grid[13][43:60])
        lon_lat.append(grid[14][42:60])
        lon_lat.append(grid[15][41:61])
        lon_lat.append(grid[16][41:61])
        lon_lat.append(grid[17][40:56])
        lon_lat.append(grid[18][39:55])
        lon_lat.append(grid[19][38:75])
        lon_lat.append(grid[20][38:75])
        lon_lat.append(grid[21][37:75])
        lon_lat.append(grid[22][36:75])
        lon_lat.append(grid[23][35:75])
        lon_lat.append(grid[24][35:75])
        lon_lat.append(grid[25][35:75])
        lon_lat.append(grid[26][35:75])
        lon_lat.append(grid[27][35:75])
        lon_lat.append(grid[28][35:75])
        lon_lat.append(grid[29][35:75])
        lon_lat.append(grid[30][35:75])
        lon_lat.append(grid[31][35:75])
        lon_lat.append(grid[32][35:75])
        lon_lat.append(grid[33][35:75])
        lon_lat.append(grid[34][35:75])
        lon_lat.append(grid[35][35:75])
        lon_lat.append(grid[36][34:75])
        lon_lat.append(grid[37][33:75])
        lon_lat.append(grid[38][12:75])
        lon_lat.append(grid[39][11:75])
        lon_lat.append(grid[40][10:75])
        lon_lat.append(grid[41][10:75])
        lon_lat.append(grid[42][10:75])
        lon_lat.append(grid[43][10:30])
        lon_lat.append(grid[43][35:75])
        lon_lat.append(grid[44][10:29])
        lon_lat.append(grid[44][35:75])
        lon_lat.append(grid[45][10:28])
        lon_lat.append(grid[45][35:75])
        lon_lat.append(grid[46][10:26])
        lon_lat.append(grid[46][35:58])
        lon_lat.append(grid[47][10:25])
        lon_lat.append(grid[47][50:55])
        lon_lat.append(grid[48][9:24])
        lon_lat.append(grid[48][46:52])
        lon_lat.append(grid[49][8:23])
        lon_lat.append(grid[49][44:48])
        lon_lat.append(grid[50][5:21])
        lon_lat.append(grid[50][44:46])
        lon_lat.append(grid[51][5:20])
        lon_lat.append(grid[52][5:19])
        lon_lat.append(grid[53][5:15])
        lon_lat.append(grid[54][4:13])
        lon_lat.append(grid[55][3:11])
        lon_lat.append(grid[56][3:9])
    elif city == 'copenhagen':
        lon_lat.append(grid[0][0:17])
        lon_lat.append(grid[1][0:22])
        lon_lat.append(grid[2][0:20])
        lon_lat.append(grid[3][0:17])
        lon_lat.append(grid[4][0:21])
        lon_lat.append(grid[5][0:22])
        lon_lat.append(grid[6][0:23])
        lon_lat.append(grid[7][0:23])
        lon_lat.append(grid[8][0:23])
        lon_lat.append(grid[9][0:23])
        lon_lat.append(grid[10][0:24])
        lon_lat.append(grid[11][0:25])
        lon_lat.append(grid[12][0:25])
        lon_lat.append(grid[13][0:27])
    elif city == 'houston':
        lon_lat.append(grid[0][:70])
        lon_lat.append(grid[1][:70])
        lon_lat.append(grid[2][:70])
        lon_lat.append(grid[3][:70])
        lon_lat.append(grid[4][:70])
        lon_lat.append(grid[5][:70])
        lon_lat.append(grid[6][:70])
        lon_lat.append(grid[7][:70])
        lon_lat.append(grid[8][:70])
        lon_lat.append(grid[9][:70])
        lon_lat.append(grid[10][:70])
        lon_lat.append(grid[11][:70])
        lon_lat.append(grid[12][:70])
        lon_lat.append(grid[13][:70])
        lon_lat.append(grid[14][:70])
        lon_lat.append(grid[15][:70])
        lon_lat.append(grid[16][:70])
        lon_lat.append(grid[17][:70])
        lon_lat.append(grid[18][:70])
        lon_lat.append(grid[19][:70])
        lon_lat.append(grid[20][:70])
        lon_lat.append(grid[21][:70])
        lon_lat.append(grid[22][:70])
        lon_lat.append(grid[23][:70])
        lon_lat.append(grid[24][:70])
        lon_lat.append(grid[25][:70])
        lon_lat.append(grid[26][:70])
        lon_lat.append(grid[27][:70])
        lon_lat.append(grid[28][:70])
        lon_lat.append(grid[29][:70])
        lon_lat.append(grid[30][:70])
        lon_lat.append(grid[31][:70])
        lon_lat.append(grid[32][:70])
        lon_lat.append(grid[33][:70])
        lon_lat.append(grid[34][:70])
        lon_lat.append(grid[35][:70])
    elif city == 'beijing':
        lon_lat.append(grid[0][:35])
        lon_lat.append(grid[1][:35])
        lon_lat.append(grid[2][:35])
        lon_lat.append(grid[3][:35])
        lon_lat.append(grid[4][:35])
        lon_lat.append(grid[5][:35])
        lon_lat.append(grid[6][:35])
        lon_lat.append(grid[7][:35])
        lon_lat.append(grid[8][:35])
        lon_lat.append(grid[9][:35])
        lon_lat.append(grid[10][:35])
        lon_lat.append(grid[11][:35])
        lon_lat.append(grid[12][:35])
        lon_lat.append(grid[13][:35])
        lon_lat.append(grid[14][:35])
        lon_lat.append(grid[15][:35])
        lon_lat.append(grid[16][:35])
        lon_lat.append(grid[17][:35])
        lon_lat.append(grid[18][:35])
        lon_lat.append(grid[19][:35])
        lon_lat.append(grid[20][:35])
        lon_lat.append(grid[21][:35])
        lon_lat.append(grid[22][:35])
        lon_lat.append(grid[23][:35])
        lon_lat.append(grid[24][:35])
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
    start_collecting(sys.argv[1])
