#!/usr/bin/env python

import sys
from start_simulation import load_accounts
import pdb

def compute_neighbors(account_path, city):
    accounts = load_accounts(account_path)
    code_grid = gen_code_grid(10, 10)
    codes = trim_grid(code_grid, city = city)

    code_user_dict, user_neighbor_dict = {}, {}
    
    for (user, email, password, port), code in zip(accounts, codes):
        code_user_dict[code] = str((user.lower(), code))

    for code, user in code_user_dict.iteritems():
        neighbor_code = morph_code(code)
        neighbors = []

        for ncode in neighbor_code:
            try:
                neighbors.append(code_user_dict[ncode])
            except KeyError:
                continue

        user_neighbor_dict[user] = set(neighbors)

    for user, neighbors in user_neighbor_dict.iteritems():
        print user + ':' + ','.join(neighbors)
#    pdb.set_trace()
#    print str(user_neighbor_dict).lower()

def morph_code(code):
    i, j = code

    return [(i + 1, j), (i - 1, j), (i, j + 1), (i, j - 1),
            (i + 1, j + 1), (i + 1, j - 1), (i - 1, j + 1),
            (i - 1, j - 1)]

def gen_code_grid(row, col):
    code_grid = []
    for i in xrange(row):
        cur_row = []
        for j in xrange(col):
            cur_row.append((i, j))

        code_grid.append(cur_row)

    return code_grid

def trim_grid(grid, total = 43, city = 'sf'):
    lon_lat = []

    if city == 'sf':
        for i in grid[0][:5]: lon_lat.append(i)
        for i in grid[1][:6]: lon_lat.append(i)
        for i in grid[2][:7]: lon_lat.append(i)
        for i in grid[3][:7]: lon_lat.append(i)
        for i in grid[4][:7]: lon_lat.append(i)
        for i in grid[5][:7]: lon_lat.append(i)
        for i in grid[6][3:7]: lon_lat.append(i)
    elif city == 'nyc':
        for i in grid[0][2:3]: lon_lat.append(i)
        for i in grid[1][2:5]: lon_lat.append(i)
        for i in grid[2][1:5]: lon_lat.append(i)
        for i in grid[3][1:12]: lon_lat.append(i)
        for i in grid[4][:10]: lon_lat.append(i)
        for i in grid[5][2:9]: lon_lat.append(i)
        for i in grid[6][3:8]: lon_lat.append(i)
        for i in grid[7][4:8]: lon_lat.append(i)
    else:
        pdb.set_trace()
    
    return lon_lat

if __name__ == '__main__':
    compute_neighbors(sys.argv[1], sys.argv[2])
