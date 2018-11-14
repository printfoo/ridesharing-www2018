#!/usr/bin/env python

"""
./collect_border_data.py ../raw_data/uber_accounts.txt sim 2> log.txt
"""

import sys, os
import multiprocessing as mp
import subprocess as sp
import time, csv
from datetime import datetime
import pdb

def start_collecting(account_path, mode):
    accounts = load_accounts(account_path)
    """
    LonLat:
    Manhattan: -74.02121512345876, 40.876199202324564,
    """
    """
    Interval:
    SF: 0.0078
    Manhattan: 0.0041
    """
    grid = generate_grid(-74.02121512345876, 40.876199202324564,
    msize = 43, interval = 0.0041)
    lon_lat = trim_grid(grid, city = 'manhattan')
    groups = group_measuring_points(lon_lat)

    for index, group in enumerate(groups):
        if index < 8: continue
        index_str = str(index)
        while (datetime.now().hour not in (5, 17, 21, 22)):
            time.sleep(600)

        outdir = os.path.join('ping_reply', index_str)

        jobs = []

        for (user, email, password, port), (lon, lat, x, y) in zip(accounts, group):
            p = mp.Process(target = do_simulation,
                args = (user, email, password, mode, port,
                lon, lat, 'stay', outdir, x, y))
            jobs.append(p)
            p.start()

        for p in jobs:
            p.join()
                
def do_simulation(user, email, password, mode, port,
    lon, lat, mv_dir, outdir, x, y):
    print user + " starts Ubering."
    sim_uber = sp.Popen(['python', 'simulate_uber.py', user, email,
                        password, mode, port, str(lon), str(lat),
                        mv_dir, outdir, str(x), str(y)])
    stdout_data, stderr_data = sim_uber.communicate()

def group_measuring_points(lon_lat):
    groups, prev_row, cur_group = [], [], []

    for cur_row in lon_lat:
        cur_row_index = 0

        for lon, lat, (x, y) in cur_row:
            if len(cur_group) >= 43:
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
