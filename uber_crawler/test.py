#!/usr/bin/env python

"""
./start_simulation.py ../raw_data/uber_accounts.txt sim 2> log.txt
"""

import sys
import multiprocessing as mp
import subprocess as sp
import time, csv
import pdb

def start_simulation(account_path, mode):
    accounts = load_accounts(account_path)
    """
    LonLat:
    SF: -122.44212712345876, 37.79872502324564
    NYC: -74.00003712345876, 40.77174202324564
    Manhattan: -74.00888312345876, 40.695541202324564
    """
    """
    Interval:
    SF: 0.0078
    NYC: 0.0041
    Manhattan: 0.0041
    """
#    grid = generate_grid(-122.44212712345876, 37.79872502324564)
#    lon_lat = trim_grid(grid)
    
    grid = generate_grid(-74.00003712345876, 40.77174202324564,
    msize = 10, interval = 0.0041)
    lon_lat = trim_grid(grid, city = 'nyc')

    jobs = []
    for (user, email, password, port), (lon, lat) in zip(accounts, lon_lat):
        p = mp.Process(target = do_simulation,
            args = (user, email, password, mode, port, lon, lat, 'stay'))
        jobs.append(p)
        p.start()
        
        time.sleep(0.1)

def do_simulation(user, email, password, mode, port, lon, lat, mv_dir):
    print user + " starts Ubering."
    sim_uber = sp.Popen(['python', 'simulate_uber.py', user, email,
                        password, mode, port, str(lon), str(lat), mv_dir])
    stdout_data, stderr_data = sim_uber.communicate()

def trim_grid(grid, total = 43, city = 'sf'):
    lon_lat = []

    if city == 'sf':
        for i in grid[0][:5]: lon_lat.append(i)
        for i in grid[1][:5]: lon_lat.append(i)
        for i in grid[2][:6]: lon_lat.append(i)
        for i in grid[3][:7]: lon_lat.append(i)
        for i in grid[4][:7]: lon_lat.append(i)
        for i in grid[5][:7]: lon_lat.append(i)
        for i in grid[6][1:7]: lon_lat.append(i)
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

def generate_grid(start_lon, start_lat, msize = 8, interval = 0.0078):
    grid = []

    for i in xrange(msize):
        grid.append([(start_lon, start_lat - (i * interval))])

    for row in grid:
        s_lon, s_lat = row[0]
        for i in xrange(1, msize):
            row.append((s_lon + (i * interval), s_lat))

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
    start_simulation(sys.argv[1], sys.argv[2])
