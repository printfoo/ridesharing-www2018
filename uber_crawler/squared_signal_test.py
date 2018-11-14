#!/usr/bin/env python

"""
./squared_signal_test.py ../raw_data/uber_accounts.txt sim 2> log.txt
"""

import sys, os
import multiprocessing as mp
import subprocess as sp
import time, csv
from datetime import datetime
import pdb

def start_testing(account_path, mode):
    accounts = load_accounts(account_path)
    ndemog, cdemog = build_demog(accounts, -73.94880312345876,
    40.81649320232456, interval = 0.0041)

#    for i, j, k in ndemog + cdemog:
#        print j[1], ',', j[0]

    cur_epoch = str(int(time.time()))
    outdir = os.path.join('ping_reply', cur_epoch)

    jobs = []

    for (user, email, password, port), (lon, lat, x, y), \
    (ping_low, ping_high) in ndemog + cdemog:
        p = mp.Process(target = do_simulation,
            args = (user, email, password, mode, port,
            lon, lat, 'stay', outdir, x, y, ping_low, ping_high))
        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()

def build_demog(accounts, clon, clat, interval = 0.0041):
    nlon_lat = \
    [(clon + interval, clat, 1, 0), (clon, clat + interval, 0, 1),
    (clon - interval, clat, -1, 0), (clon, clat - interval, 0, -1),
    (clon + interval, clat + interval, 1, 1),
    (clon - interval, clat + interval, -1, 1),
    (clon - interval, clat - interval, -1, -1),
    (clon + interval, clat - interval, 1, -1),
    (clon + 2 * interval, clat, 2, 0), (clon - 2 * interval, clat, -2, 0),
    (clon, clat + 2 * interval, 0, 2), (clon, clat - 2 * interval, 0, -2)]

    cdemog, ndemog = [], []

    while len(nlon_lat):
        cur_account = accounts.pop()
        cur_pos = nlon_lat.pop()

        ndemog.append((cur_account, cur_pos, (1440, 1440)))

    while len(accounts):
        cur_account = accounts.pop()
        cdemog.append((cur_account, (clon, clat, 0, 0), (1080, 1080)))

    return ndemog, cdemog
                
def do_simulation(user, email, password, mode, port,
    lon, lat, mv_dir, outdir, x, y, ping_low, ping_high):
    print user + " starts Ubering."
    sim_uber = sp.Popen(['python', 'simulate_uber.py', user, email,
                        password, mode, port, str(lon), str(lat),
                        mv_dir, outdir, str(x), str(y),
                        str(ping_low), str(ping_high)])
    stdout_data, stderr_data = sim_uber.communicate()

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
    start_testing(sys.argv[1], sys.argv[2])
