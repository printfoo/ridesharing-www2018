#!/usr/bin/env python

"""
./start_simulation.py ../raw_data/random_names.txt session
"""

import sys
import multiprocessing as mp
import subprocess as sp
import time, csv
import pdb

def start_simulation(account_path, mode):
    user_mov_dir = {'Heather_Fitzgerald': 'ur',
            'Irene_Sandoval': 'ul', 
            'Rafael_Mccarthy': 'dl', 
            'Salvador_Blair': 'dr'}

    accounts = load_accounts(account_path)
    
    jobs = []
    for (user, email, password, port) in accounts:
        if user not in user_mov_dir:
            continue

        mov_dir = user_mov_dir[user]
        p = mp.Process(target = do_simulation,
                args = (user, email, password, mode, port,
                '-73.97463812345876', '40.78883152324564', mov_dir))
        jobs.append(p)
        p.start()

def do_simulation(user, email, password, mode, port, lon, lat, mv_dir):
    print user + " starts Ubering."
    sim_uber = sp.Popen(['python', 'simulate_uber.py', user, email,
                        password, mode, port, str(lon), str(lat), mv_dir])
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
    start_simulation(sys.argv[1], sys.argv[2])
