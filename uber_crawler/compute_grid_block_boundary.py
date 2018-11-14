#!/usr/bin/env python

import sys

def compute_grid_block_boundary(nyc_grid):
    nyc_interval = 0.0011

    with open(nyc_grid, 'r') as f:
        for line in f:

            lat, lon = line.strip().split(' ')
            lat, lon = float(lat), float(lon)

            upper_lat = lat + nyc_interval
            upper_lon = lon + nyc_interval
            lower_lat = lat - nyc_interval
            lower_lon = lon - nyc_interval

            sys.stdout.write("(%f, %f, %f, %f, %f, %f),\n"
            %(lat, lon, upper_lat, upper_lon, lower_lat, lower_lon))

if __name__ == '__main__':
    compute_grid_block_boundary(sys.argv[1])
