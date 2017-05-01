"""
Main function for data collection.
Usage: python ../crawl_lyft_data.py -r xx -d yy
"""
import sys
import multiprocessing as mp
from handle_users import load_users, generate_user_grid
from handle_requests import send_requests, ssh_tunnel

def crawl_lyft_data(file_path = None, data_path = None, run = None):

    # Create SSH tunnel
    azure_ip_path = file_path + '/resources/azure_ip.txt'
    port_base = 6000
    port_num, tunnel = ssh_tunnel(azure_ip_path = azure_ip_path,
                                  port_base = port_base)

    # Load users' information
    users_file_path = file_path + '/resources/lyft_users_file.txt'
    users = load_users(users_file_path = users_file_path)
    print "Number of avaliable users: " + str(len(users))

    # Generate a grid of users & location & assigned ip
    # SF starts at (37.710, -122.502) and ends at (37.805, -122.388)
    grid = []
    # South 5*10
    grid_sx, users = generate_user_grid(
        users = users, area = "SX", tunnel = tunnel,
        sta_lat = 37.710, end_lat = 37.744, num_lat = 5,
        sta_lng = -122.502, end_lng = -122.388, num_lng = 10)
    grid.extend(grid_sx)
    # Hunters Point 1*2 users
    grid_hp, users = generate_user_grid(
        users = users, area = "HP", tunnel = tunnel,
        sta_lat = 37.727, end_lat = 37.727, num_lat = 1,
        sta_lng = -122.378, end_lng = -122.368, num_lng = 2)
    grid.extend(grid_hp)
    # Northwest 7*7 users
    grid_nw, users = generate_user_grid(
        users = users, area = "NW", tunnel = tunnel,
        sta_lat = 37.751, end_lat = 37.805, num_lat = 7,
        sta_lng = -122.502, end_lng = -122.452, num_lng = 7)
    grid.extend(grid_nw)
    # Northeast 10*10 users
    grid_ne, users = generate_user_grid(
        users = users, area = "NE", tunnel = tunnel,
        sta_lat = 37.751, end_lat = 37.805, num_lat = 10,
        sta_lng = -122.445, end_lng = -122.388, num_lng = 10)
    grid.extend(grid_ne)
    # Downtown 6*6 additional users
    grid_dt, users = generate_user_grid(
        users = users, area = "DT", tunnel = tunnel,
        sta_lat = 37.772, end_lat = 37.803, num_lat = 6,
        sta_lng = -122.423, end_lng = -122.391, num_lng = 6)
    grid.extend(grid_dt)
    # Island 2*1 users
    grid_il, users = generate_user_grid(
        users = users, area = "IL", tunnel = tunnel,
        sta_lat = 37.810, end_lat = 37.823, num_lat = 2,
        sta_lng = -122.367, end_lng = -122.367, num_lng = 1)
    grid.extend(grid_il)
    print "Number of active users: " + str(len(grid))
    print "Number of left users: " + str(len(users))

    # Start processor to collect data
    for user in grid:
        # var "users" is all users, "user" is a user
        p = mp.Process(target = send_requests,
                       args = (users, user, run, data_path))
        p.start()

if __name__ == "__main__":

    # Get file path
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]

    # Get running time or help
    run = "once"
    try:
        if sys.argv[1] == "-r" or "--run":
            run = sys.argv[2]
            if run not in {"once", "minute", "hour", "day", "always"}:
                print "Wrong input for mode, use default."
                run = "once"
    except IndexError:
        print "No running time input, use default."

    # Get data path
    data_path = file_path + '/raw_data/'
    try:
        if sys.argv[3] == "-d" or "--data":
            data_path = sys.argv[4]
    except IndexError:
        print "No data path input, use default."

    crawl_lyft_data(file_path = file_path, data_path = data_path, run = run) 
