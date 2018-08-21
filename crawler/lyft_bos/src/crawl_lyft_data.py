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

    # Generate a grid of users & location & assigned ip @MA
    grid, users = generate_user_grid(
        users = users, area = "MA", tunnel = tunnel,
        sta_lat = 40.704, end_lat = 40.797, num_lat = 24,
        sta_lng = -74.016, end_lng = -73.948, num_lng = 16)

    for user in grid:
        print user[3] + ", " + user[4] + ", " + user[7]
    for user in grid:
        print user[3] + ", " + user[4]

    # Print infomation
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
