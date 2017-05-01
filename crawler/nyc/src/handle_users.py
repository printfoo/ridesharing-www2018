import random

"""
Load users' information.
"""
def load_users(users_file_path = None):
    users = []
    users_tel_num = []
    users_id = []
    lyft_token = []
    line_index = 0

    # Read users' info and token
    with open(users_file_path, "r") as users_file:
        while True:
            line = users_file.readline().strip("\n")
            if not line:
                break
            if line_index % 3 == 0:
                users_tel_num.append(line)
            elif line_index % 3 == 1:
                users_id.append(line)
            elif line_index % 3 == 2:
                lyft_token.append(line)
            line_index = line_index + 1

    # Add info to users
    for i in range(len(users_tel_num)):
        users.append((users_tel_num[i], users_id[i], lyft_token[i]))

    return users

"""
Generate user grid.
"""
def generate_user_grid(users = None, area = None, tunnel = [],
                       sta_lat = 0, end_lat = 0, num_lat = 0,
                       sta_lng = 0, end_lng = 0, num_lng = 0):

    # Insert location in grid and matching users
    grid = []
    port_num = len(tunnel)

    for i in range(num_lat): # every n * len is a latitude change
        for j in range(num_lng): # every n is a longitude change
            this_lat = sta_lat + i * (end_lat - sta_lat) / (num_lat - 1)
            this_lng = sta_lng + j * (end_lng - sta_lng) / (num_lng - 1)
            if check_valid(area = area, lat = this_lat, lng = this_lng):
                grid.append((users[0][0], # Telephone number
                             users[0][1], # User ID
                             users[0][2], # Lyft Token
                             str(this_lat), # Latitude
                             str(this_lng), # Longitude
                             tunnel[(i * num_lat + j) % port_num][0], # Port number
                             tunnel[(i * num_lat + j) % port_num][1], # Assigned IP
                             area + '-' + "{:0>2d}".format(i) + '-' + "{:0>2d}".format(j), # Index
                             ))
                users.remove(users[0])
    
    return grid, users

"""
Check if a location is needed in the user grid.
"""
def check_valid(area = None, lat = 0, lng = 0):
    if area == "LM":
        # 12: West coast, 3: East coast, 45: South coast, 67: Central park
        if (1.40624 * lng - lat + 144.82552 < 0) or \
           (5.25021 * lng - lat + 429.30936 < 0) or \
           (1.12784 * lng - lat + 124.17075 > 0 and lng > -73.97408) or \
           (0.13132 * lng - lat + 50.42611 > 0 and lng < -73.97408 and lng > -73.99983) or \
           (0.51474 * lng - lat + 78.79910 > 0 and lng < -73.99983) or \
           (1.34670 * lng - lat + 140.40177 > 0 and 1.39551 * lng - lat + 143.99228 < 0 and \
            -0.35733 * lng - lat + 14.33022 < 0 and -0.40454 * lng - lat + 10.88242 > 0):
            return False
        else:
            return True
    if area == "UM":
        # 1: Southwest coner, 2: West coast, 3: East coast, 4: Northeast boarder
        if (lat < 40.79874 and lng < -73.94404) or \
           (1.61248 * lng - lat + 160.07916 < 0) or \
           (0.29951 * lng - lat + 62.93848 > 0 and lng > -73.92910) or \
           (-0.38574 * lng - lat + 12.38386 < 0):
            return False
        else:
            return True
    if area == "BQ":
        # 1234: West coast, 5: North coast, 6: Southeast boarder
        if (0.95213 * lng - lat + 111.15978 < 0) or \
           (2.76225 * lng - lat + 245.02484 < 0 and -0.30617 * lng - lat + 18.05004 < 0) or \
           (1.06845 * lng - lat + 119.75676 < 0) or \
           (0.72043 * lng - lat + 93.97466 < 0 and -0.58672 * lng - lat + -2.74466 > 0) or \
           (-0.479 * lng - lat + 5.38433 < 0) or \
           (1.13202 * lng - lat + 124.41461 > 0):
            return False
        else:
            return True
    if area == "SI" or area == "JK":
        return True

"""
Update a user's information.
"""
def update_user(user = None, users = None):
    old_user = user
    old_users = users[:]

    # Update tel number, id and token to new one
    try:
        another = users[random.randint(0, len(users)-1)]
        new_user = (another[0], another[1], another[2],
                    user[3], user[4], user[5], user[6], user[7])
        users.remove(another)
        users.append((user[0], user[1], user[2]))
    except ValueError:
        return old_user, old_users
    
    return new_user, users
