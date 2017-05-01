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
            if num_lat != 1 and num_lng != 1:
                grid.append((users[0][0], # Telephone number
                             users[0][1], # User ID
                             users[0][2], # Lyft Token
                             str(sta_lat + i * (end_lat - sta_lat) / (num_lat - 1)), # Latitude
                             str(sta_lng + j * (end_lng - sta_lng) / (num_lng - 1)), # Longitude
                             tunnel[(i * num_lat + j) % port_num][0], # Port number
                             tunnel[(i * num_lat + j) % port_num][1], # Assigned IP
                             area + '-' + "{:0>2d}".format(i) + '-' + "{:0>2d}".format(j), # Index
                             ))
            if num_lat == 1:
                grid.append((users[0][0], # Telephone number
                             users[0][1], # User ID
                             users[0][2], # Lyft Token
                             str(sta_lat), # Latitude
                             str(sta_lng + j * (end_lng - sta_lng) / (num_lng - 1)), # Longitude
                             tunnel[(i * num_lat + j) % port_num][0], # Port number
                             tunnel[(i * num_lat + j) % port_num][1], # Assigned IP
                             area + '-' + "{:0>2d}".format(i) + '-' + "{:0>2d}".format(j), # Index
                             ))
            if num_lng == 1:
                grid.append((users[0][0], # Telephone number
                             users[0][1], # User ID
                             users[0][2], # Lyft Token
                             str(sta_lat + i * (end_lat - sta_lat) / (num_lat - 1)), # Latitude
                             str(sta_lng), # Longitude
                             tunnel[(i * num_lat + j) % port_num][0], # Port number
                             tunnel[(i * num_lat + j) % port_num][1], # Assigned IP
                             area + '-' + "{:0>2d}".format(i) + '-' + "{:0>2d}".format(j), # Index
                             ))
            users.remove(users[0])

    # Find and remove some points in ocean
    temp = grid[:]
    for user in grid:
        if (float(user[3]) > 37.79 and float(user[4]) < -122.48) \
           or ((float(user[3]) + float(user[4]) > -84.598) and (area != "IL")):
            temp.remove(user)
            users.append((user[0],user[1],user[2]))
    grid = temp[:]
    
    return grid, users

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
