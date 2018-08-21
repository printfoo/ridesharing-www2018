import requests, time, json, random, uuid, os, string
from handle_users import update_user

global null, true, false
null = None
true = True
false = False

"""
Send requests.
"""
def send_requests(users = None, user = None, run = None, data_path = None):

    # Start session
    lyft_ses = requests.Session()
    output_path = data_path + user[7]
    proxies = {'http': 'socks5://127.0.0.1:' + user[5],
               'https': 'socks5://127.0.0.1:' + user[5]}
    u = str(uuid.uuid1())
    app_id = u[:8] + u[9:13] + u[14:18] + u[19:23] + u[24:]
    i = 0

    # Sent requests in sequence and save responses
    while True:
        i = i + 1
        start_time = time.time()

        # Change a .txt file every 90 minutes
        if i % 1080 == 1:
            file_time = str(int(start_time * 1000))

        # Get jsons of 3 requests to send
        drivers_url, drivers_headers, drivers_params = sim_request(
            action = "get_drivers", lyft_token = user[2],
            lat = user[3], lng = user[4])
        eta_url, eta_headers, eta_params = sim_request(
            action = "get_eta", lyft_token = user[2],
            lat = user[3], lng = user[4])
        cost_url, cost_headers, cost_params = sim_request(
            action = "get_cost", lyft_token = user[2],
            lat = user[3], lng = user[4])

        # Send 3 requests in sequence
        try:
            drivers_response = lyft_ses.request(
                method = "GET", url = drivers_url, headers = drivers_headers,
                params = drivers_params, proxies = proxies)
            eta_response = lyft_ses.request(
                method = "GET", url = eta_url, headers = eta_headers,
                params = eta_params, proxies = proxies)
            cost_response = lyft_ses.request(
                method = "GET", url = cost_url, headers = cost_headers,
                params = cost_params, proxies = proxies)
        except requests.exceptions.ConnectionError:
            # Update SSH tunnel
            tunnel_connect(port = user[5], ip = user[6])
            continue

        # Creat filefolder and slow start
        if i == 1:
            if not os.path.isdir(output_path):
                os.mkdir(output_path)
            sleep_time = 20 - (time.time() - start_time)
            if sleep_time > 0:
                time.sleep(sleep_time)

        # If response is too short, it is wrong, update user info
        if len(drivers_response.content) < 1000 or \
           drivers_response.status_code > 399 or \
           len(eta_response.content) < 100 or \
           eta_response.status_code > 399 or \
           len(cost_response.content) < 500 or \
           cost_response.status_code > 399:
            print drivers_response.content
            print eta_response.content
            print cost_response.content
            print drivers_response.status_code
            print eta_response.status_code
            print cost_response.status_code
            print len(drivers_response.content)
            print len(eta_response.content)
            print len(cost_response.content)
            user, users = update_user(user = user, users = users)
            with open(output_path + "/log.txt", "a") as log:
                log_data = "User at " + str(user[7]) + " is not working, a new one is updated."
                print log_data
                log.write(log_data + "\n")
                log.flush()
            continue

        # Build data to save
        try:
            output_drivers = json.loads(drivers_response.content)
            output_eta = json.loads(eta_response.content)
            output_cost = json.loads(cost_response.content)
        except ValueError:
            continue
        try:
            json_data = {"index": i,
                         "time": start_time,
                         "user": {"tel": user[0], "id": user[1], "lat": user[3], "lng": user[4]},
                         "drivers": output_drivers["nearby_drivers"],
                         "eta": output_eta["eta_estimates"],
                         "cost": output_cost["cost_estimates"],
                         }
        except KeyError:
            continue
        output_data = json.dumps(json_data)
        
        # Write output in file
        with open(output_path + "/" + file_time + ".txt", "a") as output:
            output.write(output_data + "\n")
            output.flush()
        with open(output_path + "/log.txt", "a") as log:
            log_data = "Successfully saved data no." + str(i) + " for user at " + str(user[7]) + "!"
            log.write(log_data + "\n")
            log.flush()

        # Sleep 5 seconds every request
        sleep_time = 5 - (time.time() - start_time)
        if sleep_time > 0:
            time.sleep(sleep_time)

        # Detect mode
        if run == "once":
            if i >= 1:
                break
        elif run == "minute":
            if i >= 12:
                break
        elif run == "hour":
            if i >= 720:
                break
        elif run == "day":
            if i >= 17280:
                break
        elif run == "always":
            continue
        else:
            print "Wrong input for mode!"
            break

"""
Simulate a request.
"""
def sim_request (action = None, users_id = None, lyft_token = None,
                 lat = None, lng = None, app_id = None):

    # PUT users' info every 5 seconds
    if action == "put_users":
        url = 'https://api.lyft.com/users/' + users_id + '/location'
        json = {
            "marker": {
                "lat": lat,
                "lng": lng
            },
            "locations": [
                {
                    "source": "polling_fg",
                    "lat": lat,
                    "recordedAt": time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
                    "lng": lng,
                    "speed": 0,
                    "accuracy": 100
                }
            ],
            "rideType": "lyft",
            "appInfoRevision": app_id,
        }
        headers = {
            'method': 'PUT',
            'scheme': 'https',
            'path': '/users/' + users_id + '/location',
            'authority': 'api.lyft.com',
            'content-type': 'application/json',
            'accept': 'application/vnd.lyft.app+json;version=43',
            'authorization': 'lyftToken ' + lyft_token,
            'user-device': 'iPhone8,1',
            'accept-encoding': 'gzip, deflate',
            'accept-language': 'zh_CN',
            'x-carrier': 'T-Mobile',
            'content-length': str(len(str(json))),
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
        }
        return url, headers, json

    # GET drivers' info every 5 seconds
    elif action == "get_drivers":
        url = 'https://api.lyft.com/v1/drivers'
        params = {
            'lat': lat,
            'lng': lng,
        }
        headers = {
            'method': 'GET',
            'scheme': 'https',
            'path': '/v1/drivers?lat=' + lat + '&lng=' + lng,
            'authority': 'api.lyft.com',
            'accept': 'application/vnd.lyft.app+json;version=43',
            'x-carrier': 'T-Mobile',
            'user-device': 'iPhone8,1',
            'accept-encoding': 'gzip, deflate',
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'authorization': 'lyftToken ' + lyft_token,
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
            'accept-language': 'zh_CN',
        }
        return url, headers, params

    # GET estimate time of arriving every 5 seconds
    elif action == "get_eta":
        url = 'https://api.lyft.com/v1/eta'
        params = {
            'lat': lat,
            'lng': lng,
        }
        headers = {
            'method': 'GET',
            'scheme': 'https',
            'path': '/v1/eta?lat=' + lat + '&lng=' + lng,
            'authority': 'api.lyft.com',
            'accept': 'application/vnd.lyft.app+json;version=43',
            'x-carrier': 'T-Mobile',
            'user-device': 'iPhone8,1',
            'accept-encoding': 'gzip, deflate',
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'authorization': 'lyftToken ' + lyft_token,
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
            'accept-language': 'zh_CN',
        }
        return url, headers, params

    # GET cost every 5 seconds
    elif action == "get_cost":
        url = 'https://api.lyft.com/v1/cost'
        params = {
            'show_route_invalid_ridetypes': 'true',
            'start_lat': lat,
            'start_lng': lng,
        }
        headers = {
            'method': 'GET',
            'scheme': 'https',
            'path': '/v1/cost?show_route_invalid_ridetypes=true&start_lat=' + lat + '&start_lng=' + lng,
            'authority': 'api.lyft.com',
            'accept': 'application/vnd.lyft.app+json;version=43',
            'x-carrier': 'T-Mobile',
            'user-device': 'iPhone8,1',
            'accept-encoding': 'gzip, deflate',
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'authorization': 'lyftToken ' + lyft_token,
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
            'accept-language': 'zh_CN',
        }
        return url, headers, params

    # Else
    else:
        return None, None, None

"""
SSH tunnel.
"""
def ssh_tunnel(azure_ip_path = None, port_base = 0):

    # First manually move the key and config file to .ssh/
    # Change the accessibility of SSH key
    os.system("chmod 400 .ssh/azure")

    # Start building tunnels
    with open(azure_ip_path, "r") as azure_ip:
        tunnel = []
        port_num = 0
        while True:
            ip = azure_ip.readline().strip('\n')
            if not ip:
                break
            port = str(port_base + port_num)
            tunnel_connect(port = port, ip = ip)
            port_num = port_num + 1
            tunnel.append((port,ip))

    return port_num, tunnel

"""
Connect one tunnel.
"""
def tunnel_connect(port = None, ip = None):
    cmd = "ssh -i .ssh/azure -fND 127.0.0.1:" + port + " -o StrictHostKeyChecking=no ride@" + ip
    print cmd
    os.system(cmd)
