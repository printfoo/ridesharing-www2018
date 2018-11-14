import sys, json, time
import requesocks as requests
import pdb

port, token, lat, lon, fpath = sys.argv[1], sys.argv[2], sys.argv[3], 
sys.argv[4], sys.argv[5]

"""Build a socks session."""
proxy_addr = 'socks5://127.0.0.1:%s' %port
session = requests.session()
session.proxies = {'http': proxy_addr, 'https': proxy_addr}

url = 'https://api.uber.com/v1/estimates/price'

parameters = {
'server_token': token,
'start_latitude': lat,
'start_longitude': lon,
'end_latitude': lat,
'end_longitude': lon
}

with open(fpath, 'a') as f:
    while True:
        try:
            response = session.get(url = url, params = parameters)
            res_json = json.loads(response.content)
        except:
            time.sleep(5)
            continue

        f.write(str(int(time.time() * 1000)) + ':::::')
        json.dump(res_json, f)
        f.write('\n')

        break
