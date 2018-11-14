import time, sys, os, json, random
from uber_flows import FlowReplayer, UberFlow
from UberMessage import UberMessage
import requests
import pdb

class UberSimulator(object):
    def __init__(self,
                 user = "",
		 email = "",
		 password = "",
                 mobile = "",
                 mode = "",
                 port = 9500,
                 outdir = '/net/data/uber/uber_ride_nyc/',
                 longitude = 0,
                 latitude = 0,
                 mov_dir = "stay"):
        self._user = user
        self._email = email
        self._password = password
        self._mobile = mobile.replace('-', '')
        self._mode = mode
        self._port = port
        self._outdir = outdir
        self._replayflow = FlowReplayer(flow_path = "", orig_flow = None)
        self._live_paras = {'epoch': 0, 'city': "", 'eTags': {}}

        proxy_addr = 'socks5://127.0.0.1:%d' %self._port
        self._session = requests.session()
        self._session.proxies = {'http': proxy_addr, 'https': proxy_addr}
        
        self._longitude = longitude
        self._latitude = latitude
        self.lon_delta, self.lat_delta = self.cal_mov_delta(mov_dir)

        self._token = None

        flow_path = self._replayflow.get_flow_path(mode, user)

        if flow_path:
            self._replayflow.load_saved_flow(flow_path)
        else:
            pdb.set_trace()

    def cal_mov_delta(self, mov_dir):
        """u/d = 0.0001/-0.0001 on lat, r/l = 0.0001/-0.0001 on lon"""
        if mov_dir == 'stay':
            lon_delta, lat_delta = 0, 0
        elif mov_dir in ('u', 'ur', 'r', 'dr', 'd', 'dl', 'l', 'ul'):
            if mov_dir == 'u':
                lon_delta, lat_delta = 0, 0.0001
            elif mov_dir == 'ur':
                lon_delta, lat_delta = 0.0001, 0.0001
            elif mov_dir == 'r':
                lon_delta, lat_delta = 0.0001, 0
            elif mov_dir == 'dr':
                lon_delta, lat_delta = 0.0001, -0.0001
            elif mov_dir == 'd':
                lon_delta, lat_delta = 0, -0.0001
            elif mov_dir == 'dl':
                lon_delta, lat_delta = -0.0001, -0.0001
            elif mov_dir == 'l':
                lon_delta, lat_delta = -0.0001, 0
            elif mov_dir == 'ul':
                lon_delta, lat_delta = -0.0001, 0.0001
        else:
            pdb.set_trace()

        return lon_delta, lat_delta

    def get_IMEI(self, N = 14):
        def luhn_residue(digits):
            return sum(sum(divmod(int(d)*(1 + i%2), 10))
                    for i, d in enumerate(digits[::-1])) % 10

        part = ''.join(str(random.randrange(0,9)) for _ in range(N-1))
        res = luhn_residue('{}{}'.format(part, 0))
        return '{}{}'.format(part, -res%10)

    def run(self):
        if self._user not in os.listdir('tokens'):
            print("Request a new token")
            login_flow = self._replayflow.load_saved_flow(
                    './flow_proto/login.flow', set_self = False)
            login_msg = UberMessage('request', login_flow[0])

            # TODO: May need to permute device-id, authId, and permId
            """ Canccel Permuration """
            """
            dummy_flow = UberFlow()

            device_ids = login_msg.json_content['deviceIds']
            device_ids['advertiserId'] = \
            dummy_flow.permute_uber_str(device_ids['advertiserId'], doit = True)
            device_ids['uberId'] = \
            dummy_flow.permute_uber_str(device_ids['uberId'], doit = True)
            device_ids['authId'] = \
            dummy_flow.permute_uber_str(device_ids['authId'], doit = True)
            new_locale = dummy_flow.permute_uber_str(
            login_msg.json_content['localeFileMD5'], doit = True)
            """
            device_data = login_msg.json_content['deviceData']
            device_data['deviceLatitude'] = '%.7f' %self._latitude
            device_data['deviceLongitude'] = '%.7f' %self._longitude
            device_data['phoneNumber'] = self._mobile
            device_data['rooted'] = False

            device_data['deviceIds']['deviceImei'] = self.get_IMEI()
            self._imei = device_data['deviceIds']['deviceImei']
            device_data['imsi'] = \
                    ''.join([d for d in device_data['imsi'][:-6]] \
                    + [str(random.randrange(0, 9)) for _ in range(6)])
            self._imsi = device_data['imsi']

            login_msg.set_field('username', self._email) \
                    .set_field('password', self._password).set_content()

            login_msg.set_headers('x-uber-pin-location-latitude',
                            str(self._latitude)) \
                    .set_headers('x-uber-pin-location-longitude',
                            str(self._longitude)) \
                    .set_headers('x-uber-device-location-latitude',
                            str(self._latitude)) \
                    .set_headers('x-uber-device-location-longitude',
                            str(self._longitude)) \
                    .set_headers('x-uber-device-mobile',
                            self._mobile) \
                    .set_headers('x-uber-device-ids',
                            ''.join(login_msg.msg_body.headers \
                                    ['x-uber-device-ids'] \
                                    .split('deviceImei:')[:1] \
                                    + ['deviceImei:'] + [self._imei])) \
                    .set_headers('x-uber-device-epoch',
                            str(int(time.time() * 1000))) \
                    .set_headers('Content-Length',
                            str(len(login_msg.get_content('str'))))

            lurl = login_msg.msg_body.url
            ldata = login_msg.msg_body.content
            lheaders = {h[0]: h[1] for h in login_msg.msg_body.headers.items()}

            login_res = \
            self._session.post(lurl, data = ldata, headers = lheaders)

            try:
                self._token = \
                        json.loads(login_res.content.decode('utf-8'))['token']
            except:
                raise Exception(self._email, 
                    " token failed " + str(login_res.content))

            with open(os.path.join('./tokens', self._user), 'w') as f:
                f.write(self._token + '\t' + self._imei + '\t' + self._imsi)
        else:
            print("Load an old token.")
            with open(os.path.join('./tokens', self._user), 'r') as f:
                self._token, self._imei, self.imsi = \
                        f.readline().strip().split('\t')

        while True:
            print("Starting a new session flow.")
            uberflow = self._replayflow.build_flow(
                    mode = self._mode,
                    longitude = self._longitude,
                    latitude = self._latitude,
                    token = self._token,
                    imei = self._imei,
                    mobile = self._mobile)
            new_flow = uberflow.new_flow()

            user_dir = os.path.join(self._outdir, self._user)

            if not os.path.exists(user_dir):
                os.makedirs(user_dir)

            cur_epoch = int(time.time() * 1000)
            fname = os.path.join(user_dir, "%d.txt" %(cur_epoch))
            print("Writing ping replies to " + fname)

            with open(fname, 'w') as f:
                for req_msg in new_flow:
                    ubermsg = UberMessage('request', req_msg)
                    action = ubermsg.get_action()

                    print(int(time.time() * 1000), self._user, action, 
                        self._latitude, self._longitude, self._port)

                    """Final update."""
                    time.sleep(5)
                    ubermsg.set_headers('x-uber-device-epoch',
                            str(int(time.time() * 1000))) \
                    .set_content() \
                    .set_headers('Content-Length',
                        str(len(ubermsg.get_content('str'))))

                    url = ubermsg.msg_body.url
                    data = ubermsg.msg_body.content
                    headers = {h[0]: h[1] for h in \
                            ubermsg.msg_body.headers.items()}

                    """Post a Uber message."""
                    try:
                        res_msg = \
                        self._session.post(url, data = data, headers = headers)
                    except Exception as e:
                        sys.stderr.write(self._user + ': ' + str(e) + '\n')
                        time.sleep(20)
                        continue

                    if action == ('POST', 'status'):
                        try:
                            res_json = \
                                    json.loads(res_msg.content.decode('utf-8'))

                            f.write(str(int(time.time() * 1000)) + ':::::')
                            json.dump(res_json, f)
                            f.write('\n')
                        except Exception as e:
                            sys.stderr.write(self._user + "json parse error.\n")
#                            sys.stderr.write(self._user + ': ' + str(e))
#                            print('\n')
#                            sys.stderr.write(res_msg.content)
#                            print('\n')
                            time.sleep(20)
                            continue

            time.sleep(5)
#            """This break is for measuring radius."""
#            break
