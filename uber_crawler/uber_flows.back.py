from libmproxy.main import mitmproxy
from libmproxy import flow as mitm_flow
from UberMessage import UberMessage
import random, string, json
import os
import pdb

class UberFlow(object):
    def __init__(self, 
                 orig_flow = None, 
                 longitude = -71.09243813705454,
                 latitude = 42.33853460106919,
                 mov_dir = "",
                 city = ""):
        self.orig_flow = orig_flow
        self.new_flow_meta = {'longitude': longitude, 'latitude': latitude}
        self.spoofed_msgs = {'init_ping': None, 'norm_ping': None,
                             'init_rider_app': None, 'end_rider_app': None,}
        self.counter = random.randint(2000, 3000)
        self.session_start_epoch = 0
        self.mov_dir = mov_dir
        self.city = city
        self.eTags = {}
    
    def new_flow(self):
        total_ping = random.randint(1, 2)
        start, end = [], []
        
        """Build start"""
        start.append(self.spoofed_msgs['init_ping'].msg)
        start.append(self.spoofed_msgs['NearestCabRequest'].msg)
        start.append(self.spoofed_msgs['MapPageView'].msg)
        start.append(self.spoofed_msgs['get_path'].msg)
        
        random.shuffle(start)
        start.insert(0, self.spoofed_msgs['SessionStart'].msg)
        start.append(self.spoofed_msgs['init_rider_app'].msg)

        """Build ping"""
        """u/d = 0.0001/-0.0001 on lat, r/l = 0.0001/-0.0001 on lon"""
        if self.mov_dir == 'stay':
            pings = [self.spoofed_msgs['norm_ping'].msg] * total_ping
        elif self.mov_dir in ('u', 'ur', 'r', 'dr', 'd', 'dl', 'l', 'ul'):
            if self.mov_dir == 'u':
                lon_delta, lat_delta = 0, 0.0001
            elif self.mov_dir == 'ur':
                lon_delta, lat_delta = 0.0001, 0.0001
            elif self.mov_dir == 'r':
                lon_delta, lat_delta = 0.0001, 0
            elif self.mov_dir == 'dr':
                lon_delta, lat_delta = 0.0001, -0.0001
            elif self.mov_dir == 'd':
                lon_delta, lat_delta = 0, -0.0001
            elif self.mov_dir == 'dl':
                lon_delta, lat_delta = -0.0001, -0.0001
            elif self.mov_dir == 'l':
                lon_delta, lat_delta = -0.0001, 0
            elif self.mov_dir == 'ul':
                lon_delta, lat_delta = -0.0001, 0.0001

            pdb.set_trace()
        else:
            pdb.set_trace()

        """Build end"""
        end.append(self.spoofed_msgs['SessionLength'].msg)
        end.append(self.spoofed_msgs['norm_ping'].msg)
        end.append(self.spoofed_msgs['end_rider_app'].msg)

        print "Number of PingClients: " + str(total_ping)
        print json.dumps(self.new_flow_meta, indent=4)
        return start + pings + end

    def build_flow(self, mode):
        self.digest_flow()
        return self
    
    def digest_flow(self):
        for msg in self.orig_flow:
            ubermsg = UberMessage('request', msg)
            ubermsg.msg.response = None
            self.spoof(ubermsg)
            
    def spoof(self, ubermsg):
        action = ubermsg.get_action()
        content = ubermsg.get_content()
        
        """The following spoofs don't include time and city update.
        Time update should be done on the fly. 
        City update can be predefined.
        eTags in cashedMessages should be updated on the fly for PingClient"""
        if action == ('POST', 'eventName', u'SessionStart'):
            self.process_sessionStart(content)
        elif action == ('POST', 'eventName', u'NearestCabRequest'):
            self.process_NearestCabRequest(content)
        elif action == ('POST', 'eventName', u'MapPageView'):
            self.process_MapPageView(content)
        elif action[0] == 'GET' and action[1] == 'path':
            new_path = self.process_GET(ubermsg.msg_body.path).encode('utf8')
            ubermsg.msg_body.path = new_path
        elif action == ('POST', 'rider_app', 'rider_app'):
            rd_key = self.process_rider_app(content)
            self.set_spoofed_msg(rd_key, ubermsg, content)
        elif action == ('POST', 'messageType', u'PingClient'):
            ping_key = self.process_pingClient(content)
            if ping_key != "":
                self.set_spoofed_msg(ping_key, ubermsg, content,
                                     [('X-Uber-Token', content['token'])])
        elif action == ('POST', 'eventName', u'SessionLength'):
            self.process_SessionLength(content)
        elif action == ('POST', 'eventName', u'NearestCabRequest'):
            self.process_NearestCabRequest(content)
        elif action == ('POST', 'eventName', u'SignInRequest'):
            pass
        elif action == ('POST', 'eventName', u'SignInResponse'):
            pass
        elif action == ('POST', 'eventName', u'SignOut'):
            pass
        elif action == ('POST', 'messageType', u'SignUpRedirect'):
            pass
        else:
            pdb.set_trace()
        
        if action[0] == 'GET':
            self.spoofed_msgs['get_path'] = ubermsg
        elif action[2] not in (u'PingClient', 'rider_app'):
            self.set_spoofed_msg(action[2], ubermsg, content)
            
    def set_spoofed_msg(self, msg_name, ubermsg, content, opt_headers = []):
        ubermsg.set_content(content)
        ubermsg.set_headers('Content-Length', 
                            str(len(ubermsg.get_content('str'))))
        for k,v in opt_headers:
            ubermsg.set_headers(k,v)
        self.spoofed_msgs[msg_name] = ubermsg
            
    def process_SessionLength(self, content):
        self.process_MapPageView(content)
            
    def process_rider_app(self, content):
        events = content['events']
        new_events = []
        
        for event in events:
            self.update_val(event, 'counter', self.get_counter())
            
            self.update_val(event['location'], 'altitude', self.fuzz_alt())
        
            longitude, latitude = self.fuzz_geo()
            self.update_val(event['location'], 'lng', longitude)
            self.update_val(event['location'], 'lat', latitude)
            
            self.update_val(event['rider_app']['device'], 'id',
                            self.new_flow_meta['deviceIds']['uberId'])

            self.update_val(event['rider_app'], 'rider_id',
                            self.new_flow_meta['parameters']['clientUuid']) 
            
            self.update_val(event, 'session_id',
                            self.new_flow_meta['analyticsV2SessionId'])
            
            new_events.append(event)
        
        content['events'] = new_events
        
        ret_key = ""
        
        if self.spoofed_msgs['init_rider_app'] is None:
            ret_key = 'init_rider_app'
        elif self.spoofed_msgs['end_rider_app'] is None:
            ret_key = 'end_rider_app'
        else:
            pdb.set_trace()
        
        return ret_key
    
    def process_GET(self, path):
        longitude, latitude = self.fuzz_geo()
        
        new_path = path.split('?')[0]
        new_path += "?latitude=%s&longitude=%s&token=%s" \
        %(latitude, longitude, self.new_flow_meta['token'])
        
        return new_path
        
    def process_MapPageView(self, content):
        """Update properties."""
        self.auto_fill(self.new_flow_meta['parameters'],
                       content['parameters'])
        
        self.auto_fill(self.new_flow_meta['deviceIds'],
                       content['deviceIds'])

        self.update_val(content, 'analyticsV2SessionId',
                        self.new_flow_meta['analyticsV2SessionId'])
        
        longitude, latitude = self.fuzz_geo()
        self.update_val(content, 'location', [longitude, latitude])
            
    def process_NearestCabRequest(self, content):
        """Checkin requestGuid"""
        guid = content['parameters']['requestGuid']
        guid_new = self.permute_uber_str(guid).upper()
        self.insert_dict(self.new_flow_meta['parameters'],
                         'requestGuid', guid_new)
        
        """Update properties."""
        self.auto_fill(self.new_flow_meta['parameters'],
                       content['parameters'])
        
        self.auto_fill(self.new_flow_meta['deviceIds'],
                       content['deviceIds'])

        self.update_val(content, 'analyticsV2SessionId',
                        self.new_flow_meta['analyticsV2SessionId'])
        
        longitude, latitude = self.fuzz_geo()
        self.update_val(content, 'location', [longitude, latitude])
        
    def process_pingClient(self, content):
        ret_key = ""
        
        """Update properties in pingclient"""
        if self.spoofed_msgs['init_ping'] is None:
            """Checkin localefileMD5, token and cloudKitId"""
            loc_md5 = content['localeFileMD5']
            loc_md5_new = self.permute_uber_str(loc_md5).upper()
            self.insert_dict(self.new_flow_meta,
                             'localeFileMD5', loc_md5_new)
            
            token = content['token']
            token_new = self.permute_uber_str(token).lower()
            self.insert_dict(self.new_flow_meta, 'token', token_new)
            
            """Do update."""
            self.do_update_pingclient(content)
           
            if 'userIds' in content:
                content.pop('userIds')
            if 'eTags' in content['cachedMessages']:
                content['cachedMessages'].pop('eTags')

            ret_key = 'init_ping'
        elif self.spoofed_msgs['norm_ping'] is None:
            """Insert userIds from norm ping."""
            cloud_id = content['userIds']['cloudKitId']
            cloud_id_new = self.permute_uber_str(cloud_id).lower()
            self.insert_dict(self.new_flow_meta, 'cloudKitId', cloud_id_new)

            """Do update."""
            self.do_update_pingclient(content)

            self.update_val(content['userIds'], 'cloudKitId',
                            self.new_flow_meta['cloudKitId'])
            ret_key = 'norm_ping'
        
        return ret_key
    
    def do_update_pingclient(self, content):
        """Update pingclient common properties."""
        if content['altitude'] != 0:
            self.update_val(content, 'altitude', self.fuzz_alt())
        
        self.auto_fill(self.new_flow_meta['deviceIds'], 
                       content['deviceIds'])
        
        longitude, latitude = self.fuzz_geo()
        self.update_val(content, 'longitude', longitude)
        self.update_val(content, 'latitude', latitude)
        
        self.update_val(content, 'localeFileMD5', 
                        self.new_flow_meta['localeFileMD5'])
        
        self.update_val(content, 'token', self.new_flow_meta['token'])

    def process_sessionStart(self, content):
        """********Begin updating the parameter property.********"""
        parameters = content['parameters']
        self.insert_dict(self.new_flow_meta, 'parameters', {})
        para_meta = self.new_flow_meta['parameters']
        
        """Update the clientuuid.
        Renew a clientuuid still works, but token doesn't..."""
        client_uuid = parameters['clientUuid']
        client_uuid_new = self.permute_uber_str(client_uuid).lower()
        self.insert_dict(para_meta, 'clientUuid', client_uuid_new)
        
        """Update the session_hash."""
        session_hash = parameters['sessionHash']
        session_hash_new = \
        self.permute_uber_str(session_hash, doit = True).lower()
        self.insert_dict(para_meta, 'sessionHash', session_hash_new)
        
        """Update locationAltitude."""
        loc_altitude = parameters['locationAltitude']
        loc_altitude_new = self.random_uber_num(loc_altitude, 0.5, 2.0)
        self.insert_dict(para_meta, 'locationAltitude', loc_altitude_new)
        """*********Finish updating the parameter property.********"""            
        
        """*****Begin updating the analyticsV2SessionId property.*****"""
        session_id = content['analyticsV2SessionId']
        session_id_new = self.permute_uber_str(session_id, doit = True).upper()
        self.insert_dict(self.new_flow_meta, 
                         'analyticsV2SessionId', session_id_new)
        """*****Finish updating the analyticsV2SessionId property.*****"""
        
        """******Begin updating the deviceIds property.*****"""
        device_ids = content['deviceIds']
        self.insert_dict(self.new_flow_meta, 'deviceIds', {})
        dev_meta = self.new_flow_meta['deviceIds']
        
        """Update ad_id"""
        ad_id = device_ids['advertiserId']
        ad_id_new = self.permute_uber_str(ad_id).upper()
        self.insert_dict(dev_meta, 'advertiserId', ad_id_new)
        
        """Update uberId"""
        uber_id = device_ids['uberId']
        uber_id_new = self.permute_uber_str(uber_id).upper()
        self.insert_dict(dev_meta, 'uberId', uber_id_new)
        
        """Update authId"""
        auth_id = device_ids['authId']
        auth_id_new = self.permute_uber_str(auth_id).lower()
        self.insert_dict(dev_meta, 'authId', auth_id_new)
        """******Finish updating the deviceIds property.*****"""
        
        """Update content of sessionStart."""
        self.auto_fill(self.new_flow_meta['parameters'],
                       content['parameters'])
        
        self.auto_fill(self.new_flow_meta['deviceIds'],
                       content['deviceIds'])
        
        self.update_val(content, 'analyticsV2SessionId', 
                        self.new_flow_meta['analyticsV2SessionId'])
        
        longitude, latitude = self.fuzz_geo()
        self.update_val(content, 'location', [longitude, latitude])
        
    def update_val(self, dict_to, k, v):
        if k not in dict_to:
            pdb.set_trace()
        
        dict_to[k] = v
    
    def get_counter(self):
        counter = self.counter
        self.counter += 1
        return counter
    
    def fuzz_alt(self):
        alt = self.new_flow_meta['parameters']['locationAltitude']
        
        return self.random_uber_num(alt, 1 - 1e-6, 1 + 1e-6)
    
    def fuzz_geo(self):
        longitude = self.random_uber_num(self.new_flow_meta['longitude'],
                                         1 - 1e-6, 1 + 1e-6)
        latitude = self.random_uber_num(self.new_flow_meta['latitude'],
                                        1 - 1e-6, 1 + 1e-6)
        
        return longitude, latitude
            
    def auto_fill(self, dict_from, dict_to):
        for k in dict_to:
            if k in dict_from:
                self.update_val(dict_to, k, dict_from[k])

    def random_uber_num(self, orig_num, low, high):
        new_num = orig_num * random.uniform(low, high)
        new_num = float('{0:.14f}'.format(new_num))
        
        return new_num
    
    def permute_uber_str(self, orig_str, delim = '-', doit = False):
        str_parts = orig_str.split(delim)
        
        if len(str_parts) == 1:
            new_str = self.update_str(str_parts[0], 4, 8)
            if orig_str[0] == '_':
                new_str = '_' + new_str[1:]
        elif len(str_parts) > 1:
            new_str_parts = []
            
            for part in str_parts:
                part_new = self.update_str(part, 1, 3)
                new_str_parts.append(part_new)
            
            new_str = delim.join(new_str_parts)
        else:
            pdb.set_trace()
        
        if not doit:
            new_str = orig_str
            
        return new_str
    
    def update_str(self, old_str, min_up, max_up):
        str_list = list(old_str)
        indices = random.sample(range(0, len(str_list)), 
                                random.choice([min_up, max_up]))
        
        for index in indices:
            str_list[index] = random.choice(string.lowercase + string.digits)
        
        return ''.join(str_list)
    
    def insert_dict(self, _dict, _key, _val):
        if _key not in _dict:
            _dict[_key] = _val
        else:
            try:
                assert(_dict[_key] == _val)
            except:
                print _dict[_key], _val
    
class FlowReplayer(object):
    def __init__(self, flow_path, orig_flow):
        self.flow_path = ""
        self.orig_flow = None
        
    def build_flow(self, mode, longitude, latitude, mov_dir):
#         seq = self.get_flow_seq(self.orig_flow)
#         for i, j, k, l in seq:
#             print i, k, l
#         pdb.set_trace()
        uberflow = UberFlow(orig_flow = self.orig_flow,
			longitude = longitude,
			latitude = latitude,
                        mov_dir = mov_dir)
        return uberflow.build_flow(mode)
    
    def get_orig_flow(self):
        return self.orig_flow
    
    def load_saved_flow(self, flow_path, third_party = False):
        flow = []

        with open(flow_path, 'rb') as flowfile:
            freader = mitm_flow.FlowReader(flowfile)
            
            for msg in freader.stream():
                if third_party: pass
                elif '.uber.' not in msg.request.host: continue
                
                flow.append(msg)

        self.orig_flow = flow

    def get_flow_path(self, mode, user):
        if self.flow_path != "":
            return self.flow_path
            
        flow_dir = os.path.join('../flow_record/', user)
        if mode == 'login':
            flowpath = os.path.join(flow_dir, 'flow_login.flow')
        elif mode == 'session':
            flowpath = os.path.join(flow_dir, 'flow_session.flow')
        elif mode == 'logout':
            flowpath = os.path.join(flow_dir, 'flow_logout.flow')
        else:
            pdb.set_trace()
                           
        self.flow_path = flowpath                                 
        return flowpath
    
    def get_flow_seq(self, flow):
        flow_seq = []
        prev_epoch = 0

        for msg in flow:
            ubermsg = UberMessage('request', msg)
            cur_epoch = ubermsg.get_epoch()
            if cur_epoch is None:
                diff_epoch = None
            else:
                diff_epoch = cur_epoch - prev_epoch
                prev_epoch = cur_epoch

            flow_seq.append((ubermsg.get_action(), msg, cur_epoch, diff_epoch))
             
        return flow_seq
    
