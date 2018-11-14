from mitmproxy.io import FlowReader
from UberMessage import UberMessage

import random, string, json
import os
import pdb

class UberFlow(object):
    def __init__(self, 
                 orig_flow = None, 
                 longitude = -71.09243813705454,
                 latitude = 42.33853460106919,
                 token = None,
                 city = "",
                 imei = "",
                 mobile = ""):
        self.orig_flow = orig_flow
        self.new_flow_meta = \
        {'longitude': longitude, 'latitude': latitude, 'token': token}
        self.spoofed_msgs = {'init_ping': None, 'norm_ping': None,
                             'init_rider_app': None, 'end_rider_app': None,}
        self.counter = random.randint(2000, 3000)
        self.session_start_epoch = 0
        self.city = city
        self.imei = imei
        self.mobile = mobile
        self.eTags = {}

    def new_flow(self):
        total_ping = random.randint(400, 1600)
 
        print("Number of PingClients: " + str(total_ping))
        print(json.dumps(self.new_flow_meta, indent=4))

        """Build ping"""
        for _ in range(random.randint(400, 1600)):
            yield self.spoofed_msgs['norm_ping'].msg

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

        """ Just spoof status update. 
        If not working, spoof all the messages."""
        if action == ('POST', 'status'):
            content['targetLocationSynced']['longitude'] = \
                    self.new_flow_meta['longitude']
            content['targetLocationSynced']['latitude'] = \
                    self.new_flow_meta['latitude']

            content['targetLocation']['longitude'] = \
                    self.new_flow_meta['longitude']
            content['targetLocation']['latitude'] = \
                    self.new_flow_meta['latitude']

            ubermsg.set_headers('x-uber-pin-location-latitude',
                            str(self.new_flow_meta['latitude'])) \
                    .set_headers('x-uber-pin-location-longitude',
                            str(self.new_flow_meta['longitude'])) \
                    .set_headers('x-uber-device-location-latitude',
                            str(self.new_flow_meta['latitude'])) \
                    .set_headers('x-uber-device-location-longitude',
                            str(self.new_flow_meta['longitude'])) \
                    .set_headers('x-uber-device-mobile', self.mobile) \
                    .set_headers('x-uber-device-ids',
                            ''.join(ubermsg.msg_body.headers \
                            ['x-uber-device-ids'].split('deviceImei:')[:1] \
                            + ['deviceImei:'] + [self.imei])) \
                    .set_headers('x-uber-token', self.new_flow_meta['token'])

            self.set_spoofed_msg('norm_ping', ubermsg, content)
        else:
            pass
            
    def set_spoofed_msg(self, msg_name, ubermsg, content, opt_headers = []):
        ubermsg.set_content(content)
        ubermsg.set_headers('Content-Length', 
                            str(len(ubermsg.get_content('str'))))
        for k,v in opt_headers:
            ubermsg.set_headers(k,v)
        self.spoofed_msgs[msg_name] = ubermsg
            
    def process_SessionLength(self, content):
        self.process_sessionStart(content)
            
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
        elif len(new_events) == 1:
            ret_key = 'end_rider_app'
        else:
            pdb.set_trace()
        
        return ret_key
    
    def process_GET(self, path):
        longitude, latitude = self.fuzz_geo()
        
        new_path = path.split('?')[0]
        new_path += "?latitude=%s&longitude=%s" \
        %(latitude, longitude)

        if 'token' in self.new_flow_meta:
            new_path += "&token=%s" %self.new_flow_meta['token']
        
        return new_path
        
    def process_MapPageView(self, content):
        self.process_sessionStart(content)
            
    def process_NearestCabRequest(self, content):
        if 'requestGuid' not in self.new_flow_meta['parameters']:
            self.update_guid_meta(content)

        self.process_sessionStart(content)

    def update_guid_meta(self, content):
        guid = content['parameters']['requestGuid']
        guid_new = self.permute_uber_str(guid, doit = True).upper()
        self.insert_dict(self.new_flow_meta['parameters'],
                         'requestGuid', guid_new)

    def process_pingClient(self, content):
        ret_key = ""
        
        """Update properties in pingclient"""
        if self.spoofed_msgs['init_ping'] is None:
            """Checkin localefileMD5, token and cloudKitId"""
            loc_md5 = content['localeFileMD5']
            loc_md5_new = self.permute_uber_str(loc_md5, doit = True).upper()
            self.insert_dict(self.new_flow_meta,
                             'localeFileMD5', loc_md5_new)
            
            """Do update."""
            self.do_update_pingclient(content)
           
            if 'userIds' in content:
                content.pop('userIds')
            if 'eTags' in content['cachedMessages']:
                content['cachedMessages'].pop('eTags')

            ret_key = 'init_ping'
        elif self.spoofed_msgs['norm_ping'] is None:
            if 'userIds' not in content:
                return ''

            """Insert userIds from norm ping."""
            cloud_id = content['userIds']['cloudKitId']
            cloud_id_new = self.permute_uber_str(cloud_id, doit = True).lower()
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
        if 'parameters' not in self.new_flow_meta:
            self.update_para_meta(content)

        self.auto_fill(self.new_flow_meta['parameters'],
                       content['parameters'])

        if 'deviceIds' not in self.new_flow_meta:
            self.update_device_meta(content)
        
        self.auto_fill(self.new_flow_meta['deviceIds'],
                       content['deviceIds'])
        
        if 'analyticsV2SessionId' not in self.new_flow_meta:
            self.update_session_meta(content)

        self.update_val(content, 'analyticsV2SessionId', 
                        self.new_flow_meta['analyticsV2SessionId'])
        
        longitude, latitude = self.fuzz_geo()
        self.update_val(content, 'location', [longitude, latitude])

    def update_para_meta(self, content):
        parameters = content['parameters']
        self.insert_dict(self.new_flow_meta, 'parameters', {})
        para_meta = self.new_flow_meta['parameters']
        
        """Update the clientuuid.
        Renew a clientuuid still works, but token doesn't..."""
        client_uuid = parameters['clientUuid']
        client_uuid_new = self.permute_uber_str(client_uuid, doit=True).lower()
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

    def update_device_meta(self, content):
        device_ids = content['deviceIds']
        self.insert_dict(self.new_flow_meta, 'deviceIds', {})
        dev_meta = self.new_flow_meta['deviceIds']
        
        """Update ad_id"""
        ad_id = device_ids['advertiserId']
        ad_id_new = self.permute_uber_str(ad_id, doit = True).upper()
        self.insert_dict(dev_meta, 'advertiserId', ad_id_new)
        
        """Update uberId"""
        uber_id = device_ids['uberId']
        uber_id_new = self.permute_uber_str(uber_id, doit = True).upper()
        self.insert_dict(dev_meta, 'uberId', uber_id_new)
        
        """Update authId"""
        auth_id = device_ids['authId']
        auth_id_new = self.permute_uber_str(auth_id, doit = True).lower()
        self.insert_dict(dev_meta, 'authId', auth_id_new)

    def update_session_meta(self, content):
        session_id = content['analyticsV2SessionId']
        session_id_new = self.permute_uber_str(session_id, doit = True).upper()
        self.insert_dict(self.new_flow_meta, 
                         'analyticsV2SessionId', session_id_new)
        
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
#        longitude = self.random_uber_num(self.new_flow_meta['longitude'],
#                                         1 - 1e-6, 1 + 1e-6)
#        latitude = self.random_uber_num(self.new_flow_meta['latitude'],
#                                        1 - 1e-6, 1 + 1e-6)
        
        return self.new_flow_meta['longitude'], self.new_flow_meta['latitude']
            
    def auto_fill(self, dict_from, dict_to):
        for k in dict_to:
            if k in dict_from:
                self.update_val(dict_to, k, dict_from[k])

    def random_uber_num(self, orig_num, low, high):
        new_num = orig_num * random.uniform(low, high)
        new_num = float('{0:.14f}'.format(new_num))
        
        return new_num
    
    def permute_uber_str(self, orig_str, delim = '-', doit = False):
        """Hard code: cancel randomization."""
        return orig_str

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
                print(_dict[_key], _val)
    
class FlowReplayer(object):
    def __init__(self, flow_path, orig_flow):
        self.flow_path = ""
        self.orig_flow = None
        
    def build_flow(self, mode, longitude, latitude, token, imei, mobile):
#         seq = self.get_flow_seq(self.orig_flow)
#         for i, j, k, l in seq:
#             print i, k, l
#         pdb.set_trace()
        uberflow = UberFlow(orig_flow = self.orig_flow,
			longitude = longitude,
			latitude = latitude,
			token = token,
                        imei = imei,
                        mobile = mobile)
        return uberflow.build_flow(mode)
    
    def get_orig_flow(self):
        return self.orig_flow
    
    def load_saved_flow(self, flow_path, third_party = False, set_self = True):
        flow = []

        with open(flow_path, 'rb') as flowfile:
            freader = FlowReader(flowfile)

            for msg in freader.stream():
                if third_party:
                    pass
                elif '.uber.' not in msg.request.host:
                    continue

                flow.append(msg)

        if set_self:
            self.orig_flow = flow
        else:
            return flow

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
        elif mode == 'sim':
            flowpath = "./flow_proto/session.flow"
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
    
