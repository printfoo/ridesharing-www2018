try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import json, gzip
import time, random
import pdb

class UberMessage(object):
    def __init__(self, mtype, msg):
        self.mtype = mtype
        self.msg = msg
        
        if self.mtype == 'request':
            self.msg_body  = self.msg.request
        elif self.mtype == 'response':
            self.msg_body  = self.msg.response
        else:
            pdb.set_trace()
        
        try:
            self.json_content = json.loads(
                    self.msg_body.content.decode('utf-8'))
        except ValueError:
            self.json_content = None

    def to_request(self):
        self.msg_body = self.msg.request

        try:
            self.json_content = json.loads(self.msg_body.content)
        except ValueError:
            self.json_content = None

        return self
            
    def to_response(self):
        self.msg_body = self.msg.response

        try:
            self.json_content = json.loads(self.msg_body.content)
        except ValueError:
            self.json_content = None

        return self

    def get_action(self):
        method, action = None, None
        
        if self.msg_body.method == 'GET':
            method = 'GET'
        elif self.msg_body.method == 'POST':
            method = 'POST'
            action = self.msg_body.url.strip('/').split('/')[-1]
        
        return method, action
    
    def get_path(self):
        return self.msg_body.path
    
    def get_method(self):
        return self.msg_body.method
    
    def get_headers(self):
        return self.msg_body.headers

    def get_content(self, ctype = 'json', gziped = False):
        content = None
        
        if ctype == 'str':
            content = self.msg_body.content
        elif ctype == 'json':
            content = self.json_content
        else:
            pdb.set_trace()
        
        if gziped:
            content = gzip.GzipFile('', 'rb', 9, 
                                    StringIO(self.msg_body.content)).read()
            if ctype == 'json':
                content = json.loads(content)
                
        return content
    
    def set_headers(self, k, v):
        if k in self.msg_body.headers:
            self.msg_body.headers[k] = v

        return self
        
    def set_content(self, content = None):
        if content:
            new_content = content
        else:
            new_content = self.json_content

        self.msg_body.content = \
                str.encode(json.dumps(new_content, separators = (',', ':')))

        return self
    
    def set_epoch(self, paras):
        action = self.get_action()

        if action[2] == 'SessionStart':
            self.json_content['epoch'] = int(time.time() * 1000)
            paras['epoch'] = self.json_content['epoch']
        elif action[2] in ('NearestCabRequest', 'MapPageView', 'SessionLength'):
            self.json_content['epoch'] = paras['epoch']
        elif action[2] == 'PingClient':
            print("Now sending a PingClient.")
            time.sleep(5)
            self.json_content['epoch'] = int(time.time() * 1000)
        elif action[0] == 'GET' and action[1] == 'path':
            pass
        elif action[2] == 'rider_app':
            events = self.json_content['events']
            new_events = []

            for event in events:
                try:
                    epoch_ms = event['epoch_ms']
                    event['epoch_ms'] = int(time.time() * 1000)
                except KeyError:
                    pass

                try:
                    sdiff = epoch_ms - event['session_start_time_ms']
                    event['session_start_time_ms'] = \
                    event['epoch_ms'] + sdiff
                except KeyError:
                    pass

                try:
                    fdiff = epoch_ms - event['foreground_start_time_ms']
                    event['foreground_start_time_ms'] = \
                    event['epoch_ms'] + fdiff
                except KeyError:
                    pass

                try:
                    ldiff = epoch_ms - event['last_user_action_epoch_ms']
                    event['last_user_action_epoch_ms'] = \
                    event['epoch_ms'] + ldiff
                except KeyError:
                    pass

                try:
                    gdiff = epoch_ms - event['location']['gps_time_ms']
                    event['location']['gps_time_ms'] = \
                    event['epoch_ms'] + gdiff
                except:
                    pass

                new_events.append(event)

            self.json_content['events'] = new_events
        else:
            pdb.set_trace()

        return self

    def set_city(self, paras):
        action = self.get_action()
        if action[2] == 'rider_app':
            events = self.json_content['events']
            new_events = []

            for event in events:
                event['rider_app']['city_name'] = paras['city']

                new_events.append(event)

            self.json_content['events'] = new_events

        return self

    def set_field(self, field_name, field_val):
        self.json_content[field_name] = field_val
        return self

    def set_eTags(self, paras):
        action = self.get_action()

        if action[2] == 'PingClient' and \
        'eTags' in self.json_content['cachedMessages']:
            self.json_content['cachedMessages']['eTags'] = \
            paras['eTags']

        return self

    """FIXME: Buggy! Should reset the lat/lon to init values after modifying."""
    def set_mov_delta(self, lon_delta, lat_delta):
        self.json_content['longitude'] = \
        self.random_geo(self.json_content['longitude']) + lon_delta
        self.json_content['latitude'] = \
        self.random_geo(self.json_content['latitude']) + lat_delta

        return self

    def random_geo(self, orig_num):
#        new_num = orig_num * random.uniform(1 - 1e-6, 1 + 1e-6)
#        new_num = float('{0:.14f}'.format(new_num))
        
        return orig_num


    def get_epoch(self):
        epoch = None
        
        if self.json_content is None:
            return None

        try:
            epoch = self.json_content['epoch']
        except KeyError: pass

        return epoch

    def get_content_property(self, property_name):
        pvalue = None
        
        if property_name in self.json_content:
            pvalue = self.json_content[property_name]
        
        return pvalue
    
    def view_msg(self, gziped = False):
        print("=" * 36 + "Headers" + "=" * 37)
        print(self.msg_body.headers)
        print("=" * 36 + "Content" + "=" * 37)
         
        if gziped:
            content = gzip.GzipFile('', 'rb', 9, 
                                    StringIO(self.msg_body.content)).read()
        else: content = self.msg_body.content
         
        print(json.dumps(json.loads(content), indent = 4))
