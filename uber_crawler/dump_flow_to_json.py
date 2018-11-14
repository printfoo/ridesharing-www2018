#!/usr/bin/env python

import sys
from uber_flows import FlowReplayer
from UberMessage import UberMessage
import json
import pdb

def dump_flow_to_json(flow_path):
    flow_replay = FlowReplayer("", None)
    flow = flow_replay.load_saved_flow(flow_path = flow_path, set_self = False)
    with open('flow_json.txt', 'w') as f:
        for msg in flow:
            msg_json = stuff_msg_json(msg)
            json.dump(msg_json, f)
            f.write('\n')

def stuff_msg_json(msg):
    msg_json, ubermsg = {}, UberMessage('request', msg)

    msg_json['request'] = {'header': str(ubermsg.get_headers()),
                            'path': ubermsg.get_path(),
                            'content': ubermsg.get_content()}

    ubermsg.to_response()
    msg_json['response'] = {'header': str(ubermsg.get_headers())}

    try:
        content = ubermsg.get_content(gziped = True)
    except ValueError, e:
        if str(e) == 'No JSON object could be decoded':
            content = ''

    msg_json['response']['content'] = content

    return msg_json

if __name__ == "__main__":
    dump_flow_to_json(sys.argv[1])
