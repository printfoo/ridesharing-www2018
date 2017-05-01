"""
Set pamaters for requests.
"""
import random, time
from faker import Faker

global null, true, false
null = None
true = True
false = False

def sim_request (action = None, tel_num = None, veri_code = None,
                 users_id = None, lyft_token = None, lat = None, lng = None):

    fake = Faker()
    
    # POST telephone number at the first time to sign up
    if action == "post_tel_num":
        url = 'https://api.lyft.com/phoneauth'
        headers = {
            'method': 'POST',
            'scheme': 'https',
            'path': '/phoneauth',
            'authority': 'api.lyft.com',
            'accept': 'application/vnd.lyft.app+json;version=43',
            'content-type': 'application/json',
            'user-device': 'iPhone8,1',
            'x-carrier': 'T-Mobile',
            'accept-encoding': 'gzip, deflate',
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
            'accept-language': 'zh_CN',
            'content-length': '37',
        }
        json = {
            "phone": {
                "number": tel_num
            }
        }
        return url, headers, json

    # POST verification code at the first time to sign up
    elif action == "post_veri_code":
        url = 'https://api.lyft.com/users'
        headers = {
            'method': 'POST',
            'scheme': 'https',
            'path': '/users/',
            'authority': 'api.lyft.com',
            'accept': 'application/vnd.lyft.app+json;version=43',
            'content-type': 'application/json',
            'user-device': 'iPhone8,1',
            'x-carrier': 'T-Mobile',
            'accept-encoding': 'gzip, deflate',
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
            'accept-language': 'zh_CN',
            'content-length': '169',
        }
        json = {
            "matId": "9F5ED12B-B3C0-4A1C-BE08-F842A6EBB815",
            "phone": {
            "number": tel_num,
            "verificationCode": int(veri_code)
            },
            "location": {
            "lat": lat,
            "lng": lng
            }
        }
        return url, headers, json

    # PUT users' id at the first time to sign up
    elif action == "put_users_id":
        url = 'https://api.lyft.com/users/' + users_id
        headers = {
            'method': 'PUT',
            'scheme': 'https',
            'path': '/users/' + users_id,
            'authority': 'api.lyft.com',
            'content-type': 'application/json',
            'accept': 'application/vnd.lyft.app+json;version=43',
            'authorization': 'lyftToken ' + lyft_token,
            'user-device': 'iPhone8,1',
            'accept-encoding': 'gzip, deflate',
            'accept-language': 'zh_CN',
            'x-carrier': 'T-Mobile',
            'content-length': '37',
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
        }
        json = {
            "phone": {
                "number": tel_num
            }
        }
        return url, headers, json

    # PUT users' info at the first time to sign up
    elif action == "put_users_info":
        first_name = str(fake.first_name())
        last_name = str(fake.last_name())
        email = first_name + last_name + "{:0>4d}".format(random.randint(0,9999)) + "@gmail.com"
        url = 'https://api.lyft.com/users/' + users_id
        headers = {
            'method': 'PUT',
            'scheme': 'https',
            'path': '/users/' + users_id,
            'authority': 'api.lyft.com',
            'content-type': 'application/json',
            'accept': 'application/vnd.lyft.app+json;version=43',
            'authorization': 'lyftToken ' + lyft_token,
            'user-device': 'iPhone8,1',
            'accept-encoding': 'gzip, deflate',
            'accept-language': 'zh_CN',
            'x-carrier': 'T-Mobile',
            'content-length': '198',
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
        }
        json = {
            "firstName": first_name,
            "lastName": last_name,
            "termsAccepted": true,
            "phone": {
                "number": tel_num
            },
            "termsUrl": "https://s3.amazonaws.com/api.lyft.com/static/terms.html",
            "email": email
        }
        return url, headers, json

    # GET charge account when every time login
    elif action == "get_charge_account":
        url = 'https://api.lyft.com/chargeaccounts'
        headers = {
            'method': 'GET',
            'scheme': 'https',
            'path': '/chargeaccounts/',
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
        return url, headers

    # PUT users' info every 5 seconds
    elif action == "put_users":
        url = 'https://api.lyft.com/users/' + users_id + '/location'
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
            'content-length': '278',
            'user-agent': 'com.zimride.instant:iOS:9.3.5:4.0.3.40695',
            'x-session': 'eyJiIjoiRTg2NjUxNzktNjI3Qi00NURBLTgxNjgtRTY1QjJDNzBCOUExIiwiZSI6IjA5Mzc3MzM0LTlBRDQtNEYyMC1CNjQ2LTgzNUQ2RkZFMTY0QiIsImkiOnRydWV9',
        }
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
            "appInfoRevision": "02dc6d597219923b3773d2d166f73aa4"
        }
        return url, headers, json

    # GET drivers' info every 5 seconds
    elif action == "get_drivers":
        url = 'https://api.lyft.com/v1/drivers'
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
        params = {
            'lat': lat,
            'lng': lng,
        }
        return url, headers, params

    # GET estimate time of arriving every 5 seconds
    elif action == "get_eta":
        url = 'https://api.lyft.com/v1/eta'
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
        params = {
            'lat': lat,
            'lng': lng,
        }
        return url, headers, params

    # GET cost every 5 seconds
    elif action == "get_cost":
        url = 'https://api.lyft.com/v1/cost'
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
        params = {
            'show_route_invalid_ridetypes': 'true',
            'start_lat': lat,
            'start_lng': lng,
        }
        return url, headers, params
