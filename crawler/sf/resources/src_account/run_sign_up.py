"""
Collect user information.
"""
import time, random, requests
from sim_request import sim_request

global true, false
true = True
false = False

def run_sign_up():
    user_num = 0
    ema6_token = ""
    lyft_ses = requests.Session()
    lat = str(random.uniform(37.7,37.8))
    lng = str(random.uniform(-122.5,-122.4))

    # Login in ema6.com
    ema6_login_url = "http://api.ema6.com:20161/Api/userLogin?uName=Semisappy&pWord=47566049&Developer=xP0RgpHmtjmd7ZxcLgPhfw%3d%3d"
    ema6_login_response = lyft_ses.request(method = "GET", url = ema6_login_url)
    if ema6_login_response.status_code == 200:
        print "Successfully login ema6.com!"
    else:
        return
    token_temp = ema6_login_response.content
    for letter in token_temp:
        if letter != "&":
            ema6_token = ema6_token + letter
        else:
            break
    
    # Start collecting users and tokens
    while True:
        restart = False

        # Release previous telephont numbers (ema6.com)
        ema6_rel_num_url = "http://api.ema6.com:20161/Api/userReleaseAllPhone?token=" + ema6_token
        ema6_rel_num_response = lyft_ses.request(method = "GET", url = ema6_rel_num_url)

        # GET a fake telephone number (ema6.com)
        ema6_tel_num_url = "http://api.ema6.com:20161/Api/userGetPhone?ItemId=27559&token=" + ema6_token + "&PhoneType=0"
        while True:
            ema6_tel_num_response = lyft_ses.request(method = "GET", url = ema6_tel_num_url)
            if ema6_tel_num_response.status_code == 200:
                ema6_tel_num = ema6_tel_num_response.content[0:11]
                print "Get a telephone number: +86" + ema6_tel_num + "..."
                break
            else:
                print "Try again for telephone number..."
                time.sleep(10)

        # POST telephone number (lyft.com)
        this_tel_num = "+86" + ema6_tel_num
        print "Using telephone number: " + this_tel_num + "..."
        tel_num_url, tel_num_headers, tel_num_json = sim_request(action = "post_tel_num", tel_num = this_tel_num)
        tel_num_response = lyft_ses.request(method = "POST", url = tel_num_url, headers = tel_num_headers, json = tel_num_json)
        if tel_num_response.status_code == 202:
            print "Telephone number is successfully posted!"
        else:
            print "Error, try again!"
            continue

        # GET verification code (ema6.com)
        waiting_veri_code_time = 0
        ema6_rec_sms_url = "http://api.ema6.com:20161/Api/userSingleGetMessage?token=" + ema6_token + "&itemId=27559&phone=" + ema6_tel_num
        while True:
            ema6_rec_sms_response = lyft_ses.request(method = "GET", url = ema6_rec_sms_url)
            sms_text = ema6_rec_sms_response.text
            if waiting_veri_code_time > 50:
                restart = True
                break
            elif "False" in sms_text:
                print "Trying again for verification code..."
                time.sleep(10)
                waiting_veri_code_time = waiting_veri_code_time + 10
            elif "Lyft" in sms_text:
                this_veri_code = sms_text[22:26]
                if this_veri_code.isdigit():
                    print "Get verification code: " + this_veri_code + "..."
                    break
                else:
                    restart = True
                    break
        if restart:
            continue
    
        # POST verification code (lyft.com)
        print "Using verification code: " + this_veri_code + "..."
        veri_code_url, veri_code_headers, veri_code_json = sim_request(action = "post_veri_code", tel_num = this_tel_num, veri_code = this_veri_code)
        veri_code_response = lyft_ses.request(method = "POST", url = veri_code_url, headers = veri_code_headers, json = veri_code_json)
        if veri_code_response.status_code == 200:
            print "Verification code is successfully posted!"
        else:
            print "Error, enter again!"
            continue

        # Remember this user ID and Lyft token (lyft.com)
        this_users_id = eval(veri_code_response.content)["user"]["id"]
        this_lyft_token = eval(veri_code_response.content)["user"]["lyftToken"]
    
        # PUT users' id (lyft.com)
        users_id_url, users_id_headers, users_id_json = sim_request(action = "put_users_id", tel_num = this_tel_num, users_id = this_users_id, lyft_token = this_lyft_token)
        users_id_response = lyft_ses.request(method = "PUT", url = users_id_url, headers = users_id_headers, json = users_id_json)
        if users_id_response.status_code == 200:
            print "User's ID is successfully put!"
        else:
            print "Error, start over!"
            continue

        # PUT users' info (lyft.com)
        users_info_url, users_info_headers, users_info_json = sim_request(action = "put_users_info", tel_num = this_tel_num, users_id = this_users_id, lyft_token = this_lyft_token)
        users_info_response = lyft_ses.request(method = "PUT", url = users_info_url, headers = users_info_headers, json = users_info_json)
        if users_info_response.status_code == 200:
            print "User's information is successfully put!"
        else:
            print "Error, start over!"
            continue
   
        # GET charge account (lyft.com)
        cha_acc_url, cha_acc_headers = sim_request(action = "get_charge_account", lyft_token = this_lyft_token)
        cha_acc_response = lyft_ses.request(method = "GET", url = cha_acc_url, headers = cha_acc_headers)
        if cha_acc_response.status_code == 200:
            print "Charge account is successfully gotten!"
        else:
            print "Error, start over!"
            continue
   
        # PUT users (lyft.com)
        users_url, users_headers, users_json = sim_request(action = "put_users", users_id = this_users_id, lyft_token = this_lyft_token, lat = lat, lng = lng)
        users_response = lyft_ses.request(method = "PUT", url = users_url, headers = users_headers, json = users_json)
        if users_response.status_code == 200:
            print "User is successfully put!"
        else:
            print "Error, start over!"
            continue

        # GET drivers (lyft.com)
        drivers_url, drivers_headers, drivers_params = sim_request(action = "get_drivers", lyft_token = this_lyft_token, lat = lat, lng = lng)
        drivers_response = lyft_ses.request(method = "GET", url = drivers_url, headers = drivers_headers, params = drivers_params)
        if drivers_response.status_code == 200:
            print "Drivers information is successfully gotten!"
        else:
            print "Error, start over!"
            continue

        # GET eta (lyft.com)
        eta_url, eta_headers, eta_params = sim_request(action = "get_eta", lyft_token = this_lyft_token, lat = lat, lng = lng)
        eta_response = lyft_ses.request(method = "GET", url = eta_url, headers = eta_headers, params = eta_params)
        if eta_response.status_code == 200:
            print "Eestimate time of arrival is successfully gotten!"
        else:
            print "Error, start over!"
            continue

        # GET cost (lyft.com)
        cost_url, cost_headers, cost_params = sim_request(action = "get_cost", lyft_token = this_lyft_token, lat = lat, lng = lng)
        cost_response = lyft_ses.request(method = "GET", url = cost_url, headers = cost_headers, params = cost_params)
        if cost_response.status_code == 200:
            print "Cost is successfully gotten!"
        else:
            print "Error, start over!"
            continue

        # Save user and token (lyft.com)
        with open('lyft_users_file.txt', 'a') as users_file:
            users_file.write(this_tel_num + "\n" + this_users_id + "\n" + this_lyft_token + "\n")
            user_num = user_num + 1
            print "Successfully saved " + str(user_num) + " users!"

        print "\n"

if __name__ == '__main__':
    run_sign_up()
