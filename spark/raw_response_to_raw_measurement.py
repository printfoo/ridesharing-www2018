from pyspark.sql import SparkSession, Row
from pyspark import StorageLevel 
import pydoop.hdfs as hdfs
import ujson as json
import re2 as re
import time, sys

# This function returns a user dictionary of a given txt file.
def get_user_info(user_info_path = None):
    user_info = {}

    # Open file and read line by line.
    with open(user_info_path, "r") as user_info_file:
        while True:
            line = user_info_file.readline().strip("\n")
            if not line:
                break
            a_user = line.replace(" ", "").split(",")
            user_info[a_user[2]] = {"user_info": a_user[2], "lat": a_user[0], "lng": a_user[1]}
    return user_info

# Uber SF: This function extract measurement information from each raw response.
def uber_sf_raw_response_to_raw_measurement(response, user):
    measurement = []
    timestamp = str(response[:10])
    car_type = "UberX"
    city = "SF"

    # Convert each response to json.
    try:
        response = json.loads(response[18:])
    except ValueError:
        return measurement
    # If invalid response.
    if "eyeball" not in response.keys():
        return measurement
    if "8" not in response["eyeball"]["nearbyVehicles"].keys():
        return measurement
    # Get measurement information.
    if "averageEta" in response["eyeball"]["nearbyVehicles"]["8"].keys():
        eta = str(response["eyeball"]["nearbyVehicles"]["8"]["averageEta"])
    else:
        eta = "100000"
    if "8" in response["eyeball"]["dynamicFares"].keys():
        surge = str(float(response["eyeball"]["dynamicFares"]["8"]["multiplier"]))
    else:
        surge = "1.00"
    # Build a row json for each measurement point.
    measurement_row = {"timestamp": timestamp, "car_type": car_type, "city": city, "user_info": str(user["user_info"]), "lat": str(user["lat"]), "lng": str(user["lng"]), "eta": eta, "surge": surge}
    measurement.append(measurement_row)
    return measurement

# Uber NYC: This function extract measurement information from each raw response.
def uber_nyc_raw_response_to_raw_measurement(response, user):
    measurement = []
    timestamp = str(response[:10])
    car_type = "UberX"
    city = "NYC"

    # Convert each response to json.
    try:
        response = json.loads(response[18:])
    except ValueError:
        return measurement
    # If invalid response.
    if "eyeball" not in response.keys():
        return measurement
    if "39" not in response["eyeball"]["nearbyVehicles"].keys():
        return measurement
    # Get measurement information.
    if "averageEta" in response["eyeball"]["nearbyVehicles"]["39"].keys():
        eta = str(response["eyeball"]["nearbyVehicles"]["39"]["averageEta"])
    else:
        eta = "100000"
    if "39" in response["eyeball"]["dynamicFares"].keys():
        surge = str(float(response["eyeball"]["dynamicFares"]["39"]["multiplier"]))
    else:
        surge = "1.00"
    # Build a row json for each measurement point.
    measurement_row = {"timestamp": timestamp, "car_type": car_type, "city": city, "user_info": str(user["user_info"]), "lat": str(user["lat"]), "lng": str(user["lng"]), "eta": eta, "surge": surge}
    measurement.append(measurement_row)
    return measurement

# Lyft SF: This function extract measurement information from each raw response.
def lyft_sf_raw_response_to_raw_measurement(response, user):
    measurement = []
    car_type = "Lyft"
    city = "SF"

    # Convert each response to json.
    try:
        response = json.loads(response)
    except ValueError:
        return measurement
    # If invalid response.
    if "closestDriverEta" not in response["rideTypes"][1].keys():
        return measurement
    if "pricing" not in response["rideTypes"][1].keys():
        return measurement
    # Get measurement information.
    timestamp = str(int(time.mktime(time.strptime(response["time"],"%Y-%m-%dT%H:%M:%SZ"))) - 18000)
    if "closestDriverEta" in response["rideTypes"][1].keys():
        eta = str(response["rideTypes"][1]["closestDriverEta"])
    else:
        eta = "100000"
    if "dynamicPricing" in response["rideTypes"][1]["pricing"].keys():
        raw_surge = response["rideTypes"][1]["pricing"]["dynamicPricing"]
        surge = str(1 + float(raw_surge)/100)
    else:
        surge = "1.00"
    # Build a row json for each measurement point.
    measurement_row = {"timestamp": timestamp, "car_type": car_type, "city": city, "user_info": str(user["user_info"]), "lat": str(user["lat"]), "lng": str(user["lng"]), "eta": eta, "surge": surge}
    measurement.append(measurement_row)
    return measurement    

# Lyft NYC: This function extract measurement information from each raw response.
def lyft_nyc_raw_response_to_raw_measurement(response, user):
    measurement = []
    car_type = "Lyft"
    city = "NYC"

    # Convert each response to json.
    try:
        response = json.loads(response)
    except ValueError:
        return measurement
    # Get measurement information.
    timestamp = str(int(response["time"]))
    try:
        eta = str(response["eta"][1]["eta_seconds"])
    except:
        eta = "100000"
    try:
        surge = str(response["cost"][1]["primetime_multiplier"])
    except:
        surge = "1.00"
    # Build a row json for each measurement point.
    measurement_row = {"timestamp": timestamp, "car_type": car_type, "city": city, "user_info": str(user["user_info"]), "lat": str(user["lat"]), "lng": str(user["lng"]), "eta": eta, "surge": surge}
    measurement.append(measurement_row)
    return measurement

# Main function.
if __name__ == "__main__":

    # Get commands and check if valid.
    try:
        app = sys.argv[1].lower()
        city = sys.argv[2].lower()
    except IndexError:
        print "Error in application name (Uber/Lyft) or city name (SF/NYC)."
        sys.exit()
    if app not in {"uber","lyft"} or city not in {"sf", "nyc"}:
        print "Error in application name (Uber/Lyft) or city name (SF/NYC)."
        sys.exit()    

    # Get paths then delete old results and create new path.
    file_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/raw/" + app + "_" + city + "_raw_response"
    raw_measurement_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_" + city + "_raw_measurement"
    user_info_path = "resources/" + city + "_user_info.txt"
    try:
        hdfs.rmr(raw_measurement_path)
    except:
        hdfs.mkdir(raw_measurement_path)

    # Get user info  dictionary.
    user_info = get_user_info(user_info_path = user_info_path)

    # Start spark SQL session.
    spark = SparkSession.builder.appName("raw_response_to_raw_measurement").getOrCreate()

    # Traverse each measurement location.
    for data_path in hdfs.ls(file_path):
        data_check = re.search("[A-Z][A-Z]-[0-9][0-9]-[0-9][0-9]", data_path)
        if not data_check:
            continue
        user = user_info[data_check.group()]
        # Load response data RDD.
        responses_rdd = spark.sparkContext.textFile(data_path).persist(StorageLevel.DISK_ONLY)
        # Convert response data RDD to trajectory data RDD.
        if app == "uber" and city == "sf":
            raw_measurements_rdd = responses_rdd.flatMap(lambda response: uber_sf_raw_response_to_raw_measurement(response, user)).persist(StorageLevel.DISK_ONLY)
        if app == "lyft" and city == "sf":
            raw_measurements_rdd = responses_rdd.flatMap(lambda response: lyft_sf_raw_response_to_raw_measurement(response, user)).persist(StorageLevel.DISK_ONLY)
        if app == "uber" and city == "nyc":
            raw_measurements_rdd = responses_rdd.flatMap(lambda response: uber_nyc_raw_response_to_raw_measurement(response, user)).persist(StorageLevel.DISK_ONLY)
        if app == "lyft" and city == "nyc":
            raw_measurements_rdd = responses_rdd.flatMap(lambda response: lyft_nyc_raw_response_to_raw_measurement(response, user)).persist(StorageLevel.DISK_ONLY)
        # Save data.
        raw_measurements_rdd.saveAsTextFile(raw_measurement_path + "/" + data_check.group())
