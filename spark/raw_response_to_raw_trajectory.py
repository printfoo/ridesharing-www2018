from pyspark.sql import SparkSession
from pyspark import StorageLevel 
import pydoop.hdfs as hdfs
import ujson as json
import re2 as re
import time, sys

# Uber SF: This function extract car information from each raw response.
def uber_sf_raw_response_to_raw_trajectory(response):
    trajectory = []
    car_type = "UberX"
    city = "SF"

    # Convert each response to json.
    try:
        response = json.loads(response[18:])
    except ValueError:
        return trajectory
    # If invalid response.
    if "eyeball" not in response.keys():
        return trajectory
    if "8" not in response["eyeball"]["nearbyVehicles"].keys():
        return trajectory
    if "vehiclePaths" not in response["eyeball"]["nearbyVehicles"]["8"].keys():
        return trajectory
    # Traverse each car.
    for car_id in response["eyeball"]["nearbyVehicles"]["8"]["vehiclePaths"].keys():
        car_id_str = str(car_id)
        # Traverse each recorded location.
        for record in response["eyeball"]["nearbyVehicles"]["8"]["vehiclePaths"][car_id]:
            try:
                timestamp = str(record["epoch"])
                timestamp = timestamp[:len(timestamp)-3]
            except KeyError:
                continue
            lat = str(record["latitude"])
            lng = str(record["longitude"])
            try:
                orien = str(record["course"])
            except KeyError:
                orien = str(0)
            # Build a row json for each trajectory.
            trajectory_row = {"timestamp": timestamp, "car_id": car_id_str, "car_type": car_type, "city": city, "lat": lat, "lng": lng, "orien": orien}
            trajectory.append(trajectory_row)
    return trajectory

# Uber NYC: This function extract car information from each raw response.
def uber_nyc_raw_response_to_raw_trajectory(response):
    trajectory = []
    car_type = "UberX"
    city = "NYC"

    # Convert each response to json.
    try:
        response = json.loads(response[18:])
    except ValueError:
        return trajectory
    # If invalid response.
    if "eyeball" not in response.keys():
        return trajectory
    if "39" not in response["eyeball"]["nearbyVehicles"].keys():
        return trajectory
    if "vehiclePaths" not in response["eyeball"]["nearbyVehicles"]["39"].keys():
        return trajectory
    # Traverse each car.
    for car_id in response["eyeball"]["nearbyVehicles"]["39"]["vehiclePaths"].keys():
        car_id_str = str(car_id)
        # Traverse each recorded location.
        for record in response["eyeball"]["nearbyVehicles"]["39"]["vehiclePaths"][car_id]:
            try:
                timestamp = str(record["epoch"])
                timestamp = timestamp[:len(timestamp)-3]
            except KeyError:
                continue
            lat = str(record["latitude"])
            lng = str(record["longitude"])
            try:
                orien = str(record["course"])
            except KeyError:
                orien = str(0)
            # Build a row json for each trajectory.
            trajectory_row = {"timestamp": timestamp, "car_id": car_id_str, "car_type": car_type, "city": city, "lat": lat, "lng": lng, "orien": orien}
            trajectory.append(trajectory_row)
    return trajectory

# Lyft SF: This function extract car information from each raw response.
def lyft_sf_raw_response_to_raw_trajectory(response):
    trajectory = []
    car_type = "Lyft"
    city = "SF"

    # Convert each response to json.
    try:
        response = json.loads(response)
    except ValueError:
        return trajectory
    # If invalid response.
    if "drivers" not in response["rideTypes"][1].keys():
        return trajectory
    # Traverse each car.
    for car in response["rideTypes"][1]["drivers"]:
        car_id = car["id"]
        car_id_str = str(car_id)
        # Traverse each recorded location.
        for record in car["recentLocations"]:
            timestamp = record["recordedAt"]
            timestamp = str(int(time.mktime(time.strptime(timestamp,"%Y-%m-%dT%H:%M:%S+0000"))) - 18000)
            lat = str(record["lat"])
            lng = str(record["lng"])
            try:
                orien = str(int(record["bearing"]))
            except KeyError:
                orien = str(0)
            # Build a row json for each trajectory.
            trajectory_row = {"timestamp": timestamp, "car_id": car_id_str, "car_type": car_type, "city": city, "lat": lat, "lng": lng, "orien": orien}
            trajectory.append(trajectory_row)
    return trajectory

# Lyft NYC: This function extract car information from each raw response.
def lyft_nyc_raw_response_to_raw_trajectory(response):
    trajectory = []
    car_type = "Lyft"
    city = "NYC"

    # Convert each response to json.
    try:
        response = json.loads(response)
    except ValueError:
        return trajectory
    # If invalid response.
    try:
        test = response["drivers"][3]["drivers"]
    except IndexError:
        return trajectory
    # Traverse each car.
    for car in response["drivers"][3]["drivers"]:
        car_id = car["id"]
        car_id_str = str(car_id)
        # Traverse each recorded location.
        for record in car["locations"]:
            timestamp = str(int(record["recorded_at_ms"] / 1000))
            lat = str(record["lat"])
            lng = str(record["lng"])
            try:
                orien = str(int(record["bearing"]))
            except (KeyError, TypeError):
                orien = str(0)
            # Build a row json for each trajectory.
            trajectory_row = {"timestamp": timestamp, "car_id": car_id_str, "car_type": car_type, "city": city, "lat": lat, "lng": lng, "orien": orien}
            trajectory.append(trajectory_row)
    return trajectory

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
    file_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/raw/"+ app + "_" + city + "_raw_response"
    raw_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_" + city + "_raw_trajectory"
    try:
        hdfs.rmr(raw_trajectory_path)
    except:
        hdfs.mkdir(raw_trajectory_path)

    # Start spark SQL session.
    spark = SparkSession.builder.appName("raw_response_to_raw_trajectory").getOrCreate()

    # Traverse each measurement location.
    for data_path in hdfs.ls(file_path):
        data_check = re.search("[A-Z][A-Z]-[0-9][0-9]-[0-9][0-9]", data_path)
        if not data_check:
            continue
        # Load response data RDD.
        responses_rdd = spark.sparkContext.textFile(data_path).persist(StorageLevel.DISK_ONLY)
        # Convert response data RDD to trajectory data RDD.
        if app == "uber" and city == "sf":
            raw_trajectories_rdd = responses_rdd.flatMap(uber_sf_raw_response_to_raw_trajectory).persist(StorageLevel.DISK_ONLY)
        if app == "uber" and city == "nyc":
            raw_trajectories_rdd = responses_rdd.flatMap(uber_nyc_raw_response_to_raw_trajectory).persist(StorageLevel.DISK_ONLY)
        if app == "lyft" and city == "sf":
            raw_trajectories_rdd = responses_rdd.flatMap(lyft_sf_raw_response_to_raw_trajectory).persist(StorageLevel.DISK_ONLY)
        if app == "lyft" and city == "nyc":
            raw_trajectories_rdd = responses_rdd.flatMap(lyft_nyc_raw_response_to_raw_trajectory).persist(StorageLevel.DISK_ONLY)
        # Save data.
        raw_trajectories_rdd.saveAsTextFile(raw_trajectory_path + "/" + data_check.group())
