from pyspark.sql import SparkSession
from pyspark import StorageLevel
from math import atan2
import pydoop.hdfs as hdfs
import time, sys

# This function returns the orientation of two points.
def get_orien(start_lat, start_lng, end_lat, end_lng):
    orien = int(atan2(end_lat - start_lat, end_lng - start_lng) * 180.0/3.14159265)
    return orien if orien >= 0 else 180 - orien

# Taxi: This function extract car information from each row.
def clean_telemetry_to_clean_trajectory(row):
    trajectory = []
    car_type = "Taxi"
    city = "SF"
    car_id = row["vehicle_id"]

    # Set start point and end point.
    try:
        start_time = int(time.mktime(time.strptime(row["time"],"%Y-%m-%d %H:%M:%S"))) + 10800
        start_lat = float(row["lat"])
        start_lng = float(row["lng"])
        end_time = int(time.mktime(time.strptime(row["next_time"],"%Y-%m-%d %H:%M:%S"))) + 10800
        end_lat = float(row["next_lat"])
        end_lng = float(row["next_lng"])
    except (TypeError, ValueError):
        return trajectory
    orien = str(get_orien(start_lat, start_lng, end_lat, end_lng)) # Compute orientation.

    # If invalid telemetry or this car is not avaliable.
    if float(car_id) < 0:
        return trajectory
    if (row["car_stat"] != "2" and row["next_car_stat"] != "2") or end_time - start_time > 10 * 60:
        return trajectory

    # Fill in missing points.
    timestamp = start_time
    while timestamp <= end_time:
        # Interpolate locations.
        lat = start_lat + (end_lat - start_lat) / (end_time - start_time) * (timestamp - start_time)
        lng = start_lng + (end_lng - start_lng) / (end_time - start_time) * (timestamp - start_time)
        # Build a row json for each trajectory.
        trajectory_row = {"timestamp": str(timestamp), "car_id": car_id, "car_type": car_type, "city": city, "lat": str(lat), "lng": str(lng), "orien": orien}
        trajectory.append(trajectory_row)
        timestamp += 1
    return trajectory

# Main function.
if __name__ == "__main__":

    # Get paths then delete old results.
    clean_telemetry_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/raw/taxi_sf_clean_telemetry"
    clean_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/taxi_sf_clean_trajectory"
    try:
        hdfs.rmr(clean_trajectory_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("clean_telemetry_to_clean_trajectory").getOrCreate()

    # Load telemetry csv as daraframe.
    clean_telemetries_df = spark.read.csv(clean_telemetry_path).persist(StorageLevel.DISK_ONLY)
    clean_telemetries_df = clean_telemetries_df.filter(clean_telemetries_df["_c29"] != "0 days 00:00:00.000000000")
    clean_telemetries_df = clean_telemetries_df.select(["_c7", "_c8", "_c10", "_c11", "_c12", "_c15", "_c25"]) \
        .withColumnRenamed("_c7", "vehicle_id").withColumnRenamed("_c10", "lat").withColumnRenamed("_c11", "lng") \
        .withColumnRenamed("_c12", "car_stat").withColumnRenamed("_c15", "time").withColumnRenamed("_c25", "next_time")

    # Self-join to get current location and next location.
    next_telemetries_df = clean_telemetries_df.select(["vehicle_id", "time", "lat", "lng", "car_stat"]) \
        .withColumnRenamed("time", "next_time").withColumnRenamed("lat", "next_lat").withColumnRenamed("lng", "next_lng") \
        .withColumnRenamed("car_stat", "next_car_stat")
    clean_telemetries_df = clean_telemetries_df.join(next_telemetries_df, ["vehicle_id", "next_time"], "left_outer")
    
    # Convert dataframe to RDD and fill in missing data points.
    clean_trajectories_rdd = clean_telemetries_df.rdd.flatMap(clean_telemetry_to_clean_trajectory).persist(StorageLevel.DISK_ONLY)
    
    # Transform to dataframe and save data.
    clean_trajectories_df = spark.createDataFrame(clean_trajectories_rdd)
    clean_trajectories_df.write.save(path = clean_trajectory_path, format = "json", mode = "overwrite")
