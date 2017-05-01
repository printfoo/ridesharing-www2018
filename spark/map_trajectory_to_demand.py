from pyspark.sql import SparkSession
from pyspark import StorageLevel
import pydoop.hdfs as hdfs
import shutil, sys

# Main function.
if __name__ == "__main__":

    # Get commands and check if valid.
    try:
        city = sys.argv[1].lower()
        goal = sys.argv[2].lower()
    except IndexError:
        print "Error in city name (SF/NYC) or goal (5min/10min)."
        sys.exit()
    if goal not in {"5min", "10min"} or city not in {"sf", "nyc"}:
        print "Error in city name (SF/NYC) or goal (5min/10min)."
        sys.exit()

    # Get paths then delete old results and create new path.
    map_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/" + city + "_map_trajectory"
    demand_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/results/" + city + "_demand_" + goal
    try:
        hdfs.rmr(demand_path)
    except:
        pass
    local_path = "results/" + city + "_demand_" + goal + ".csv"
    try:
        shutil.rmtree(local_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("map_trajectory_to_demand").getOrCreate()

    # Load map trajectory dataframes.
    map_df = spark.read.json(map_trajectory_path).persist(StorageLevel.DISK_ONLY)

    # Set time interval and the name of roundoff time column.
    if goal == "5min":
        time_interval = 300
    if goal == "10min":
        time_interval = 600
    old_col_name = "(timestamp - (timestamp % " + str(time_interval) + "))"

    # If get minute interval demand.
    if "min" in goal:
        # Uber and Lyft and Taxi.
        # 1 creat an index of all timestamp and all geo id;
        roundtime_df = map_df.select(map_df["timestamp"] - map_df["timestamp"] % time_interval).distinct().withColumnRenamed(old_col_name, "timestamp")
        index_df = roundtime_df.crossJoin(map_df.select("geo_id").distinct())
        # 2 get the last timestamp of each trip;
        # 3 join dataframe to get last record of each trip;
        last_df = map_df.select(map_df["timestamp"].cast("int"), map_df["car_id"]) \
            .groupBy("car_id").max("timestamp").withColumnRenamed("max(timestamp)", "timestamp").persist(StorageLevel.DISK_ONLY)
        last_df = last_df.join(map_df, ["timestamp", "car_id"], "left").persist(StorageLevel.DISK_ONLY)
        # 4 roundoff timestamp and get corresponding car id and geo id;
        # 5 get distinct timestamp, car id, geo id combination;
        # 6 count distinct car id in each time interval and geo id;
        uber_demand_df = last_df.filter(last_df["car_type"] == "UberX").select(last_df["timestamp"] - last_df["timestamp"] % time_interval, last_df["car_id"], last_df["geo_id"]) \
            .distinct().withColumnRenamed(old_col_name, "timestamp") \
            .groupBy("timestamp", "geo_id").count().withColumnRenamed("count", "uber_dem").persist(StorageLevel.DISK_ONLY)
        lyft_demand_df = last_df.filter(last_df["car_type"] == "Lyft").select(last_df["timestamp"] - last_df["timestamp"] % time_interval, last_df["car_id"], last_df["geo_id"]) \
            .distinct().withColumnRenamed(old_col_name, "timestamp") \
            .groupBy("timestamp", "geo_id").count().withColumnRenamed("count", "lyft_dem").persist(StorageLevel.DISK_ONLY)
        taxi_demand_df = last_df.filter(last_df["car_type"] == "Taxi").select(last_df["timestamp"] - last_df["timestamp"] % time_interval, last_df["car_id"], last_df["geo_id"]) \
            .distinct().withColumnRenamed(old_col_name, "timestamp") \
            .groupBy("timestamp", "geo_id").count().withColumnRenamed("count", "taxi_dem").persist(StorageLevel.DISK_ONLY)
        # 7 join Uber and Lyft data and get their common geo id;
        # 8 fill NaN with 0.
        demand_df = index_df.join(uber_demand_df, ["timestamp", "geo_id"], "left").join(lyft_demand_df, ["timestamp", "geo_id"], "left") \
            .join(taxi_demand_df, ["timestamp", "geo_id"], "left").fillna(0).persist(StorageLevel.DISK_ONLY)

    # Save data and copy data to local.
    demand_df.write.save(path = demand_path, format = "json", mode = "overwrite")
    demand_df.toPandas().to_csv(local_path)
