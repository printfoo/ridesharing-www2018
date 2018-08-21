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
        print "Error in city name (SF/NYC) or goal (x min)."
        sys.exit()
    if "min" not in goal or city not in {"sf", "nyc"}:
        print "Error in city name (SF/NYC) or goal (x min)."
        sys.exit()

    # Get paths then delete old results and create new path.
    map_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/" + city + "_map_trajectory"
    supply_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/results/" + city + "_supply_" + goal
    try:
        hdfs.rmr(supply_path)
    except:
        pass
    local_path = "results/" + city + "_supply_" + goal + ".csv"
    try:
        shutil.rmtree(local_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("map_trajectory_to_supply").getOrCreate()

    # Load map trajectory dataframes.
    map_df = spark.read.json(map_trajectory_path).persist(StorageLevel.DISK_ONLY)
    
    # Set time interval and the name of roundoff time column.
    if goal == "5min":
        time_interval = 300
    if goal == "15min":
        time_interval = 900
    if goal == "30min":
        time_interval = 1800
    if goal == "60min":
        time_interval = 3600
    if goal == "90min":
        time_interval = 5400
    old_col_name = "(timestamp - (timestamp % " + str(time_interval) + "))"

    # If get minute interval supply.
    if "min" in goal:
        # Uber and Lyft and Taxi.
        # 1 creat an index of all timestamp and all geo id;
        roundtime_df = map_df.select(map_df["timestamp"] - map_df["timestamp"] % time_interval).distinct().withColumnRenamed(old_col_name, "timestamp")
        index_df = roundtime_df.crossJoin(map_df.select("geo_id").distinct())
        # 2 roundoff timestamp and get corresponding car id and geo id;
        # 3 get distinct timestamp, car id, geo id combination;
        # 4 count distinct car id in each time interval and geo id;
        uber_supply_df = map_df.filter(map_df["car_type"] == "UberX").select(map_df["timestamp"] - map_df["timestamp"] % time_interval, map_df["car_id"], map_df["geo_id"]) \
            .distinct().withColumnRenamed(old_col_name, "timestamp") \
            .groupBy("timestamp", "geo_id").count().withColumnRenamed("count", "uber_sup").persist(StorageLevel.DISK_ONLY)
        lyft_supply_df = map_df.filter(map_df["car_type"] == "Lyft").select(map_df["timestamp"] - map_df["timestamp"] % time_interval, map_df["car_id"], map_df["geo_id"]) \
            .distinct().withColumnRenamed(old_col_name, "timestamp") \
            .groupBy("timestamp", "geo_id").count().withColumnRenamed("count", "lyft_sup").persist(StorageLevel.DISK_ONLY)
        taxi_supply_df = map_df.filter(map_df["car_type"] == "Taxi").select(map_df["timestamp"] - map_df["timestamp"] % time_interval, map_df["car_id"], map_df["geo_id"]) \
            .distinct().withColumnRenamed(old_col_name, "timestamp") \
            .groupBy("timestamp", "geo_id").count().withColumnRenamed("count", "taxi_sup").persist(StorageLevel.DISK_ONLY)
        # 5 join Uber and Lyft data and get their common geo id;
        # 6 fill NaN with 0.
        supply_df = index_df.join(uber_supply_df, ["timestamp", "geo_id"], "left").join(lyft_supply_df, ["timestamp", "geo_id"], "left") \
            .join(taxi_supply_df, ["timestamp", "geo_id"], "left").fillna(0).persist(StorageLevel.DISK_ONLY)

    # Save data and copy data to local.
    supply_df.write.save(path = supply_path, format = "json", mode = "overwrite")
    supply_df.toPandas().to_csv(local_path)
