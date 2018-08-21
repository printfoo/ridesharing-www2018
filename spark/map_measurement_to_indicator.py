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
    map_measurement_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/" + city + "_map_measurement"
    indicator_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/results/" + city + "_indicator_" + goal
    try:
        hdfs.rmr(indicator_path)
    except:
        pass
    local_path = "results/" + city + "_indicator_" + goal + ".csv"
    try:
        shutil.rmtree(local_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("map_measurement_to_indicator").getOrCreate()

    # Load map trajectory dataframes.
    map_df = spark.read.json(map_measurement_path).persist(StorageLevel.DISK_ONLY)
    map_df = map_df.filter(map_df["eta"] < 10000)
 
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

    # If get minute indicator.
    if "min" in goal:
        # 1 creat an index of all timestamp and all measurement points;
        roundtime_df = map_df.filter(map_df["car_type"] == "UberX").select(map_df["timestamp"] - map_df["timestamp"] % time_interval) \
            .distinct().withColumnRenamed(old_col_name, "timestamp")
        index_df = roundtime_df.crossJoin(map_df.select(map_df["lat"], map_df["lng"], map_df["user_info"]).distinct())
        # 2 roundoff timestamp and get corresponding eta and surge;
        # 3 get distinct value combination;
        # 4 count average indicator in each time interval and measurement point;
        uber_indicator_df = map_df.filter(map_df["car_type"] == "UberX").filter(map_df["eta"] != "NaN") \
            .select(map_df["timestamp"] - map_df["timestamp"] % time_interval, map_df["user_info"], map_df["eta"].cast("float"), map_df["surge"].cast("float")) \
            .distinct().withColumnRenamed(old_col_name, "timestamp").groupBy("timestamp", "user_info").avg("eta", "surge") \
            .withColumnRenamed("avg(eta)", "uber_eta").withColumnRenamed("avg(surge)", "uber_surge").persist(StorageLevel.DISK_ONLY)
        lyft_indicator_df = map_df.filter(map_df["car_type"] == "Lyft").filter(map_df["eta"] != "NaN") \
            .select(map_df["timestamp"] - map_df["timestamp"] % time_interval, map_df["user_info"], map_df["eta"].cast("float"), map_df["surge"].cast("float")) \
            .distinct().withColumnRenamed(old_col_name, "timestamp").groupBy("timestamp", "user_info").avg("eta", "surge") \
            .withColumnRenamed("avg(eta)", "lyft_eta").withColumnRenamed("avg(surge)", "lyft_surge").persist(StorageLevel.DISK_ONLY)
        # 5 join Uber and Lyft data and get their common geo id;
        # 6 fill NaN with 0;
        indicator_df = uber_indicator_df.join(lyft_indicator_df, "user_info").persist(StorageLevel.DISK_ONLY)
        indicator_df = index_df.join(uber_indicator_df, ["timestamp", "user_info"], "left").join(lyft_indicator_df, ["timestamp", "user_info"], "left") \
            .fillna(0.0).persist(StorageLevel.DISK_ONLY)

    # Save data and copy data to local.
    indicator_df.write.save(path = indicator_path, format = "json", mode = "overwrite")
    indicator_df.toPandas().to_csv(local_path)
