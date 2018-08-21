from pyspark.sql import SparkSession
from pyspark import StorageLevel
import pydoop.hdfs as hdfs
import shutil, sys

# Main function.
if __name__ == "__main__":

    # Get commands and check if valid.
    try:
        city = sys.argv[1].lower()
    except IndexError:
        print "Error in city name (SF/NYC) or goal (5min/10min)."
        sys.exit()
    if city not in {"sf", "nyc"}:
        print "Error in city name (SF/NYC) or goal (5min/10min)."
        sys.exit()

    # Get paths then delete old results and create new path.
    map_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/" + city + "_map_trajectory"
    demand_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/results/" + city + "_demand_points"
    try:
        hdfs.rmr(demand_path)
    except:
        pass
    local_path = "results/" + city + "_demand_points.csv"
    try:
        shutil.rmtree(local_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("map_trajectory_to_demand").getOrCreate()

    # Load map trajectory dataframes.
    map_df = spark.read.json(map_trajectory_path).persist(StorageLevel.DISK_ONLY)
    last_df = map_df.select(map_df["timestamp"].cast("int"), map_df["car_id"]) \
        .groupBy("car_id").max("timestamp").withColumnRenamed("max(timestamp)", "timestamp").persist(StorageLevel.DISK_ONLY)
    last_df = last_df.join(map_df, ["timestamp", "car_id"], "left").persist(StorageLevel.DISK_ONLY)

    # Save data and copy data to local.
    last_df.write.save(path = demand_path, format = "json", mode = "overwrite")
    #last_df.toPandas().to_csv(local_path)
