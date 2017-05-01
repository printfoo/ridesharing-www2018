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
        print "Error in city name (SF/NYC)."
        sys.exit()
    if city not in {"sf", "nyc"}:
        print "Error in city name (SF/NYC)."
        sys.exit()

    # Get paths then delete old results and create new path.
    map_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/" + city + "_map_trajectory"
    idletime_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/results/" + city + "_idletime.csv"
    try:
        hdfs.rmr(idletime_path)
    except:
        pass
    local_path = "results/" + city + "_idletime.csv"
    try:
        shutil.rmtree(local_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("map_trajectory_to_idletime").getOrCreate()

    # Load map trajectory dataframes.
    map_df = spark.read.json(map_trajectory_path).persist(StorageLevel.DISK_ONLY)
    map_df = map_df.select(map_df["timestamp"].cast("int"), map_df["car_id"], map_df["car_type"])
    max_df = map_df.groupby(["car_id", "car_type"]).max("timestamp")
    min_df = map_df.groupby(["car_id", "car_type"]).min("timestamp")
    idle_df = max_df.join(min_df, ["car_id", "car_type"])

    # Save data and copy data to local.
    idle_df.write.save(path = idletime_path, format = "json", mode = "overwrite")
    idle_df.toPandas().to_csv(local_path)
