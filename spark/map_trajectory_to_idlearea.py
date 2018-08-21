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
    map_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + city + "_map_trajectory"
    idlearea_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/results/" + city + "_idlearea.csv"
    try:
        hdfs.rmr(idlearea_path)
    except:
        pass
    local_path = "results/" + city + "_idlearea.csv"
    try:
        shutil.rmtree(local_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("map_trajectory_to_idlearea").getOrCreate()

    # Load map trajectory dataframes.
    map_df = spark.read.json(map_trajectory_path).persist(StorageLevel.DISK_ONLY)
    tmp_df = map_df.select(map_df["timestamp"].cast("int"), map_df["car_id"], map_df["car_type"])
    max_df = tmp_df.groupby(["car_id", "car_type"]).max("timestamp")
    min_df = tmp_df.groupby(["car_id", "car_type"]).min("timestamp")
    tmp_df = map_df.select(map_df["car_id"], map_df["car_type"], map_df["geo_id"]).distinct()
    count_df = tmp_df.groupby(["car_id", "car_type"]).count()
    idle_df = max_df.join(min_df, ["car_id", "car_type"]).join(count_df, ["car_id", "car_type"])

    # Save data and copy data to local.
    idle_df.write.save(path = idlearea_path, format = "json", mode = "overwrite")
    idle_df.toPandas().to_csv(local_path)
