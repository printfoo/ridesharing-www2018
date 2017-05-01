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
    old_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + city + "_map_trajectory"
    new_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/" + city + "_map_trajectory"
    stat_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/results/" + city + "_stats"
    try:
        hdfs.rmr(stat_path)
    except:
        pass
    local_path = "results/" + city + "_stats.csv"
    try:
        shutil.rmtree(local_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("map_trajectory_to_statistics").getOrCreate()

    # Load map trajectory dataframes.
    old_df = spark.read.json(old_trajectory_path).persist(StorageLevel.DISK_ONLY)
    new_df = spark.read.json(new_trajectory_path).persist(StorageLevel.DISK_ONLY)

    # Compute statistics.
    if city.lower() == "sf":
        old_df = old_df.filter(old_df["timestamp"].between(1478937600, 1478937600 + 86400 * 40))
        new_df = new_df.filter(new_df["timestamp"].between(1478937600, 1478937600 + 86400 * 40))
    old_df = old_df.groupby("car_id", "car_type").count()
    old_df = old_df.filter(old_df["count"] > 5).groupby("car_type").count().withColumnRenamed("count", "id_num")
    new_df = new_df.groupby("car_id", "car_type").count()
    new_df = new_df.filter(new_df["count"] > 5).groupby("car_type").count().withColumnRenamed("count", "trip_num")
    stat_df = old_df.join(new_df, "car_type")

    # Save data and copy data to local.
    stat_df.write.save(path = stat_path, format = "json", mode = "overwrite")
    stat_df.toPandas().to_csv(local_path)

