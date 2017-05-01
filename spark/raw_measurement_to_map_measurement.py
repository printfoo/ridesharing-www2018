from pyspark.sql import SparkSession
from pyspark import StorageLevel
import pydoop.hdfs as hdfs

# Main function.
if __name__ == "__main__":

    # Get paths then delete old results and create new path.
    uber_raw_measurement_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/uber_sf_raw_measurement/*"
    lyft_raw_measurement_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/lyft_sf_raw_measurement/*"
    map_measurement_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/sf_map_measurement"
    try:
        hdfs.rmr(map_measurement_path)
    except:
        pass

    # Start spark SQL session.
    spark = SparkSession.builder.appName("raw_measurement_to_map_measurement").getOrCreate()

    # Load clean measurement dataframes.
    uber_raw_measurement_df = spark.read.json(uber_raw_measurement_path).persist(StorageLevel.DISK_ONLY)
    lyft_raw_measurement_df = spark.read.json(lyft_raw_measurement_path).persist(StorageLevel.DISK_ONLY)

    # Merge and save data.
    map_measurement_df = uber_raw_measurement_df.union(lyft_raw_measurement_df).persist(StorageLevel.DISK_ONLY)
    map_measurement_df.write.save(path = map_measurement_path, format = "json", mode = "overwrite")
