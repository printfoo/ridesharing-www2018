from pyspark.sql import SparkSession
from pyspark import StorageLevel
import pydoop.hdfs as hdfs
import sys

# Main function.
if __name__ == "__main__":

    # Get command and check if valid.
    area_box = {"DT", "NE", "NW", "SX", "IL", "HP", "ALL_SF", "LM", "UM", "BQ", "SI", "JK", "ALL_NYC"}
    try:
        app = sys.argv[1].lower()
        area = sys.argv[2].upper()
    except IndexError:
        print "Error in application name (Uber/Lyft) or area code (DT/NE/NW/SX/IL/HP/ALL_SF/LM/UM/BQ/SI/JK/ALL_NYC)."
        sys.exit()
    if app not in {"uber","lyft"} or area not in area_box:
        print "Error in application name (Uber/Lyft) or area code (DT/NE/NW/SX/IL/HP/ALL_SF/LM/UM/BQ/SI/JK/ALL_NYC)."
        sys.exit()

    # If area is dividing step, get all raw trajectory in that area.
    if area in {"DT", "NE", "NW", "SX", "IL", "HP"}:
        raw_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_sf_raw_trajectory/" + area + "*"
        file_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_sf_proc_trajectory"
        try:
            hdfs.mkdir(file_path)
        except:
            pass
        clean_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_sf_proc_trajectory/" + area
        try:
            hdfs.rmr(clean_trajectory_path)
        except:
            pass
    elif area in {"LM", "UM", "BQ", "SI", "JK"}:
        raw_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_nyc_raw_trajectory/" + area + "*"
        file_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_nyc_proc_trajectory"
        try:
            hdfs.mkdir(file_path)
        except:
            pass
        clean_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_nyc_proc_trajectory/" + area
        try:
            hdfs.rmr(clean_trajectory_path)
        except:
            pass
    # If area is merging step, get all clean trajectory in all areas.
    elif area == "ALL_SF":
        raw_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_sf_proc_trajectory/*"
        clean_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_sf_clean_trajectory"
        try:
            hdfs.rmr(clean_trajectory_path)
        except:
            pass
    elif area == "ALL_NYC":
        raw_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_nyc_proc_trajectory/*"
        clean_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + app + "_nyc_clean_trajectory"
        try:
            hdfs.rmr(clean_trajectory_path)
        except:
            pass
    # Else area code is invalid.
    else:
        print "Something weird happened, check your code!"
        sys.exit()

    # Start spark SQL session.
    spark = SparkSession.builder.appName("raw_trajectory_to_clean_trajectory").getOrCreate()

    # Load raw trajectory dataframe.
    raw_trajectories_df = spark.read.json(raw_trajectory_path).persist(StorageLevel.DISK_ONLY)

    # Clean data
    clean_trajectories_df = raw_trajectories_df.distinct().persist(StorageLevel.DISK_ONLY)
   
    # Save data.
    clean_trajectories_df.write.save(path = clean_trajectory_path, format = "json", mode = "overwrite")
