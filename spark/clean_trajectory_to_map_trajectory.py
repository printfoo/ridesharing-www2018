from pyspark.sql import SparkSession
from pyspark import StorageLevel
from shapely.geometry import Point, shape
import pydoop.hdfs as hdfs
import ujson as json
import sys

# SF & NYC: This function returns a block dictionary of a given geojson file.
def get_block(block_path = None):

    # Open file and read as json.
    with open(block_path, "r") as block_file:
        block_js = block_file.read().strip("\n")
    block_js = json.loads(block_js)

    # Build a dictionary of neighborhood information.
    block = {}
    for i in range(len(block_js["features"])):
        polygon = shape(block_js["features"][i]["geometry"])
        #if polygon.is_valid:
        geo_id = block_js["features"][i]["properties"]["GEOID"]
        block[geo_id] = polygon
    return block

# This function returns a geo_id of a given location.
def get_geo_id(line = {}, block = {}):

    # Get the block geo_id of the location.
    line = json.loads(line)
    location = Point(float(line["lng"]), float(line["lat"]))
    for geo_id in block:
        if location.within(block[geo_id]):
            line[u"geo_id"] = unicode(geo_id)
            line = json.dumps(line)
            return line
    line[u"geo_id"] = u"000000000000"
    line = json.dumps(line)
    return line

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
    block_path = "resources/" + city + "_block_groups_2015.geojson"
    uber_clean_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/uber_" + city + "_clean_trajectory"
    lyft_clean_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/lyft_" + city + "_clean_trajectory"
    if city == "sf":
        taxi_clean_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/taxi_" + city + "_clean_trajectory"
    map_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + city + "_map_trajectory"
    try:
        hdfs.rmr(map_trajectory_path)
    except:
        pass

    # Get block dictionary.
    block = get_block(block_path = block_path)

    # Start spark SQL session.
    spark = SparkSession.builder.appName("clean_trajectory_to_map_trajectory").getOrCreate()

    # Load clean trajectory RDD.
    uber_clean_trajectories_rdd = spark.sparkContext.textFile(uber_clean_trajectory_path).persist(StorageLevel.DISK_ONLY)
    lyft_clean_trajectories_rdd = spark.sparkContext.textFile(lyft_clean_trajectory_path).persist(StorageLevel.DISK_ONLY)
    if city == "sf":
        taxi_clean_trajectories_rdd = spark.sparkContext.textFile(taxi_clean_trajectory_path).persist(StorageLevel.DISK_ONLY)

    # Add key geo_id.
    uber_map_trajectories_rdd = uber_clean_trajectories_rdd.map(lambda line: get_geo_id(line, block)).persist(StorageLevel.DISK_ONLY)
    lyft_map_trajectories_rdd = lyft_clean_trajectories_rdd.map(lambda line: get_geo_id(line, block)).persist(StorageLevel.DISK_ONLY)
    if city == "sf":
        taxi_map_trajectories_rdd = taxi_clean_trajectories_rdd.map(lambda line: get_geo_id(line, block)).persist(StorageLevel.DISK_ONLY)

    # Merge and save data.
    if city == "sf":
        map_trajectories_rdd = uber_map_trajectories_rdd.union(lyft_map_trajectories_rdd).union(taxi_map_trajectories_rdd).persist(StorageLevel.DISK_ONLY)
    else:
        map_trajectories_rdd = uber_map_trajectories_rdd.union(lyft_map_trajectories_rdd).persist(StorageLevel.DISK_ONLY)
    map_trajectories_rdd.saveAsTextFile(map_trajectory_path)
