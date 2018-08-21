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
def get_geo_id(line, block):
    # Get the block geo_id of the location.
    try:
        location = Point(float(line["lng"]), float(line["lat"]))
    except:
        return []
    for geo_id in block:
        if location.within(block[geo_id]):
            return [{"lat": line["lat"], "lng": line["lng"], "car_type": line["car_type"], "geo_id": geo_id}]
    return [{"lat": line["lat"], "lng": line["lng"], "car_type": line["car_type"], "geo_id": "000000000000"}]

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
    clean_truth_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/nyc_truth.csv"
    map_truth_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/nyc_map_truth"
    try:
        hdfs.rmr(map_truth_path)
    except:
        pass
    local_path = "results/map_truth.csv"
    try:
        shutil.rmtree(local_path)
    except:
        pass

    # Get block dictionary.
    block = get_block(block_path = block_path)

    # Start spark SQL session.
    spark = SparkSession.builder.appName("clean_truth_to_map_truth").getOrCreate()

    # Load clean trajectory RDD.
    clean_truth_df = spark.read.csv(clean_truth_path).persist(StorageLevel.DISK_ONLY)
    clean_truth_df = clean_truth_df.withColumnRenamed("_c0", "index").withColumnRenamed("_c1", "lat").withColumnRenamed("_c2", "lng").withColumnRenamed("_c3", "car_type")
 
    # Add key geo_id.
    map_truth_rdd = clean_truth_df.rdd.flatMap(lambda line: get_geo_id(line, block)).persist(StorageLevel.DISK_ONLY)
    map_truth_df = spark.createDataFrame(map_truth_rdd)

    # Save data.
    map_truth_df.write.save(path = map_truth_path, format = "json", mode = "overwrite")
    map_truth_df.toPandas().to_csv(local_path)
