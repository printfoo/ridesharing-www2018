from pyspark import SparkContext
import pydoop.hdfs as hdfs
import ujson as json
import shutil, sys

def reverselabel(line):
    line["car_id"] = line["car_id"][:-3]
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
    map_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/" + city + "_map_trajectory"
    new_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/proc/" + city + "_map_trajectory"
    try:
        hdfs.rmr(new_trajectory_path)
    except:
        pass

    sc = SparkContext(appName = "reverselabel")
    rdd = sc.textFile(map_trajectory_path)
    
    new_rdd = rdd.map(json.loads).map(reverselabel).map(json.dumps)
    
    new_rdd.saveAsTextFile(new_trajectory_path)
