from pyspark import SparkContext
import pydoop.hdfs as hdfs
import ujson as json
import shutil, sys

def get_trajectory(line):
    return ((line["car_id"], line["car_type"]), [(int(line["timestamp"]), line["geo_id"])])

def overlap((car_info, trajectory)):
    trajectory.sort(key = lambda (ts, _) : ts)
    block_seq = ["000000000000"] # get a sequence of non-reqeated blocks
    time_seq = ["0000000000"] # get a sequence of passed time slots (5-min)
    for spot in trajectory:
	if spot[0] - spot[0] % (60 * 5) != time_seq[-1]: # remove repeatable time slot after flooring
            time_seq.append(spot[0] - spot[0] % (60 * 5))
        if spot[1] != block_seq[-1]: # remove repeatable blocks because ignore sampling rate
            block_seq.append(spot[1])
    js = {"car_id": car_info[0], "car_type": car_info[1], "block_seq": block_seq, "time_seq": time_seq}
    return js

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
    new_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/results/" + city + "_overlap_prep"
    try:
        hdfs.rmr(new_trajectory_path)
    except:
        pass

    sc = SparkContext(appName = "overlap")
    rdd = sc.textFile(map_trajectory_path)
    
    new_rdd = rdd.map(json.loads).map(get_trajectory) \
        .reduceByKey(lambda a, b: a + b) \
        .map(overlap) \
        .map(json.dumps)
    
    new_rdd.saveAsTextFile(new_trajectory_path)
