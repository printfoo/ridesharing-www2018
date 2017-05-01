from pyspark import SparkContext
import pydoop.hdfs as hdfs
import ujson as json
import shutil, sys

def get_trajectory(line):
    return ((line["car_id"]), [(int(line["timestamp"]), line)])

def relabel((car_id, trajectory)):
    trajectory.sort(key = lambda (ts, _) : ts)
    new_trajectory = []
    suffix = 0
    for i in range(1, len(trajectory)):
        if trajectory[i][0] - trajectory[i - 1][0] > 60:
            suffix += 1
        trajectory[i][1]["car_id"] += str(suffix)
        new_trajectory.append(trajectory[i][1])
    return new_trajectory

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
    new_trajectory_path = "hdfs://megatron.ccs.neu.edu/user/jiangshan/ridesharing/clean/" + city + "_map_trajectory"
    try:
        hdfs.rmr(new_trajectory_path)
    except:
        pass

    sc = SparkContext(appName = "relabel")
    rdd = sc.textFile(map_trajectory_path)
    
    new_rdd = rdd.map(json.loads).map(get_trajectory) \
        .reduceByKey(lambda a, b: a + b) \
        .flatMap(relabel).map(json.dumps)
    
    new_rdd.saveAsTextFile(new_trajectory_path)
