import sys, os
import numpy as np

# Compare two car trajectories
def lcs_length(a, b):
    table = [[0] * (len(b) + 1) for _ in xrange(len(a) + 1)]
    for i, ca in enumerate(a, 1):
        for j, cb in enumerate(b, 1):
            table[i][j] = (table[i - 1][j - 1] + 1 if ca == cb else max(table[i][j - 1], table[i - 1][j]))
    return table[-1][-1]

def lcsubstring_length(a, b):
    table = [[0] * (len(b) + 1) for _ in xrange(len(a) + 1)]
    l = 0
    for i, ca in enumerate(a, 1):
        for j, cb in enumerate(b, 1):
            if ca == cb:
                table[i][j] = table[i - 1][j - 1] + 1
                if table[i][j] > l:
                    l = table[i][j]
    return l

# Compare trajectories and get number
def get_overlapping(cars0, cars1, sim_thres):
    ovlp_num = 0
    for car0 in cars0:
        for car1 in cars1:
            if sim_thres <= 0.99:
                l = lcs_length(car0, car1)
                len1 = min(len(car0), len(car1))
                len2 = max(len(car0), len(car1))
                if (l >= sim_thres * len1) and (len1 >= sim_thres * len2):
                    ovlp_num += 1
                    break
            else:
                if car0 == car1:
                    ovlp_num += 1
                    break
    return ovlp_num

# Get 2 list of trajectories
def get_trajectories(comp_f0_path, comp_f1_path, num_thres, sim_thres):
    cars0 = []
    try:
        comp_f0 = open(comp_f0_path, "r")
    except:
        return {"overlap": np.nan, "lyft": np.nan, "uber": np.nan}
    while True:
        line = comp_f0.readline().strip("\n")
        if not line:
            comp_f0.close()
            break
        line = eval(line)
        if len(line) < num_thres:
            continue
        cars0.append(line)

    cars1 = []
    try:
        comp_f1 = open(comp_f1_path, "r")
    except:
        return {"overlap": np.nan, "lyft": np.nan, "uber": np.nan}
    while True:
        line = comp_f1.readline().strip("\n")
        if not line:
            comp_f1.close()
            break
        line = eval(line)
        if len(line) < num_thres:
            continue
        cars1.append(line)

    ovlp_num = get_overlapping(cars0, cars1, sim_thres)
    return {"overlap": ovlp_num, "lyft": len(cars0), "uber": len(cars1)}

if __name__ == "__main__":
    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    sim_thres = float(sys.argv[2])
    ovlp_path = file_path + "/resources/" + city + "_data/" + city + "_overlap/"
    num_thres = 3
    file_path = file_path + "/resources/" + city + "_data/" + city + "_overlap_" + \
        str(num_thres) + "_" + str(sim_thres).replace(".", "p") + ".txt"
    with open(file_path, "w") as f:
        f.write("time,overlap,lyft,uber\n")

    keys = ["Lyft/", "UberX/"]
    if city == "sf":
        times = [1481702400 + 300 * i for i in range(7 * 24 * 12)]
    if city == "nyc":
        times = [1485925200 + 300 * i for i in range(7 * 24 * 12)]

    ovlp_list = []
    for time in times:
        comp_f0_path = ovlp_path + keys[0] + str(time)
        comp_f1_path = ovlp_path + keys[1] + str(time)
        dict = get_trajectories(comp_f0_path, comp_f1_path, num_thres, sim_thres)
        print(dict)
        with open(file_path, "a") as f:
            f.write(str(time) + "," + str(dict["overlap"]) + "," + \
                    str(dict["lyft"]) + "," + str(dict["uber"]) + "\n")
        ovlp_list.append(dict)
    

    
