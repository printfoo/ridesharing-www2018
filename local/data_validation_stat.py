from shapely.geometry import shape, Point
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import os, sys, json, random, time

def comp_k(p1, p2, t, A):
    count = 0
    for k in p1:
        for l in p2:
            if (k[0]-l[0])**2 + (k[1]-l[1])**2 < t**2:
                count += 1
    k = (count * A + .0) / (len(p1) * len(p2))
    return k;

def get_points(data, app, n):
    _tmp = data[data["car_type"] == app]
    measure = _tmp[_tmp["data_type"] == "measure"].sample(n)
    truth = _tmp[_tmp["data_type"] == "truth"].sample(n)
    return measure[["lat", "lng"]].values, truth[["lat", "lng"]].values

def get_points_sim(data, app, n):
    _tmp = data[data["car_type"] == app]
    measure = _tmp[_tmp["data_type"] == "measure"].sample(n)
    truth = _tmp[_tmp["data_type"] == "truth"].sample(n)
    df = pd.concat([measure, truth])
    p1, p2 = train_test_split(df, test_size = 0.5)
    return p1[["lat", "lng"]].values, p2[["lat", "lng"]].values

def generate_random(number, polygon):
    list_of_points = []
    minx, miny, maxx, maxy = polygon.bounds
    counter = 0
    while counter < number:
        pnt = Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
        if polygon.contains(pnt):
            list_of_points.append((pnt.y, pnt.x))
            counter += 1
    return list_of_points

if __name__ == "__main__":
    
    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    app = sys.argv[1]
    vali_path = file_path + "/resources/nyc_data/nyc_validation.csv"
    manh_js_path = file_path + "/resources/nyc_data/manhattan.geojson"
    sim_path = file_path + "/resources/nyc_data/sim_points.txt"
    with open(manh_js_path, "r") as f:
        manh_js = json.load(f)
        manh_poly = shape(manh_js["geometry"])
    with open(sim_path, "r") as f:
        random_p = eval(f.read())
    k_path = file_path + "/results/" + app + "_k_values.csv"
    res_path = file_path + "/results/mc_sim_" + app + ".csv"
    
    if not os.path.exists(vali_path):
        data_path = file_path + "/resources/nyc_data/nyc_demand_points.csv"
        truth_path = file_path + "/resources/nyc_truth/clean/map_truth.csv"
        data = pd.read_csv(data_path)
        data = data[data["timestamp"] >= 1485900000]
        data = data[data["timestamp"] <= 1486400000]
        data = data[(data["geo_id"] - data["geo_id"] % 10000000) == 360610000000]
        data["data_type"] = "measure"
        truth = pd.read_csv(truth_path)
        truth["data_type"] = "truth"
        truth = truth[(truth["geo_id"] - truth["geo_id"] % 10000000) == 360610000000]
        uber_truth = truth[truth["car_type"] == "UberX"][["lat", "lng", "car_type", "data_type"]]
        lyft_truth = truth[truth["car_type"] == "Lyft"][["lat", "lng", "car_type", "data_type"]]
        uber_data = data[data["car_type"] == "UberX"][["lat", "lng", "car_type", "data_type"]]
        lyft_data = data[data["car_type"] == "Lyft"][["lat", "lng", "car_type", "data_type"]]
        vali = pd.concat([uber_truth, lyft_truth, uber_data, lyft_data])
        vali.to_csv(vali_path)
    else:
        vali = pd.read_csv(vali_path)

    if not os.path.exists(res_path):
        with open(res_path, "w") as f:
            f.write("exp_no,k_value\n")

    # Uber and Lyft.
    A = 1
    n = 2000
    measure, truth = get_points(data = vali, app = app, n = n)
    with open(k_path, "w") as f:
        f.write("t,self,measure,random\n")
    for t in [0.22/50 * (i + 1) for i in range(50)]:
        k1 = comp_k(p1 = truth, p2 = truth, t = t, A = A)
        k2 = comp_k(p1 = truth, p2 = measure, t = t, A = A)
        k3 = comp_k(p1 = truth, p2 = random_p, t = t, A = A)
        print k1, k2, k3
        with open(k_path, "a") as f:
            f.write(str(t) + "," + str(k1) + "," + str(k2) + "," + str(k3) + "\n")
    t = 0.05
    for _ in range(5000):
        measure, truth = get_points(data = vali, app = app, n = n)
        k = comp_k(p1 = measure, p2 = truth, t = t, A = A)
        with open(res_path, "a") as f:
            f.write("1," + str(k) + "\n")
        p1, p2 = get_points_sim(data = vali, app = app, n = n)
        k = comp_k(p1 = p1, p2 = p2, t = t, A = A)
        with open(res_path, "a") as f:
            f.write("2," + str(k) + "\n")
