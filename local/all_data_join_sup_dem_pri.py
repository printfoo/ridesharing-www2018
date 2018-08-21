from sklearn import neighbors
import geopandas as gpd
import pandas as pd
import numpy as np
import sys

def checktime_taxi(row):
    time = row["timestamp"]
    if (time > 1478937600 + 86400 * 0.9 and time < 1478937600 + 86400 * 2.3) or \
       (time > 1478937600 + 86400 * 12.9  and time < 1478937600 + 86400 * 23.9): #or \
       #(time >= 1481209050 - 3600 * 1 and time <= 1481209050 + 3600 * 2):
        return np.nan
    return row[row.index[1]]

def checktime_uber(row):
    time = row["timestamp"]
    if (time >= 1479416700 and time <= 1479435600):
        return np.nan
    return row[row.index[1]]

def checktime_period(t, period):
    if period == "all":
        return True if (t >= 1478937600 and t <= 1478937600 + 86400 * 40) else False
    elif period == "peak":
        for i in range(40):
            if (t >= 1478937600 + 86400 * i + 7 * 3600 and t <= 1478937600 + 86400 * i + 23 * 3600):
                return True
        return False
    elif period == "nadir":
        for i in range(40):
            if (t >= 1478937600 + 86400 * i + 7 * 3600 and t <= 1478937600 + 86400 * i + 22 * 3600):
                return False
        return True
    else:
        return False


if __name__ == "__main__":

    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    period = sys.argv[2].lower()
    supply_path = file_path + "/resources/" + city + "_data/" + city + "_supply_5min.csv"
    demand_path = file_path + "/resources/" + city + "_data/" + city + "_demand_5min.csv"
    indicator_path = file_path + "/resources/" + city + "_data/" + city + "_indicator_5min.csv"
    mapping_path = file_path + "/resources/" + city + "_block_groups/" + city + "_user_mapping.csv"
    all_path = file_path + "/resources/" + city + "_data/" + city + "_" + period + "_data.csv"

    # Read data.
    supply_df = pd.read_csv(supply_path)
    demand_df = pd.read_csv(demand_path)
    indicator_df = pd.read_csv(indicator_path)
    del indicator_df["lat"]
    del indicator_df["lng"]
    mapping_df = pd.read_csv(mapping_path)

    # Join data.
    all_df = supply_df \
        .merge(demand_df, on = ["timestamp", "geo_id"], how = "left") \
        .merge(mapping_df, on = ["geo_id"], how = "left") \
        .merge(indicator_df, on = ["timestamp", "user_info"], how = "left")
    if city == "sf":
        all_df = all_df[all_df["timestamp"].apply(lambda t: checktime_period(t, period))]
        all_df["taxi_sup_tmp"] = all_df["taxi_sup"] * 2026.0 / 554.0
        all_df["taxi_dem_tmp"] = all_df["taxi_dem"] * 2026.0 / 554.0
        all_df["taxi_sup"] = all_df[["timestamp", "taxi_sup_tmp"]].apply(checktime_taxi, axis = 1)
        all_df["taxi_dem"] = all_df[["timestamp", "taxi_dem_tmp"]].apply(checktime_taxi, axis = 1)
        all_df["uber_sup_tmp"] = all_df["uber_sup"]
        all_df["uber_dem_tmp"] = all_df["uber_dem"]
        all_df["uber_sup"] = all_df[["timestamp", "uber_sup_tmp"]].apply(checktime_uber, axis = 1)
        all_df["uber_dem"] = all_df[["timestamp", "uber_dem_tmp"]].apply(checktime_uber, axis = 1)
    else:
        all_df = all_df[all_df["timestamp"] >= 1485850000]
        all_df = all_df[all_df["timestamp"] <= 1486600000]
    all_df["uber_sur"] = (all_df["uber_sup"] - all_df["uber_dem"]) / (all_df["uber_dem"] + 1)
    all_df["lyft_sur"] = (all_df["lyft_sup"] - all_df["lyft_dem"]) / (all_df["lyft_dem"] + 1)
    all_df["taxi_sur"] = (all_df["taxi_sup"] - all_df["taxi_dem"]) / (all_df["taxi_dem"] + 1)
    all_df["taxi_pri"] = 1.0
    all_df["uber_pri"] = all_df["uber_surge"]
    all_df["lyft_pri"] = all_df["lyft_surge"]
        

    # Save data.
    select = ["timestamp", "geo_id", "lat", "lng",
              "uber_sup", "lyft_sup", "taxi_sup",
              "uber_dem", "lyft_dem", "taxi_dem",
              "uber_sur", "lyft_sur", "taxi_sur",
              "uber_pri", "lyft_pri", "taxi_pri",
              "uber_surge", "lyft_surge",
              "uber_eta", "lyft_eta"]
    all_df[select].to_csv(all_path)
