from shapely.geometry import Point
import geopandas as gpd
import pandas as pd
import numpy as np
import sys

def get_geoid(p):
    geoid_list = bg_gdf[bg_gdf["geometry"].contains(p)]["GEOID"].values
    try:
        geoid = geoid_list[0]
    except IndexError:
        return 0
    return int(geoid)

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    bg_path = file_path + "/resources/" + city + "_block_groups/" + city + "_block_groups_2015.geojson"
    bus_path = file_path + "/resources/" + city + "_acs15/opendata_bus_stops.csv"
    parking_meter_path = file_path + "/resources/" + city + "_acs15/opendata_parking_meters.csv"
    parking_off_path = file_path + "/resources/" + city + "_acs15/opendata_parking_off_street.csv"
    save_path = file_path + "/resources/" + city + "_acs15/opendata_with_geoid.csv"

    # Read data.
    bg_gdf = gpd.read_file(bg_path)
    bus_df = pd.read_csv(bus_path)
    parking_meter_df = pd.read_csv(parking_meter_path)
    parking_off_df = pd.read_csv(parking_off_path)

    # Get geoid.
    bus_df["geoid"] = bus_df[["stop_lat", "stop_lon"]] \
        .apply(lambda p: get_geoid(Point(p["stop_lon"], p["stop_lat"])), axis = 1)
    parking_meter_df["geoid"] = parking_meter_df["LOCATION"] \
        .apply(lambda p: get_geoid(Point(eval(p)[1], eval(p)[0])))
    parking_off_df["geoid"] = parking_off_df["Location 1"] \
        .apply(lambda p: get_geoid(Point(eval(p)[1], eval(p)[0])))

    # Groupby and count.
    df1 = bus_df.groupby("geoid").count()
    df1["geoid"] = df1.index
    df2 = parking_meter_df.groupby("geoid").count()
    df2["geoid"] = df2.index
    df3 = parking_off_df.groupby("geoid").count()
    df3["geoid"] = df3.index

    # Merge and save.
    df = df1.merge(df2, on = "geoid", how = "outer").merge(df3, on = "geoid", how = "outer")
    df.to_csv(save_path)
    
