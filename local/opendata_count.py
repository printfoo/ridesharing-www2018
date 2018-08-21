from shapely.geometry import Point
import geopandas as gpd
import pandas as pd
import numpy as np
import sys, time

def get_geoid(p):
    geoid_list = bg_gdf[bg_gdf["geometry"].contains(p)]["GEOID"].values
    try:
        geoid = geoid_list[0]
    except IndexError:
        return 0
    return int(geoid)

def check_sign(s):
    if "no " in s.lower():
        return "F"
    else:
        return "T"

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    bg_path = file_path + "/resources/" + city + "_block_groups/" + city + "_block_groups_2015.geojson"
    save_path = file_path + "/resources/" + city + "_acs15/opendata_with_geoid.csv"
    bg_gdf = gpd.read_file(bg_path)

    # SF
    if city.lower() == "sf":
        bus_path = file_path + "/resources/" + city + "_acs15/opendata_bus_stops.csv"
        parking_meter_path = file_path + "/resources/" + city + "_acs15/opendata_parking_meters.csv"
        parking_off_path = file_path + "/resources/" + city + "_acs15/opendata_parking_off_street.csv"

        # Read data.
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

    # NYC
    elif city.lower() == "nyc":
        bus1_path = file_path + "/resources/" + city + "_acs15/stops_bus_nyc_jan2017"
        bus2_path = file_path + "/resources/" + city + "_acs15/stops_expbus_nyc_jan2017"
        parking_meter_path = file_path + "/resources/" + city + "_acs15/opendata_parking_sign/Parking_Regulation_Shapefile"
        parking_off_path = file_path + "/resources/" + city + "_acs15/opendata_parking_off_street.csv"

        # Read data.
        bus1_df = gpd.read_file(bus1_path)
        bus2_df = gpd.read_file(bus2_path)
        parking_meter_df = gpd.read_file(parking_meter_path)
        parking_meter_df["sel"] = parking_meter_df["SIGNDESC1"].apply(check_sign)
        parking_meter_df = parking_meter_df[parking_meter_df["sel"] == "T"]
        parking_off_df = pd.read_csv(parking_off_path)

        # Get geoid.
        print "t1", time.time()
        bus1_df["geoid"] = bus1_df[["stop_lat", "stop_lon"]] \
            .apply(lambda p: get_geoid(Point(p["stop_lon"], p["stop_lat"])), axis = 1)
        print "t2", time.time()
        bus2_df["geoid"] = bus2_df[["stop_lat", "stop_lon"]] \
            .apply(lambda p: get_geoid(Point(p["stop_lon"], p["stop_lat"])), axis = 1)
        print "t3", time.time()
        parking_meter_df["geoid"] = parking_meter_df["geometry"] \
            .apply(lambda p: get_geoid(p))
        print "t4", time.time()
        parking_off_df["geoid"] = parking_off_df[["Latitude", "Longitude"]] \
            .apply(lambda p: get_geoid(Point(p["Longitude"], p["Latitude"])), axis = 1)
        print "t5", time.time()
        
        # Groupby and count.
        df1 = bus1_df.groupby("geoid").count()
        df1["geoid"] = df1.index
        df2 = bus2_df.groupby("geoid").count()
        df2["geoid"] = df2.index
        df3 = parking_meter_df.groupby("geoid").count()
        df3["geoid"] = df3.index
        df4 = parking_off_df.groupby("geoid").count()
        df4["geoid"] = df4.index

        # Merge and save.
        df = df1.merge(df2, on = "geoid", how = "outer").merge(df3, on = "geoid", how = "outer").merge(df4, on = "geoid", how = "outer")
        df.to_csv(save_path)
