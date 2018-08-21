from shapely.geometry import shape, Point
import geopandas as gpd
import pandas as pd
import numpy as np
import json, sys

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
    location = Point(float(line["lng"]), float(line["lat"]))
    for geo_id in block:
        if location.within(block[geo_id]):
            line["geo_id"] = geo_id
            return line
    line["geo_id"] = "000000000000"
    return line

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    block_path = file_path + "/resources/" + city + "_block_groups/" + city + "_block_groups_2015.geojson"
    user_path = file_path + "/resources/" + city + "_block_groups/" + city + "_user_info.txt"
    mapping_path = file_path + "/resources/" + city + "_block_groups/" + city + "_user_mapping.csv"

    # Get block dictionary.
    block = get_block(block_path = block_path)

    # Read data.
    city_sep = {"nyc": ", ", "sf": ","}
    user_df = pd.read_csv(user_path, sep = city_sep[city])

    # Map points inside.
    mapping_df = user_df.apply(lambda line: get_geo_id(line, block), axis = 1)

    # Save data.
    mapping_df.to_csv(mapping_path, index = False)
