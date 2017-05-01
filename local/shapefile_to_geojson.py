import geopandas as gpd
import sys

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    year = sys.argv[1]
    city = sys.argv[2].lower()
    key_dict = {"2010": "COUNTYFP10", "2015": "COUNTYFP"}
    city_dict = {"sf": ["06", ["075"]],
        "nyc": ["36", ["005", "047", "061", "081", "085"]]}
    # city_dict[city][0] is state FIPS, city_dict[city][1] is county FIPS.
    # SF has 1 county, NYC has 5 counties.

    # Read shapefile.
    state_shp_path = file_path + "/resources/" + city + "_block_groups/tl_" + year + "_" + \
        city_dict[city][0] + "_bg/tl_" + year + "_" + city_dict[city][0] + "_bg.shp"
    state_gdf = gpd.read_file(state_shp_path)

    # Select SF from CA and NYC from NY.
    city_gdf = state_gdf[state_gdf["GEOID"] == "fool"] # Empty
    for county_index in city_dict[city][1]:
        city_gdf = city_gdf.append(state_gdf[state_gdf[key_dict[year]] == county_index])
    city_gdf = city_gdf[city_gdf["INTPTLON"].astype("float") < -73.83] #-73.844
    city_gdf = city_gdf[1.13202 * city_gdf["INTPTLON"].astype("float") - city_gdf["INTPTLAT"].astype("float") + 124.38 < 0] #124.41461
    city_gjs = city_gdf.to_json()

    # Write geojson.
    city_gjs_path = file_path + "/resources/" + city + "_block_groups/" + city + \
        "_block_groups_" + year + ".geojson"
    with open(city_gjs_path, "w") as city_gjs_file:
        city_gjs_file.write(city_gjs)
