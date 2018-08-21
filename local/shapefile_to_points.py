import geopandas as gpd
import matplotlib.pyplot as plt
import sys

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    year = "2016"
    city = "bos"
    key_dict = {"2010": "COUNTYFP10", "2015": "COUNTYFP"}
    city_dict = {"sf": ["06", ["075"]],
        "nyc": ["36", ["005", "047", "061", "081", "085"]],
        "bos": ["25"]}

    state_shp_path = file_path + "/resources/" + city + "_block_groups/tl_" + year + "_" + \
        city_dict[city][0] + "_tract/tl_" + year + "_" + city_dict[city][0] + "_tract.shp"
    state_gdf = gpd.read_file(state_shp_path)

    gdf = state_gdf
    """
    gdf = gdf[gdf["INTPTLON"].astype("float") < -71.209]
    #gdf = gdf[gdf["INTPTLON"].astype("float") > -71.007]
    gdf = gdf[gdf["INTPTLAT"].astype("float") > 42.262]
    gdf = gdf[gdf["INTPTLAT"].astype("float") < 42.424]
    """
    gdf.plot()
    plt.show()

    """
    # Write geojson.
    city_gjs_path = file_path + "/resources/" + city + "_block_groups/" + city + \
        "_block_groups_" + year + ".geojson"
    with open(city_gjs_path, "w") as city_gjs_file:
        city_gjs_file.write(city_gjs)
    """
