from sklearn import neighbors
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib
from matplotlib import *
import geopandas as gpd
import pandas as pd
import numpy as np
import json, re, os, sys, math, random
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42
matplotlib.rcParams["font.size"] = 10

def cut_max(x, maxv):
    if x < maxv:
        return x
    else:
        return maxv

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    sf_mp_path = file_path + "/resources/sf_block_groups/sf_user_info.txt"
    nyc_mp_path = file_path + "/resources/nyc_block_groups/nyc_user_info.txt"
    fig_path = file_path + "/results/measurement_points.pdf"
    fig, ax = plt.subplots(nrows = 1, ncols = 2, figsize=(8, 2.5))
    sf_mp = pd.read_csv(sf_mp_path)
    nyc_mp = pd.read_csv(nyc_mp_path, sep = ", ", engine = "python")
    
    # SF
    map_path = file_path + "/resources/sf_block_groups/sf_block_groups_2015.geojson"
    map_add_path = file_path + "/resources/sf_block_groups/sf_block_groups_nowater.geojson"
    plot_path = file_path + "/resources/sf_data/sf_overspace_plot_data.json"
    with open(plot_path, "r") as plot_file:
        data = json.loads(plot_file.read().strip("\n"))

    map = gpd.read_file(map_path)
    map = map[map["AWATER"] == 0]
    map["geoid"] = map["GEOID"].astype("int")
    map = map[map["geoid"] - map["geoid"] % 100000 == 60750100000]
    map = map[["geoid", "geometry"]]
    ids = map["geoid"].values

    map_add = gpd.read_file(map_add_path)
    map_add["geoid"] = map_add["stfid"].astype("int")
    map_add = map_add[["geoid", "geometry"]]
    
    #map = pd.concat([map_add, map])
    #map = gpd.GeoDataFrame(map)
    map = map_add
    map["area"] = map["geometry"].area * 20000000
    map["bg_lng"] = map.centroid.apply(lambda p: p.x)
    map["bg_lat"] = map.centroid.apply(lambda p: p.y)
    map = map[map["geoid"] != 60750179021]

    sup = pd.DataFrame.from_dict(data["sup"])
    sup["geoid"] = data["index"]
    sup = sup[sup["geoid"] != 60750601001]
    sup = sup[sup["geoid"] != 60750604001]
    sup = sup[sup["geoid"] != 60750332011]
    sup = sup[sup["geoid"] != 60750610002]
    sup = sup[sup["geoid"] != 60750264022]
    sup = sup[sup["geoid"] != 60750258002]
    sup[sup["geoid"] == 60750610001] = 1
    sup = map.merge(sup, on = "geoid", how = "left")

    # Read map data.
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    font2 = FontProperties()
    font2.set_size(10)
    alpha = 0.3
    k = 4
    cmap = "RdYlGn"
    sup["plot"] = sup["uber"]
    knn = neighbors.KNeighborsRegressor(k, "distance")
    train_x = sup[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
    train_y = sup["plot"].dropna().values
    predict_x = sup[["bg_lat", "bg_lng"]].values
    sup["plot"] = knn.fit(train_x, train_y).predict(predict_x)
    vmin = sup["plot"].min()
    vmax = sup["plot"].quantile(0.85)
    # plot
    sup.plot(ax = ax[0], linewidth = 0, column = "plot", cmap = cmap,
             alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
    # add colorbar
    fig = ax[0].get_figure()
    cax = fig.add_axes([0.19, 0.08, 0.22, 0.015])
    sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
    sm._A = []
    fig.colorbar(sm, cax = cax, alpha = alpha, extend = "both", orientation = "horizontal")
    
    # NYC
    map_path = file_path + "/resources/nyc_block_groups/nyc_bg_with_data_acs15.geojson"
    plot_path = file_path + "/resources/nyc_data/nyc_overspace_plot_data.json"
    with open(plot_path, "r") as plot_file:
        data = json.loads(plot_file.read().strip("\n"))

    map = gpd.read_file(map_path)
    map = map[map["population"].astype("float") > 10.0]
    map["geoid"] = map["geo_id"].astype("int")
    sup = pd.DataFrame.from_dict(data["sup"])
    sup["geoid"] = data["index"]
    sup = map.merge(sup, on = "geoid", how = "left")
    
    # Read map data.
    alpha = 0.3
    k = 4
    cmap = "RdYlGn"
    sup["plot"] = sup["uber"]
    vmin = sup["plot"].min()
    vmax = sup["plot"].quantile(0.85)
    # plot
    sup.plot(ax = ax[1], linewidth = 0, column = "plot", cmap = cmap,
             alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
             # add colorbar
    fig = ax[1].get_figure()
    cax = fig.add_axes([0.62, 0.08, 0.22, 0.015])
    sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
    sm._A = []
    fig.colorbar(sm, cax = cax, alpha = alpha, extend = "both", orientation = "horizontal")

    # Set figure configuration.
    ax[0].set_xlim([-122.513 - 0.05, -122.355 + 0.05])
    ax[0].set_ylim([37.707, 37.833])
    ax[0].text(-122.515, 37.833, "(a)           SF", fontproperties = font)
    ax[0].annotate("Hunter's Point", xy = (-122.357, 37.723), xytext = (-122.365, 37.737),
                   fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=-.2"))
    ax[0].annotate("Excelsior", xy = (-122.433, 37.725), xytext = (-122.563, 37.707),
                                  fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=-.2"))
    ax[0].annotate("Chinatown", xy = (-122.407, 37.796), xytext = (-122.42, 37.815),
                                  fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=-.2"))
    ax[0].annotate("South of Market", xy = (-122.386, 37.780), xytext = (-122.384, 37.79),
                                  fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=-.2"))
    ax[1].set_xlim([-74.055 - 0.05, -73.88 + 0.05])
    ax[1].set_ylim([40.64, 40.90])
    ax[1].text(-74.055, 40.90, "(b)              NYC", fontproperties = font)
    ax[1].annotate("Manhattan", xy = (-74.004, 40.764), xytext = (-74.055, 40.80),
                   fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=.2"))
    ax[1].annotate("Staten Island", xy = (-74.103, 40.645), xytext = (-74.105, 40.67),
                   fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=.2"))
    ax[1].annotate("Bronx", xy = (-73.88, 40.83), xytext = (-73.88, 40.86),
                   fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=-.2"))
    ax[1].annotate("Queens", xy = (-73.925, 40.71), xytext = (-73.908, 40.733),
                   fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=-.2"))
    ax[1].annotate("Brooklyn", xy = (-73.987, 40.649), xytext = (-73.962, 40.663),
                   fontproperties = font2, arrowprops = dict(arrowstyle="->", connectionstyle = "arc3,rad=-.2"))
    for i in range(2):
        ax[i].set_axis_off()
        ax[i].xaxis.set_major_locator(plt.NullLocator())
        ax[i].yaxis.set_major_locator(plt.NullLocator())

    # Plot data.
    ax[0].scatter(sf_mp["lng"], sf_mp["lat"], c = "k", s = 2, marker = "^")
    ax[1].scatter(nyc_mp["lng"], nyc_mp["lat"], c = "k", s = 2, marker = "^")

    # Show figure.
    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
