from sklearn import neighbors
from matplotlib.font_manager import FontProperties
from matplotlib.gridspec import GridSpec
import matplotlib.pyplot as plt
import matplotlib
from matplotlib import *
import geopandas as gpd
import pandas as pd
import numpy as np
import sys, json
from scipy.stats import pearsonr
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42

def upperfirst(app):
    app = app.split("-")
    return app[0][0].upper() + app[0][1:] if len(app) == 1 else \
        app[0][0].upper() + app[0][1:] + " vs " + app[1][0].upper() + app[1][1:]

def plot():
    map_path = file_path + "/resources/sf_block_groups/sf_block_groups_nowater.geojson"
    coc_path = file_path + "/resources/sf_block_groups/coc"
    plot_path = file_path + "/resources/sf_data/sf_overspace_plot_data.json"
    fig_path = file_path + "/results/sf_change_overspace.pdf"

    # Read data.
    with open(plot_path, "r") as plot_file:
        data = json.loads(plot_file.read().strip("\n"))
    
    coc = gpd.read_file(coc_path)
    coc = coc[coc["GEOID"].astype("int") - coc["GEOID"].astype("int") % 1000000 == 6075000000]
    coc = coc[coc["GEOID"].astype("int") != 6075017902]
    coc = coc[coc["COCFLAG__1"] == 1]
    coc = coc.to_crs({"init": "epsg:4326"})

    map = gpd.read_file(map_path)
    map["geoid"] = map["stfid"].astype("int")
    map = map[["geoid", "geometry"]]
    map["bg_lng"] = map.centroid.apply(lambda p: p.x)
    map["bg_lat"] = map.centroid.apply(lambda p: p.y)
    map = map[map["geoid"] != 60750179021]

    # Get supply curve data
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

    # Get price curve data
    pri = pd.DataFrame.from_dict(data["pri"])
    pri["geoid"] = data["index"]
    pri = map.merge(pri, on = "geoid", how = "left")

    # Plot parameter and setting.
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    matplotlib.rcParams.update({"font.size": 6})
    alpha = 0.5
    alpha2 = 0.3
    k = 2
    bar_cons = 0.66
    bar_mv = 0.27
    for i in [0, 1, 2, 3, 4]:
        ax[i].set_xlim([-122.513, -122.355])
        ax[i].set_ylim([37.707, 37.833])
        ax[i].set_axis_off()
        ax[i].xaxis.set_major_locator(plt.NullLocator())
        ax[i].yaxis.set_major_locator(plt.NullLocator())
        coc.plot(ax = ax[i], linewidth = 0.5, alpha = 0)
    app_list = ["uber", "lyft", "taxi"]
    cmap = "RdYlGn"

    f = 0
    for i in [0, 1, 2]:
        sup["plot"] = sup[app_list[i]] #/ sup["area"] * 581
        knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
        train_x = sup[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
        train_y = sup["plot"].dropna().values
        predict_x = sup[["bg_lat", "bg_lng"]].values
        sup["plot"] = knn.fit(train_x, train_y).predict(predict_x)
        vmin = sup["plot"].min()
        vmax = sup["plot"].quantile(0.95)
        # plot
        sup.plot(ax = ax[i], linewidth = 0, column = "plot", cmap = cmap,
            alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[i].set_title(upperfirst(app_list[i]) + " Supply", fontproperties = font)
        fig = ax[i].get_figure()
        cax = fig.add_axes([0.128 + 0.087 * i, 0.07, 0.07, 0.02])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax, alpha = alpha2, extend = "both", orientation = "horizontal")

    cmap = "RdYlGn_r"
    f = 2
    for i in [3, 4]:
        pri["plot"] = (pri[app_list[i - 3]] - 1) * 100
        knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
        train_x = pri[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
        train_y = pri["plot"].dropna().values
        predict_x = pri[["bg_lat", "bg_lng"]].values
        pri["plot"] = knn.fit(train_x, train_y).predict(predict_x)
        vmin = 0
        vmax = 12
        print pri["plot"].max() - pri["plot"].min()
        print pri["plot"].std()
        # plot
        pri.plot(ax = ax[i], linewidth = 0, column = "plot", cmap = cmap,
            alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[i].set_title(upperfirst(app_list[i - 3]) + " Price", fontproperties = font)
        fig = ax[i].get_figure()
        cax = fig.add_axes([0.128 + 0.087 * i, 0.07, 0.07, 0.02])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax, alpha = alpha2, extend = "both", orientation = "horizontal")

    map_path = file_path + "/resources/nyc_block_groups/nyc_bg_with_data_acs15.geojson"
    plot_path = file_path + "/resources/nyc_data/nyc_overspace_plot_data.json"
    fig_path = file_path + "/results/nyc_change_overspace.pdf"
    
    # Read data.
    with open(plot_path, "r") as plot_file:
        data = json.loads(plot_file.read().strip("\n"))

    map = gpd.read_file(map_path)
    coc = map.sort_values("income")[:80]
    map = map[map["population"].astype("float") > 10.0]
    map["geoid"] = map["geo_id"].astype("int")
    map = map[["geoid", "geometry"]]
    map["bg_lng"] = map.centroid.apply(lambda p: p.x)
    map["bg_lat"] = map.centroid.apply(lambda p: p.y)

    # Get supply curve data
    sup = pd.DataFrame.from_dict(data["sup"])
    sup["geoid"] = data["index"]
    sup = map.merge(sup, on = "geoid", how = "left")
    
    # Get price curve data
    pri = pd.DataFrame.from_dict(data["pri"])
    pri["geoid"] = data["index"]
    pri = pri[pri["uber"] > 1.0]
    pri = pri[pri["lyft"] > 1.0]
    pri = map.merge(pri, on = "geoid", how = "left")
    
    # Plot parameter and setting.
    bar_cons = 0.66
    bar_mv = 0.27
    for i in [5, 6, 7, 8]:
        ax[i].set_xlim([-74.055, -73.88])
        ax[i].set_ylim([40.64, 40.90])
        ax[i].set_axis_off()
        ax[i].xaxis.set_major_locator(plt.NullLocator())
        ax[i].yaxis.set_major_locator(plt.NullLocator())
        coc.plot(ax = ax[i], linewidth = 0.5, alpha = 0)
    app_list = ["uber", "lyft"]
    cmap = "RdYlGn"

    f = 0
    for i in [5, 6]:
        sup["plot"] = sup[app_list[i - 5]]
        vmin = sup["plot"].min()
        if i == 5:
            vmax = 7 #sup["plot"].quantile(0.9)
        else:
            vmax = 5
        # plot
        sup.plot(ax = ax[i], linewidth = 0, column = "plot", cmap = cmap,
                 alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[i].set_title(upperfirst(app_list[i - 5]) + " Supply", fontproperties = font)
        fig = ax[i].get_figure()
        cax = fig.add_axes([0.132 + 0.087 * i, 0.07, 0.07, 0.02])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax, alpha = alpha2, extend = "both", orientation = "horizontal")

    cmap = "RdYlGn_r"
    f = 2
    for i in [7, 8]:
        pri["plot"] = (pri[app_list[i - 3 - 4]] - 1 ) * 100
        knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
        train_x = pri[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
        train_y = pri["plot"].dropna().values
        predict_x = pri[["bg_lat", "bg_lng"]].values
        pri["plot"] = knn.fit(train_x, train_y).predict(predict_x)
        vmin = 0
        if i == 7:
            vmax = 2.5 #sup["plot"].quantile(0.9)
        else:
            vmax = 7
        print pri["plot"].max() - pri["plot"].min()
        print pri["plot"].std()
        # plot
        pri.plot(ax = ax[i], linewidth = 0, column = "plot", cmap = cmap,
                 alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[i].set_title(upperfirst(app_list[i - 3 - 4]) + " Price", fontproperties = font)
        fig = ax[i].get_figure()
        cax = fig.add_axes([0.132 + 0.087 * i, 0.07, 0.07, 0.02])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax, alpha = alpha2, extend = "both", orientation = "horizontal")

if __name__ == "__main__":
    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    fig_path = file_path + "/results/change_overspace.pdf"
    fig, ax = plt.subplots(nrows = 1, ncols = 9, figsize=(16, 1.8))
    plot()
    
    # Save figure.
    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
