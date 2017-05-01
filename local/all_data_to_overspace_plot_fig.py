from sklearn import neighbors
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib
from matplotlib import *
import geopandas as gpd
import pandas as pd
import numpy as np
import sys, json

def upperfirst(app):
    app = app.split("-")
    return app[0][0].upper() + app[0][1:] if len(app) == 1 else \
        app[0][0].upper() + app[0][1:] + " vs " + app[1][0].upper() + app[1][1:]

if __name__ == "__main__":
    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    map_path = file_path + "/resources/sf_block_groups/sf_block_groups_nowater.geojson"
    plot_path = file_path + "/resources/" + city + "_data/" + city + "_overspace_plot_data.json"
    fig_path = file_path + "/results/" + city + "_change_overspace.pdf"

    # Read data.
    with open(plot_path, "r") as plot_file:
        data = json.loads(plot_file.read().strip("\n"))
    map = gpd.read_file(map_path)
    map["geoid"] = map["stfid"].astype("int")
    map["area"] = map["geometry"].area * 20000000
    map["bg_lng"] = map.centroid.apply(lambda p: p.x)
    map["bg_lat"] = map.centroid.apply(lambda p: p.y)

    # Get supply curve data
    sup = pd.DataFrame.from_dict(data["sup"])
    sup["geoid"] = data["index"]
    sup = map.merge(sup, on = "geoid", how = "left")

    # Get demand curve data
    dem = pd.DataFrame.from_dict(data["dem"])
    dem["geoid"] = data["index"]
    dem = map.merge(dem, on = "geoid", how = "left")

    # Get price curve data
    pri = pd.DataFrame.from_dict(data["pri"])
    pri["geoid"] = data["index"]
    pri = map.merge(pri, on = "geoid", how = "left")
    pri["uber-taxi"] = 0.0
    pri["lyft-taxi"] = 0.0

    # Plot parameter and setting.
    font = FontProperties()
    font.set_weight("bold")
    font.set_size("small")
    matplotlib.rcParams.update({"font.size": 8})
    alpha = 1.0
    k = 4
    bar_cons = 0.66
    bar_mv = 0.27
    fig, ax = plt.subplots(nrows = 3, ncols = 3, figsize=(8, 7))
    for row in ax:
        for an_ax in row:
            an_ax.set_xlim([-122.513, -122.32])
            an_ax.set_ylim([37.707, 37.833])
            an_ax.set_axis_off()
            an_ax.xaxis.set_major_locator(plt.NullLocator())
            an_ax.yaxis.set_major_locator(plt.NullLocator())
    app_list = ["uber", "lyft", "taxi"]
    sim_list = ["uber-lyft", "uber-taxi", "lyft-taxi"]
    cmap = "PuBu"

    # 1: Supply
    f = 0
    for i in range(len(app_list)):
        sup["plot"] = sup[app_list[i]] / sup["area"] * 581
        knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
        train_x = sup[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
        train_y = sup["plot"].dropna().values
        predict_x = sup[["bg_lat", "bg_lng"]].values
        sup["plot"] = knn.fit(train_x, train_y).predict(predict_x)
        vmin = sup["plot"].min()
        vmax = sup["plot"].quantile(0.85)
        # plot
        sup.plot(ax = ax[f, i], linewidth = 0, column = "plot", cmap = cmap,
            alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[f, i].text(-122.475, 37.817, upperfirst(app_list[i]) + " Sup.", fontproperties = font)
        # add colorbar
        fig = ax[f, i].get_figure()
        cax = fig.add_axes([0.32 + i * 0.273, bar_cons - bar_mv * f, 0.008, 0.12])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax)

    """
    # 2: Supply Similarity
    f = 1
    for i in range(len(sim_list)):
        sup["plot"] = sup[sim_list[i]]
        knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
        train_x = sup[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
        train_y = sup["plot"].dropna().values
        predict_x = sup[["bg_lat", "bg_lng"]].values
        sup["plot"] = knn.fit(train_x, train_y).predict(predict_x)
        vmin = sup["plot"].min()
        vmax = sup["plot"].quantile(0.95)
        # plot
        sup.plot(ax = ax[f, i], linewidth = 0, column = "plot", cmap = cmap,
            alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[f, i].text(-122.485, 37.817, upperfirst(sim_list[i]) + "\n  Sup. Sim.", fontproperties = font)
        # add colorbar
        fig = ax[f, i].get_figure()
        cax = fig.add_axes([0.32 + i * 0.273, bar_cons - bar_mv * f, 0.008, 0.07])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax)
    """

    # 3: Demand
    f = 1
    for i in range(len(app_list)):
        dem["plot"] = dem[app_list[i]] / sup["area"] * 581
        knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
        train_x = dem[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
        train_y = dem["plot"].dropna().values
        predict_x = dem[["bg_lat", "bg_lng"]].values
        dem["plot"] = knn.fit(train_x, train_y).predict(predict_x)
        vmin = dem["plot"].min()
        vmax = dem["plot"].quantile(0.85)
        # plot
        dem.plot(ax = ax[f, i], linewidth = 0, column = "plot", cmap = cmap,
            alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[f, i].text(-122.475, 37.817, upperfirst(app_list[i]) + " Dem.", fontproperties = font)
        # add colorbar
        fig = ax[f, i].get_figure()
        cax = fig.add_axes([0.32 + i * 0.273, bar_cons - bar_mv * f, 0.008, 0.12])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax)

    """
    # 4: Demand Similarity
    f = 3
    for i in range(len(sim_list)):
        dem["plot"] = dem[sim_list[i]]
        knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
        train_x = dem[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
        train_y = dem["plot"].dropna().values
        predict_x = dem[["bg_lat", "bg_lng"]].values
        dem["plot"] = knn.fit(train_x, train_y).predict(predict_x)
        vmin = dem["plot"].min()
        vmax = dem["plot"].quantile(0.95)
        # plot
        dem.plot(ax = ax[f, i], linewidth = 0, column = "plot", cmap = cmap,
            alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[f, i].text(-122.485, 37.817, upperfirst(sim_list[i]) + "\n  Dem. Sim.", fontproperties = font)
        # add colorbar
        fig = ax[f, i].get_figure()
        cax = fig.add_axes([0.32 + i * 0.273, bar_cons - bar_mv * f, 0.008, 0.07])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax)
    """

    # 5: Price and Price Similarity
    f = 2
    for i in range(len(app_list) - 1):
        pri["plot"] = pri[app_list[i]]
        knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
        train_x = pri[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
        train_y = pri["plot"].dropna().values
        predict_x = pri[["bg_lat", "bg_lng"]].values
        pri["plot"] = knn.fit(train_x, train_y).predict(predict_x)
        vmin = 1.0
        vmax = pri["plot"].quantile(0.95)
        # plot
        pri.plot(ax = ax[f, i], linewidth = 0, column = "plot", cmap = cmap,
            alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
        ax[f, i].text(-122.475, 37.817, upperfirst(app_list[i]) + " Pri.", fontproperties = font)
        # add colorbar
        fig = ax[f, i].get_figure()
        cax = fig.add_axes([0.32 + i * 0.273, bar_cons - bar_mv * f, 0.008, 0.12])
        sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
        sm._A = []
        fig.colorbar(sm, cax = cax)
    i += 1
    pri["plot"] = pri[sim_list[i - 2]]
    knn = neighbors.KNeighborsRegressor(k, "distance") # Fill empty area.
    train_x = pri[["plot", "bg_lat", "bg_lng"]].dropna()[["bg_lat", "bg_lng"]].values
    train_y = pri["plot"].dropna().values
    predict_x = pri[["bg_lat", "bg_lng"]].values
    pri["plot"] = knn.fit(train_x, train_y).predict(predict_x)
    vmin = 0
    vmax = 0.7
    # plot
    pri.plot(ax = ax[f, i], linewidth = 0, column = "plot", cmap = cmap,
    alpha = alpha, k = 10, vmin = vmin, vmax = vmax)
    ax[f, i].text(-122.485, 37.817, upperfirst(sim_list[i]) + "\n  Pri. Sim.", fontproperties = font)
    # add colorbar
    fig = ax[f, i].get_figure()
    cax = fig.add_axes([0.32 + i * 0.273, bar_cons - bar_mv * f, 0.008, 0.12])
    sm = plt.cm.ScalarMappable(cmap = cmap, norm = plt.Normalize(vmin = vmin, vmax = vmax))
    sm._A = []
    fig.colorbar(sm, cax = cax)

    ax[0, 0].text(-122.511, 37.825, "(a)")
    ax[1, 0].text(-122.511, 37.825, "(b)")
    ax[2, 0].text(-122.511, 37.825, "(c)")
    ax[2, 2].text(-122.511, 37.825, "(d)")

    ax[0, 0].axhline(y = 37.69, color = "k", linewidth = 1,zorder = 0, clip_on = False)
    ax[0, 1].axhline(y = 37.69, color = "k", linewidth = 1,zorder = 0, clip_on = False)
    ax[0, 2].axhline(y = 37.69, color = "k", linewidth = 1,zorder = 0, clip_on = False)
    ax[1, 0].axhline(y = 37.69, color = "k", linewidth = 1,zorder = 0, clip_on = False)
    ax[1, 1].axhline(y = 37.69, color = "k", linewidth = 1,zorder = 0, clip_on = False)
    ax[1, 2].axhline(y = 37.69, color = "k", linewidth = 1,zorder = 0, clip_on = False)
    ax[2, 2].axvline(x = -122.528, color = "k", linewidth = 1,zorder = 0, clip_on = False)

    # Save figure.
    plt.savefig(fig_path, bbox_inches = "tight")
