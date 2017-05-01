import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
import numpy as np
import json, re, os, sys, math, random

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
    sf_data_path = file_path + "/resources/sf_block_groups/sf_block_groups_nowater.geojson"
    nyc_data_path = file_path + "/resources/nyc_block_groups/nyc_bg_with_data_acs15.geojson"
    sf_mp_path = file_path + "/resources/sf_block_groups/sf_user_info.txt"
    nyc_mp_path = file_path + "/resources/nyc_block_groups/nyc_user_info.txt"
    fig_path = file_path + "/results/measurement_points.pdf"

    # Read map data.
    sf_df = gpd.read_file(sf_data_path)
    sf_df["dens"] = sf_df["pop2000"].astype("float") / (sf_df["geometry"].area * 100000000) 
    sf_df["plot"] = 2 - sf_df["dens"].apply(lambda x: cut_max(x, 2))
    nyc_df = gpd.read_file(nyc_data_path)
    nyc_df = nyc_df[nyc_df["population"].astype("float") > 10.0]
    nyc_df["dens"] = nyc_df["population"].astype("float") / (nyc_df["geometry"].area * 100000000)
    nyc_df["plot"] = 5 - nyc_df["dens"].apply(lambda x: cut_max(x, 5))
    sf_mp = pd.read_csv(sf_mp_path)
    nyc_mp = pd.read_csv(nyc_mp_path, sep = ", ", engine = "python")

    # Set figure configuration.
    fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize=(2.7, 2.7))
    fontsize = 10
    ax.set_xlim([-122.513, -122.355])
    ax.set_ylim([37.707, 37.833])
    ax.set_axis_off()
    ax.xaxis.set_major_locator(plt.NullLocator())
    ax.yaxis.set_major_locator(plt.NullLocator())
    """
    ax[1].set_xlim([-74.26, -73.82])
    ax[1].set_ylim([40.49, 40.92])
    for i in range(2):
        ax[i].set_axis_off()
        ax[i].xaxis.set_major_locator(plt.NullLocator())
        ax[i].yaxis.set_major_locator(plt.NullLocator())
    """

    # Plot data.
    gpd.plotting.plot_dataframe(sf_df, ax = ax, linewidth = 0, column = "plot",
        cmap = "RdYlGn", alpha = 0.5, k = 10)
    ax.scatter(sf_mp["lng"], sf_mp["lat"], c = "k", s = 4, marker = "^")
    """
    gpd.plotting.plot_dataframe(nyc_df, ax = ax[1], linewidth = 0, column = "plot",
        cmap = "RdYlGn", alpha = 0.6, k = 10)
    ax[1].scatter(nyc_mp["lng"], nyc_mp["lat"], c = "k", s = 2, marker = "^")
    """

    # Show figure.
    plt.gca().set_axis_off()
    plt.subplots_adjust(top = 1, bottom = 0, right = 1, left = 0, hspace = 0, wspace = 0)
    plt.margins(0, 0)
    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
