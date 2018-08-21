import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon
from geopandas.tools import sjoin
from matplotlib.font_manager import FontProperties
from scipy.stats import pearsonr
import pysal as ps
import numpy as np
import sys
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    fig_path = file_path + "/results/" + city + "_effect_size.pdf"
    Ylist = ["uber_sup", "lyft_sup", "taxi_sup", "uber_dem", "lyft_dem", "taxi_dem"]
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    matplotlib.rcParams.update({"font.size": 10})
    fig, ax = plt.subplots(nrows = 3, ncols = 4, figsize=(8, 4.5))
    map_path = file_path + "/resources/" + city + "_block_groups/" + city + "_bg_with_data_acs15.geojson"
    all_data_path = file_path + "/resources/" + city + "_data/" + city + "_all_group_geo.csv"
    map_df = gpd.read_file(map_path)
    map_df = map_df.to_crs({"init": "epsg:3857"})
    map_df["area"] = map_df["geometry"].area
    data_df = pd.read_csv(all_data_path)
    gdf = map_df.merge(data_df, on = "geo_id", how = "left")
    gdf = gdf.fillna(0)
    gdf.index = gdf["geo_id"]
    gdf["white"] = gdf["white"] / 100
    gdf["income"] = gdf["income"] / 1000

    if city == "sf":
        areas = [60750260011, 60750232001, 60750113001, 60750176012]
        Ycoef = {"parking_meter": [0.0136, 0.0047, 0.0085, 0.0066, 0.002, 0.0013],
                 "family_ratio": [-2.3186, -1.1234, -2.5165, -0.3969, -0.2072, -0.1211],
                 "white": [0.05, 0.0283, -0.1104, 0.0266, 0.0112, -0.0106]}
        Yrho = [0.0727, 0.0878, 0.0643, 0.0509, 0.0645, 0.0585]
        title = ["Excelsior,\nSF", "Hunter's Point,\nSF", "Chinatown,\nSF", "South of Market,\nSF"]
        keys = ["parking_meter", "family_ratio", "white"]

    if city == "nyc":
        areas = [360470085003, 360050189004, 360610174011, 360610119002]
        Ycoef = {"parking_meter": [0.0421, 0.0141, 0.0, 0.0122, 0.0032, 0.0],
                 "family_ratio": [-1.7693, -0.6729, 0.0, -0.236, -0.0699, 0.0],
                 "income": [0.007, 0.0017, 0.0, 0.001, 0.0002, 0.0]}
        Yrho = [0.108, 0.1036, 0.0, 0.0893, 0.0933, 0.0]
        title = ["Red Hook,\nBrooklyn, NYC", "Highbridge,\nBronx, NYC", "East Harlem,\nManhattan, NYC", " Gramercy Park,\nManhattan, NYC"]
        keys = ["parking_meter", "family_ratio", "income"]

    de_dict = {}
    te_dict = {}
    for key in keys:
        de_dict[key] = {}
        te_dict[key] = {}
        for area in areas:
            de_dict[key][area] = []
            te_dict[key][area] = []

    for key in keys:
        for area in areas:
            for yl, yc, yr in zip(Ylist, Ycoef[key], Yrho):
                bk = gdf[gdf["geo_id"] == area]
                de = 1.0 * bk[key] * yc / bk[yl]
                te = 1.0 * bk[key] * yc / (bk[yl] * (1 - yr))
                de_dict[key][area].append(de.values[0])
                te_dict[key][area].append(te.values[0])

    for j in range(3):
        for i in range(4):
            if city == "sf":
                x = ax[j, i].bar(np.arange(1,7), de_dict[keys[j]][areas[i]], width = 1)
                y = ax[j, i].bar(np.arange(1,7), te_dict[keys[j]][areas[i]], alpha = 0.15, width = 1)
                if j == 0:
                    ax[j, i].set_ylim([0, 1.6])
                    if i == 0:
                        ax[j, i].set_yticks([0, 1])
                        ax[j, i].set_yticklabels([" 0.0%", "1.0%"])
                if j == 1:
                    ax[j, i].set_ylim([-4.5, 0])
                    if i == 0:
                        ax[j, i].set_yticks([-4, -2, 0])
                        ax[j, i].set_yticklabels(["-4.0%", "-2.0%", "0.0%"])
                if j == 2:
                    ax[j, i].set_ylim([-1.5, 1.1])
                    if i == 0:
                        ax[j, i].set_yticks([-1, 0, 1])
                        ax[j, i].set_yticklabels(["-1.0%", "0.0%", "1.0%"])
            if city == "nyc":
                #log_flag = True if j == 2 else False
                log_flag = False
                x = ax[j, i].bar(np.arange(1,7), de_dict[keys[j]][areas[i]], width = 1, log = log_flag)
                y = ax[j, i].bar(np.arange(1,7), te_dict[keys[j]][areas[i]], alpha = 0.15, width = 1, log = log_flag)
                if j == 0:
                    ax[j, i].set_ylim([0, 8.5])
                    if i == 0:
                        ax[j, i].set_yticks([0, 4, 8])
                        ax[j, i].set_yticklabels([" 0.00%", " 4.00%", " 8.00%"])
                if j == 1:
                    ax[j, i].set_ylim([-8.8, 0])
                    if i == 0:
                        ax[j, i].set_yticks([-8, -4, 0])
                        ax[j, i].set_yticklabels(["-8.00%", "-4.00%", " 0.00%"])
                if j == 2:
                    ax[j, i].set_ylim([0, 0.8])
                    if i == 0:
                        ax[j, i].set_yticks([0, 0.4, 0.8])
                        ax[j, i].set_yticklabels([" 0.00%", " 0.40%", " 0.80%"])
            x[0].set_facecolor("#1C6C67")
            x[1].set_facecolor("#E70B81")
            x[2].set_facecolor("#FFB300")
            x[3].set_facecolor("#1C6C67")
            x[4].set_facecolor("#E70B81")
            x[5].set_facecolor("#FFB300")
            y[0].set_facecolor("#1C6C67")
            y[1].set_facecolor("#E70B81")
            y[2].set_facecolor("#FFB300")
            y[3].set_facecolor("#1C6C67")
            y[4].set_facecolor("#E70B81")
            y[5].set_facecolor("#FFB300")
            ax[j, i].set_xlim([0.5, 6.5])
            ax[j, i].axvline(x = 5, linewidth = 48.5, color = "k", alpha = 0.1)
            ax[j, i].set_xticks([])
            if j == 0:
                ax[j, i].set_title("  Supply  Demand", fontproperties = font)
            if j == 2:
                ax[j, i].set_xlabel(title[i], fontproperties = font)
            if i != 0:
                ax[j, i].set_yticks([])
            if i == 0:
                if j == 0:
                    ax[j, i].set_ylabel("On-Street\nParking Meter", fontproperties = font)
                if j == 1:
                    ax[j, i].set_ylabel("Family Ratio", fontproperties = font)
                if j == 2 and city == "sf":
                    ax[j, i].set_ylabel("White Number", fontproperties = font)
                if j == 2 and city == "nyc":
                    ax[j, i].set_ylabel("Median Income", fontproperties = font)

    ax[2, 3].hist([-1], 1, color = "#1C6C67", label = "Uber Direct Effect")
    ax[2, 3].hist([-1], 1, color = "#1C6C67", label = "Uber Indirect Effect", alpha = 0.15)
    ax[2, 3].hist([-1], 1, color = "#E70B81", label = "Lyft Direct Effect")
    ax[2, 3].hist([-1], 1, color = "#E70B81", label = "Lyft Indirect Effect", alpha = 0.15)
    if city == "sf":
        ax[2, 3].hist([-1], 1, color = "#FFB300", label = "Taxi Direct Effect")
        ax[2, 3].hist([-1], 1, color = "#FFB300", label = "Taxi Indirect Effect", alpha = 0.15)
        leg = plt.legend(bbox_to_anchor=(-3.5, -0.85, 0, 0), loc = 3, ncol = 3, borderaxespad = 0.)
    if city == "nyc":
        leg = plt.legend(bbox_to_anchor=(-2.6, -0.85, 0, 0), loc = 3, ncol = 2, borderaxespad = 0.)
    leg.get_frame().set_alpha(0)


    # Save figure.
    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)


