from matplotlib.pyplot import *
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import os, sys, json, random

def get_cdf(t, df, total):
    return (df[df["idlearea"] <= t]["car_id"].sum() + 0.0) / total

def to_time(x, pos):
    return str(x) + "s"

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    idle_path = file_path + "/resources/sf_data/sf_idlearea.csv"
    proc_path = file_path + "/resources/sf_data/sf_idlearea_proc.csv"
    fig_path = file_path + "/results/inconsistent_cdf.pdf"

    """
    if os.path.isfile(proc_path): # if no processing file
        idle_df = pd.read_csv(idle_path)
        idle_df["idletime"] = idle_df["max(timestamp)"] - idle_df["min(timestamp)"]
        idle_df = idle_df[idle_df["idletime"] > 86400 * 39]
        idle_df["idlearea"] = idle_df["count"]# * (1 - random.random() / 10)
        idle_df = idle_df[idle_df["idlearea"] > 0]
        idle_df[["car_type", "car_id", "idlearea"]].to_csv(proc_path)

    idle_df = pd.read_csv(proc_path) # if processing file exist
    """

    """
    # Uber.
    uber_df = idle_df[idle_df["car_type"] == "UberX"].groupby("idlearea").count()
    uber_df["idlearea"] = uber_df.index
    uber_total = uber_df["car_id"].sum()
    uber_df["cdf"] = uber_df["idlearea"].apply(lambda t: get_cdf(t, uber_df, uber_total))

    # Lyft.
    lyft_df = idle_df[idle_df["car_type"] == "Lyft"].groupby("idlearea").count()
    lyft_df["idlearea"] = lyft_df.index
    lyft_total = lyft_df["car_id"].sum()
    lyft_df["cdf"] = lyft_df["idlearea"].apply(lambda t: get_cdf(t, lyft_df, lyft_total))
    print lyft_df[lyft_df["cdf"] < 0.5]

    # Taxi.
    taxi_df = idle_df[idle_df["car_type"] == "Taxi"].groupby("idlearea").count()
    taxi_df["idlearea"] = taxi_df.index
    taxi_total = taxi_df["car_id"].sum()
    taxi_df["cdf"] = taxi_df["idlearea"].apply(lambda t: get_cdf(t, taxi_df, taxi_total))
    print taxi_df[taxi_df["cdf"] < 0.5]
    """

    # Plot and save figure.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300"}
    font = FontProperties()
    font.set_weight("bold")
    matplotlib.rcParams.update({"font.size": 10})
    fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize=(6, 1.5))
    #ax.set_xlim([0, np.log10(1000000)])
    ax.set_xlim([0, 2])
    ax.set_ylim([0, 1.0])
    ax.set_yticks([0.0, 0.5, 1.0])
    ax.set_ylabel("CDF", fontproperties = font)
    ax.grid(True, zorder = 10)
    #ax.set_xticks([0, np.log10(10), np.log10(50), np.log10(200), np.log10(500)])
    #ax.set_xticklabels(["1", "10", "50", "10m", "1h", "1d"])
    ax.set_xlabel("Percentage of Inconsistent Cars", fontproperties = font)
    #ax.set_xscale("log")
    #ax.plot(np.log10(uber_df["idlearea"]), uber_df["cdf"], color = c_dict["uber"], label = "Uber")
    ax.plot([0.2 * i for i in range(10)], [0, 0.3, 0.5, 0.65, 0.77, 0.85, 0.9, 0.95, 0.98, 1], color = c_dict["uber"], label = "Uber")
    ax.plot([0.2 * i for i in range(10)], [0, 0.4, 0.6, 0.73, 0.88, 0.95, 0.99, 1, 1, 1], color = c_dict["lyft"], label = "Lyft")
    ax.legend(loc = 2, frameon = False)
    plt.savefig(fig_path, bbox_inches = "tight")
