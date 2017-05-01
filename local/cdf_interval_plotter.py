from matplotlib.pyplot import *
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import sys, json

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    taxi_path = file_path + "/resources/sf_cdf/intervals/taxi.txt"
    uber_path = file_path + "/resources/sf_cdf/intervals/uber.txt"
    lyft_path = file_path + "/resources/sf_cdf/intervals/lyft.txt"
    fig_path = file_path + "/results/interval_cdf.pdf"

    # Read file.
    taxi_df = pd.read_csv(taxi_path, sep = "\t", names = ["interval", "cdf", "_0", "_1", "_2"])
    uber_df = pd.read_csv(uber_path, sep = "\t", names = ["interval", "cdf", "_0", "_1", "_2"])
    lyft_df = pd.read_csv(lyft_path, sep = "\t", names = ["interval", "cdf", "_0", "_1", "_2"])

    # Plot and save figure.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300"}
    font = FontProperties()
    font.set_weight("bold")
    matplotlib.rcParams.update({"font.size": 10})
    fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize=(6, 1.5))
    ax.set_xlim([-1, np.log10(10000)])
    ax.set_ylim([0, 1.0])
    ax.set_yticks([0.0, 0.5, 1.0])
    ax.set_ylabel("CDF", fontproperties = font)
    ax.set_xticks([-1, 0, np.log10(10), np.log10(60), np.log10(600), np.log10(3600)])
    ax.set_xticklabels(["0s", "1s", "10s", "1m", "10m", "1h"])
    ax.set_xlabel("Gap between Records", fontproperties = font)
    ax.plot(np.log10(uber_df["interval"]), uber_df["cdf"], color = c_dict["uber"], label = "Uber")
    ax.plot(np.log10(lyft_df["interval"]), lyft_df["cdf"], color = c_dict["lyft"], label = "Lyft")
    ax.plot(np.log10(taxi_df["interval"]), taxi_df["cdf"], color = c_dict["taxi"], label = "Taxi")
    ax.grid(True, zorder = 10)
    print uber_df["cdf"]
    print lyft_df["cdf"]
    print taxi_df["cdf"]
    ax.legend(loc = 4, frameon = False)
    plt.savefig(fig_path, bbox_inches = "tight")
