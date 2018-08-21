from matplotlib.pyplot import *
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import os, sys, json, random
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42

def get_cdf(t, df, total, key):
    return (df[df[key] <= t]["car_id"].sum() + 0.0) / total

def to_time(x, pos):
    return str(x) + "s"

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    idle_path = file_path + "/resources/sf_data/sf_idlearea.csv"
    proc_path = file_path + "/resources/sf_data/sf_idlearea_proc.csv"
    fig_path = file_path + "/results/idlearea_cdf.pdf"

    if os.path.isfile(proc_path): # if no processing file
        idle_df = pd.read_csv(idle_path)
        idle_df["idletime"] = idle_df["max(timestamp)"] - idle_df["min(timestamp)"]
        idle_df = idle_df[idle_df["idletime"] > 86400 * 30]
        idle_df["idlearea"] = idle_df["count"]# * (1 - random.random() / 10)
        idle_df = idle_df[idle_df["idlearea"] > 0]
        idle_df[["car_type", "car_id", "idlearea"]].to_csv(proc_path)

    idle_df = pd.read_csv(proc_path) # if processing file exist

    # Lyft.
    lyft_df = idle_df[idle_df["car_type"] == "Lyft"].groupby("idlearea").count()
    lyft_df["idlearea"] = lyft_df.index
    lyft_total = lyft_df["car_id"].sum()
    lyft_df["cdf"] = lyft_df["idlearea"].apply(lambda t: get_cdf(t, lyft_df, lyft_total, key = "idlearea"))

    # Taxi.
    taxi_df = idle_df[idle_df["car_type"] == "Taxi"].groupby("idlearea").count()
    taxi_df["idlearea"] = taxi_df.index
    taxi_total = taxi_df["car_id"].sum()
    taxi_df["cdf"] = taxi_df["idlearea"].apply(lambda t: get_cdf(t, taxi_df, taxi_total, key = "idlearea"))

    # Plot and save figure.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300"}
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    matplotlib.rcParams.update({"font.size": 10})
    fig, ax = plt.subplots(nrows = 1, ncols = 2, figsize=(8, 1.2))
    #ax.set_xlim([0, np.log10(1000000)])
    ax[1].text(15, 0.82, "(b)", fontproperties = font)
    ax[1].set_xlim([0, 581])
    ax[1].set_ylim([0, 1.0])
    ax[1].set_yticks([0.0, 0.5, 1.0])
    ax[1].set_yticklabels(["", "", ""])
    ax[1].grid(True, zorder = 10)
    #ax.set_xticks([0, np.log10(10), np.log10(50), np.log10(200), np.log10(500)])
    #ax.set_xticklabels(["1", "10", "50", "10m", "1h", "1d"])
    ax[1].set_xlabel("Visited Block Groups", fontproperties = font)
    #ax.set_xscale("log")
    #ax.plot(np.log10(uber_df["idlearea"]), uber_df["cdf"], color = c_dict["uber"], label = "Uber")
    ax[1].plot(lyft_df["idlearea"], lyft_df["cdf"], color = c_dict["lyft"], label = "Lyft (>30d)")
    ax[1].plot(taxi_df["idlearea"], taxi_df["cdf"], color = c_dict["taxi"], label = "Taxi (>30d)")
    ax[1].legend(loc = 4, frameon = False)

    idle_path = file_path + "/resources/sf_data/sf_lifespan.csv"
    
    # Read file.
    idle_df = pd.read_csv(idle_path)
    idle_df["lifespan"] = idle_df["max(timestamp)"] - idle_df["min(timestamp)"]
    idle_df = idle_df[idle_df["lifespan"] > 0]
    
    # Uber.
    """
    uber_df = idle_df[idle_df["car_type"] == "UberX"].groupby("lifespan").count()
    uber_df["lifespan"] = uber_df.index
    uber_total = uber_df["car_id"].sum()
    uber_df["cdf"] = uber_df["lifespan"].apply(lambda t: get_cdf(t, uber_df, uber_total, key = "lifespan"))
    """
    
    # Lyft.
    lyft_df = idle_df[idle_df["car_type"] == "Lyft"].groupby("lifespan").count()
    lyft_df["lifespan"] = lyft_df.index
    lyft_total = lyft_df["car_id"].sum()
    lyft_df["cdf"] = lyft_df["lifespan"].apply(lambda t: get_cdf(t, lyft_df, lyft_total, key = "lifespan"))
    select = lyft_df[lyft_df["lifespan"] >= 30 * 24 * 60 * 60]
    print select
    print len(select)
    print len(lyft_df["cdf"])
    
    # Taxi.
    taxi_df = idle_df[idle_df["car_type"] == "Taxi"].groupby("lifespan").count()
    taxi_df["lifespan"] = taxi_df.index
    taxi_total = taxi_df["car_id"].sum()
    taxi_df["cdf"] = taxi_df["lifespan"].apply(lambda t: get_cdf(t, taxi_df, taxi_total, key = "lifespan"))
    select = taxi_df[taxi_df["lifespan"] >= 30 * 24 * 60 * 60]
    print select
    print len(select)
    print len(taxi_df)
    
    # Plot and save figure.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300"}
    ax[0].text(0.2, 0.82, "(a)", fontproperties = font)
    ax[0].set_xlim([0, np.log10(50000000)])
    ax[0].set_ylim([0, 1.0])
    ax[0].set_yticks([0.0, 0.5, 1.0])
    ax[0].set_ylabel("CDF", fontproperties = font)
    ax[0].set_xticks([0, np.log10(60), np.log10(3600), np.log10(86400), np.log10(86400 * 30)])
    ax[0].set_xticklabels(["1s", "1m", "1h", "1d", "30d"])
    ax[0].set_xlabel("Lifespan (Before Splitting)", fontproperties = font)
    #ax[0].plot(np.log10(uber_df["lifespan"]), uber_df["cdf"], color = c_dict["uber"], label = "Uber")
    ax[0].plot(np.log10(lyft_df["lifespan"]), lyft_df["cdf"], color = c_dict["lyft"], label = "Lyft")
    ax[0].plot(np.log10(taxi_df["lifespan"]), taxi_df["cdf"], color = c_dict["taxi"], label = "Taxi")
    ax[0].grid(True, zorder = 10)
    ax[0].axvline(x = np.log10(86400 * 30 * 30), linewidth = 78, color = "gray", alpha = 0.15)
    ax[0].legend(loc = 3, frameon = False)

    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
