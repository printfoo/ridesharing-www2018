from datetime import datetime
from matplotlib.pyplot import *
from matplotlib.gridspec import GridSpec
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import os, sys, json
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42

def get_cdf(t, df, total):
    return (df[df["idletime"] <= t]["car_id"].sum() + 0.0) / total

def to_time(x, pos):
    return str(x) + "s"

def get_slot(t):
    bos_hour = int(datetime.fromtimestamp(t).strftime("%H"))
    sf_hour = ( 24 + bos_hour - 3 ) % 24
    if sf_hour in [22, 23, 0, 1]:
        slot = 22
    elif sf_hour in [2, 3, 4, 5]:
        slot = 2
    elif sf_hour in [6, 7, 8, 9]:
        slot = 6
    elif sf_hour in [10, 11, 12, 13]:
        slot = 10
    elif sf_hour in [14, 15, 16, 17]:
        slot = 14
    else:
        slot = 18
    return slot

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    sf_idle_path = file_path + "/resources/sf_data/sf_idletime.csv"
    sf_proc_path = file_path + "/resources/sf_data/sf_idletime_proc.csv"
    nyc_idle_path = file_path + "/resources/nyc_data/nyc_idletime.csv"
    nyc_proc_path = file_path + "/resources/nyc_data/nyc_idletime_proc.csv"
    fig_path = file_path + "/results/idletime_vio.pdf"

    sf_idle_df = pd.read_csv(sf_proc_path)
    nyc_idle_df = pd.read_csv(nyc_proc_path)
    idle_df = pd.concat([sf_idle_df, nyc_idle_df])

    # Data.
    vio = {}
    uber_df = idle_df[idle_df["car_type"] == "UberX"]
    slot_list = [2, 6, 10, 14, 18, 22]
    for slot in slot_list:
        df = idle_df[idle_df["slot"] == slot]
        vio[slot] = []
        vio[slot].append(np.log10(df[df["car_type"] == "UberX"]["idletime"].values))
        vio[slot].append(np.log10(df[df["car_type"] == "Lyft"]["idletime"].values))
        vio[slot].append(np.log10(df[df["car_type"] == "Taxi"]["idletime"].values))

    # Plot.
    fig = plt.subplots(figsize=(16, 1.2))
    gs = GridSpec(1, 23)
    ax = [None for i in range(9)]
    ax[0] = plt.subplot(gs[0, :5])
    ax[1] = plt.subplot(gs[0, 5:10])
    ax[2] = plt.subplot(gs[0, 11:13])
    ax[3] = plt.subplot(gs[0, 13:15])
    ax[4] = plt.subplot(gs[0, 15:17])
    ax[5] = plt.subplot(gs[0, 17:19])
    ax[6] = plt.subplot(gs[0, 19:21])
    ax[7] = plt.subplot(gs[0, 21:23])

    # Violin.
    c_dict = {0: "#1C6C67", 1: "#E70B81", 2: "#FFB300"}
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    matplotlib.rcParams.update({"font.size": 10})
    for i in range(len(slot_list)):
        handle = ax[i + 2].violinplot(vio[slot_list[i]], showmeans = False, showmedians = True)
        for j in range(len(handle["bodies"])):
            handle["bodies"][j].set_facecolor(c_dict[j])
            handle["bodies"][j].set_alpha(0.7)
        handle["cmedians"].set_color("k")
        handle["cmedians"].set_lw(1)
        handle["cbars"].set_color("k")
        handle["cbars"].set_lw(1)
        handle["cmaxes"].set_color("k")
        handle["cmaxes"].set_lw(1)
        handle["cmins"].set_color("k")
        handle["cmins"].set_lw(1)
        ax[i + 2].set_xticks([1, 2, 3])
        ax[i + 2].set_xticklabels(["Uber", "Lyft", "Taxi"])
        ax[i + 2].set_xlabel(str(slot_list[i]) + ":00 - " + str((slot_list[i] + 4) % 24) + ":00",
                         fontproperties = font)
        ax[i + 2].set_ylim([-0.2, np.log10(800000)])
        if i == 0:
            ax[i + 2].text(0.75, 0.82 * np.log10(800000), "(c)", fontproperties = font)
            ax[i + 2].set_yticks([0, np.log10(60), np.log10(3600), np.log10(86400 / 2)])
            ax[i + 2].set_yticklabels(["1s", "1m", "1h", "12h"])
            ax[i + 2].set_ylabel("Idle Time", fontproperties = font)
        else:
            ax[i + 2].set_yticks([])

    # CDF.
    idle_df = pd.read_csv(sf_idle_path)
    idle_df["idletime"] = idle_df["max(timestamp)"] - idle_df["min(timestamp)"]
    idle_df = idle_df[idle_df["idletime"] > 0]

    # Uber.
    uber_df = idle_df[idle_df["car_type"] == "UberX"].groupby("idletime").count()
    uber_df["idletime"] = uber_df.index
    uber_total = uber_df["car_id"].sum()
    uber_df["cdf"] = uber_df["idletime"].apply(lambda t: get_cdf(t, uber_df, uber_total))

    # Lyft.
    lyft_df = idle_df[idle_df["car_type"] == "Lyft"].groupby("idletime").count()
    lyft_df["idletime"] = lyft_df.index
    lyft_total = lyft_df["car_id"].sum()
    lyft_df["cdf"] = lyft_df["idletime"].apply(lambda t: get_cdf(t, lyft_df, lyft_total))

    # Taxi.
    taxi_df = idle_df[idle_df["car_type"] == "Taxi"].groupby("idletime").count()
    taxi_df["idletime"] = taxi_df.index
    taxi_total = taxi_df["car_id"].sum()
    taxi_df["cdf"] = taxi_df["idletime"].apply(lambda t: get_cdf(t, taxi_df, taxi_total))

    # Plot and save figure.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300"}
    ax[0].text(0.1, 0.82, "(a)", fontproperties = font)
    ax[0].set_xlim([0, np.log10(100000)])
    ax[0].set_ylim([0, 1.0])
    ax[0].set_yticks([0.0, 0.5, 1.0])
    ax[0].set_ylabel("CDF", fontproperties = font)
    ax[0].grid(True, zorder = 10)
    ax[0].set_xticks([0, np.log10(10), np.log10(60), np.log10(600), np.log10(3600), np.log10(86400 / 2)])
    ax[0].set_xticklabels(["1s", "10s", "1m", "10m", "1h", "12h"])
    ax[0].set_xlabel("Idle Time (SF)", fontproperties = font)
    ax[0].plot(np.log10(uber_df["idletime"]), uber_df["cdf"], color = c_dict["uber"], label = "Uber")
    ax[0].plot(np.log10(lyft_df["idletime"]), lyft_df["cdf"], color = c_dict["lyft"], label = "Lyft")
    ax[0].plot(np.log10(taxi_df["idletime"]), taxi_df["cdf"], color = c_dict["taxi"], label = "Taxi")
    ax[0].legend(loc = 4, frameon = False)

    # CDF.
    idle_df = pd.read_csv(nyc_idle_path)
    idle_df["idletime"] = idle_df["max(timestamp)"] - idle_df["min(timestamp)"]
    idle_df = idle_df[idle_df["idletime"] > 0]

    # Uber.
    uber_df = idle_df[idle_df["car_type"] == "UberX"].groupby("idletime").count()
    uber_df["idletime"] = uber_df.index
    uber_total = uber_df["car_id"].sum()
    uber_df["cdf"] = uber_df["idletime"].apply(lambda t: get_cdf(t, uber_df, uber_total))
    
    # Lyft.
    lyft_df = idle_df[idle_df["car_type"] == "Lyft"].groupby("idletime").count()
    lyft_df["idletime"] = lyft_df.index
    lyft_total = lyft_df["car_id"].sum()
    lyft_df["cdf"] = lyft_df["idletime"].apply(lambda t: get_cdf(t, lyft_df, lyft_total))
    
    # Plot and save figure.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300"}
    ax[1].text(0.1, 0.82, "(b)", fontproperties = font)
    ax[1].set_xlim([0, np.log10(100000)])
    ax[1].set_ylim([0, 1.0])
    ax[1].set_yticks([0, 0.5, 1])
    ax[1].set_yticklabels([])
    ax[1].grid(True, zorder = 10)
    ax[1].set_xticks([0, np.log10(10), np.log10(60), np.log10(600), np.log10(3600), np.log10(86400 / 2)])
    ax[1].set_xticklabels(["1s", "10s", "1m", "10m", "1h", "12h"])
    ax[1].set_xlabel("Idle Time (NYC)", fontproperties = font)
    ax[1].plot(np.log10(uber_df["idletime"]), uber_df["cdf"], color = c_dict["uber"], label = "Uber")
    ax[1].plot(np.log10(lyft_df["idletime"]), lyft_df["cdf"], color = c_dict["lyft"], label = "Lyft")
    ax[1].legend(loc = 4, frameon = False)

    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
