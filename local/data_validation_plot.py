import matplotlib.pyplot as plt
import matplotlib
from matplotlib.font_manager import FontProperties
from matplotlib.gridspec import GridSpec
from shapely.geometry import shape, Point
from scipy.stats import ks_2samp
import numpy as np
import pandas as pd
import os, sys, json, random, time
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42
matplotlib.rcParams["font.size"] = 10

if __name__ == "__main__":
    
    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    vali_path = file_path + "/resources/nyc_data/nyc_validation.csv"
    sim_uber_path = file_path + "/results/mc_sim_UberX.csv"
    sim_lyft_path = file_path + "/results/mc_sim_Lyft.csv"
    uber_k_path = file_path + "/results/UberX_k_values.csv"
    lyft_k_path = file_path + "/results/Lyft_k_values.csv"
    fig_path = file_path + "/results/validation.pdf"
    
    """
    vali = pd.read_csv(vali_path)
    uber_points = vali[vali["car_type"] == "UberX"]
    uber_measure_points = uber_points[uber_points["data_type"] == "measure"].sample(n = 2000)
    uber_truth_points = uber_points[uber_points["data_type"] == "truth"].sample(n = 2000)
    lyft_points = vali[vali["car_type"] == "Lyft"]
    lyft_measure_points = lyft_points[lyft_points["data_type"] == "measure"].sample(n = 2000)
    lyft_truth_points = lyft_points[lyft_points["data_type"] == "truth"].sample(n = 2000)
    """
    
    n = 1000
    sim_uber = pd.read_csv(sim_uber_path)
    sim_uber = sim_uber[sim_uber["k_value"] > 0.5]
    print sim_uber
    uber_list_sep = sim_uber[sim_uber["exp_no"] == 1]["k_value"][:n]
    uber_list_com = sim_uber[sim_uber["exp_no"] == 2]["k_value"][:n]
    uber_mean = uber_list_sep.mean()
    n1 = len(uber_list_com[uber_list_com < uber_mean])
    n2 = len(uber_list_com[uber_list_com > 2 * uber_list_com.mean() - uber_mean])
    p = (n1 + n2 + 0.0) / len(uber_list_com)
    print p
    
    sim_lyft = pd.read_csv(sim_lyft_path)
    sim_lyft = sim_lyft[sim_lyft["k_value"] > 0.5]
    lyft_list_sep = sim_lyft[sim_lyft["exp_no"] == 1]["k_value"][:n]
    lyft_list_com = sim_lyft[sim_lyft["exp_no"] == 2]["k_value"][:n]
    lyft_mean = lyft_list_sep.mean()
    n1 = len(lyft_list_com[lyft_list_com < lyft_mean])
    n2 = len(lyft_list_com[lyft_list_com > 2 * lyft_list_com.mean() - lyft_mean])
    p = (n1 + n2 + 0.0) / len(lyft_list_com)
    print p
    
    uber_k_df = pd.read_csv(uber_k_path)
    lyft_k_df = pd.read_csv(lyft_k_path)

    # Plot.
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    a1 = 0.2
    a2 = 0.3
    s = 5
    bins = 14
    fig, ax = plt.subplots(nrows = 1, ncols = 3, figsize=(8, 1.6))
    gs = GridSpec(1, 17)
    ax = [None for i in range(13)]
    ax[0] = plt.subplot(gs[0, :4])
    ax[1] = plt.subplot(gs[0, 4:8])
    ax[2] = plt.subplot(gs[0, 9:13])
    ax[3] = plt.subplot(gs[0, 13:17])
    
    ax[0].plot(uber_k_df["t"], uber_k_df["self"], c = "#1C6C67", alpha = 0.5, label = "vs Ground-Truth", linestyle = "--")
    ax[0].plot(uber_k_df["t"], uber_k_df["measure"], c = "#1C6C67", alpha = 1.0, label = "vs Measured")
    ax[0].plot(uber_k_df["t"], uber_k_df["random"], c = "k", alpha = 0.5, label = "vs Random", linestyle = "--")
    ax[0].legend(loc = 4, frameon = False, fontsize = 7)
    ax[0].set_xlabel("Search Radius $t$", fontproperties = font)
    ax[0].set_ylabel("$K$ Value", fontproperties = font)
    ax[0].set_title("(a)            Uber               ", fontproperties = font)
    ax[0].set_ylim([0, 1.1])
    ax[0].set_yticks([0, 0.5, 1])
    ax[0].set_xlim([0, 0.19])
    ax[0].set_xticks([0, 0.05, 0.1, 0.15])
    ax[0].set_xticklabels(["0", "0.05", "0.1", "0.15"])
    ax[0].grid(True, zorder = 10)
    
    ax[1].plot(lyft_k_df["t"], lyft_k_df["self"], c = "#E70B81", alpha = 0.5, label = "vs Ground-Truth", linestyle = "--")
    ax[1].plot(lyft_k_df["t"], lyft_k_df["measure"], c = "#E70B81", alpha = 1.0, label = "vs Measured")
    ax[1].plot(lyft_k_df["t"], lyft_k_df["random"], c = "k", alpha = 0.5, label = "vs Random", linestyle = "--")
    ax[1].legend(loc = 4, frameon = False, fontsize = 7)
    ax[1].set_xlabel("Search Radius $t$", fontproperties = font)
    ax[1].set_title("Lyft", fontproperties = font)
    ax[1].set_ylim([0, 1.1])
    ax[1].set_yticks([0, 0.5, 1])
    ax[1].set_yticklabels([])
    ax[1].set_xlim([0, 0.19])
    ax[1].set_xticks([0, 0.05, 0.1, 0.15])
    ax[1].set_xticklabels(["0", "0.05", "0.1", "0.15"])
    ax[1].grid(True, zorder = 10)

    ax[2].hist(uber_list_sep, bins, color = "#1C6C67", alpha = a2, label = "$K_{P^*_m P^*_g}$")
    ax[2].hist(uber_list_com, bins, color = "#000000", alpha = a2, label = "$K_{P_i P_j}$")
    ax[2].axvline(uber_mean, color = "#1C6C67", linestyle = "--", linewidth = 2, label = "$K_{P_m P_g}$")
    ax[2].legend(loc = 2, frameon = False, fontsize = 7)
    ax[2].set_ylim([0, 250])
    ax[2].set_yticklabels([0, 0.04, 0.08, 0.12, 0.16])
    ax[2].set_xlabel("$K$ Value ($t=0.05$)", fontproperties = font)
    ax[2].set_ylabel("Frequency", fontproperties = font)
    ax[2].set_title("(b)           Uber                ", fontproperties = font)
    ax[2].set_xticks([uber_mean - 0.018, uber_mean, uber_mean + 0.018])
    print uber_mean
    ax[2].set_xticklabels(["0.73", "0.748", "0.766"])
    ax[2].set_yticks([])

    ax[3].hist(lyft_list_sep, bins, color = "#E70B81", alpha = a2, label = "$K_{P^*_m P^*_g}$")
    ax[3].hist(lyft_list_com, bins, color = "#000000", alpha = a2, label = "$K_{P_i P_j}$")
    ax[3].axvline(lyft_mean, color = "#E70B81", linestyle = "--", linewidth = 2, label = "$K_{P_m P_g}$")
    ax[3].legend(loc = 2, frameon = False, fontsize = 7)
    ax[3].set_ylim([0, 195])
    ax[3].set_xlabel("$K$ Value ($t=0.05$)", fontproperties = font)
    ax[3].set_title("Lyft", fontproperties = font)
    ax[3].set_xticks([lyft_mean - 0.018, lyft_mean, lyft_mean + 0.018])
    print lyft_mean
    ax[3].set_xticklabels(["0.705", "0.723", "0.741"])
    ax[3].set_yticks([])
    
    """
    for i in range(4):
        ax[i].set_xticks([])
        ax[i].set_yticks([])
    """
        
    # Show figure.
    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
