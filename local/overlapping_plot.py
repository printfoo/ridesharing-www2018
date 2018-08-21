import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib
import pandas as pd
import numpy as np
import sys, json, random
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42

def upperfirst(app):
    app = app.split("-")
    return app[0][0].upper() + app[0][1:] if len(app) == 1 else \
        app[0][0].upper() + app[0][1:] + " vs " + app[1][0].upper() + app[1][1:]

def plot_sf(subp = 0):
    
    # Read data.
    plot_path1 = file_path + "/resources/sf_data/sf_overlap_2_1p0.txt"
    plot_path2 = file_path + "/resources/sf_data/sf_overlap_2_0p9.txt"
    plot_path3 = file_path + "/resources/sf_data/sf_overlap_2_0p8.txt"
    datas = []
    smo_datas = []
    datas.append(pd.read_csv(plot_path1))
    datas.append(pd.read_csv(plot_path2))
    datas.append(pd.read_csv(plot_path3))
    smo_inter = 2 * 60 * 60
    for data in datas:
        data["ts"] = data["time"]
        data["round_ts"] = data["ts"] - data["ts"] % smo_inter
        data["r"] = data["overlap"] / (data["lyft"] + data["uber"])
        print (data["r"].mean(), data["r"].max(), data["r"].min(), data["r"].std())
        smo = data.groupby("round_ts").mean()
        smo_datas.append(smo)
    
    # Plot parameter and setting.
    date = [1481702400 + 86400 * 1 * i for i in range(6)]
    date_label = ["12/11", "12/12", "12/13", "12/14", "12/15", "12/16"]
    week = [1481702400 + 86400 * 1 * i for i in range(6)]
    week_label = ["W", "R", "F", "S", "U", "M"]
    ax[subp].set_xlim([1481702400, 1481702400 + 86400 * 6])
    ax[subp].set_xticks([])
    ax[subp].set_yticks([0, 1])
    ax[subp].axvline(x = 1481702400 + 86400 * 4, linewidth = 68, color = "gray", alpha = 0.15)
    
    # 1: overlap
    ax[subp].set_ylim([0, 0.1])
    ax[subp].set_yticks([0, 0.05, 0.1])
    ax[subp].set_yticklabels(["0%", "5%", "10%"])
    ax[subp].set_ylabel("Shared Drivers", fontproperties = font)
    ax[subp].set_xlabel("SF (2016)", fontproperties = font)
    ax[subp].text(1481702400 + 7200, 0.08, "(a)", fontproperties = font)
    for data, smo, ls, c, label in zip(datas, smo_datas, ls_list, c_list, label_list):
        ax[subp].plot(data["ts"], data["r"], c = c, alpha = 0.15, label = label + " 5-Min")
        ax[subp].plot(smo["r"], c = c, alpha = 1.0, label = label + " 2-Hour")

    leg = ax[subp].legend(bbox_to_anchor=(0.13, -1.2, 0, 0), loc = 3, ncol = 3, borderaxespad = 0.)
    leg.get_frame().set_alpha(0)
    ax[subp].set_xticks(date)
    ax[subp].set_xticklabels(date_label)
    ax_upper = ax[subp].twiny()
    ax_upper.set_xlim([1481702400, 1481702400 + 86400 * 6])
    ax_upper.set_xticks(week)
    ax_upper.set_xticklabels(week_label)

def plot_nyc(subp = 1):
    
    # Read data.
    plot_path1 = file_path + "/resources/nyc_data/nyc_overlap_2_1p0.txt"
    plot_path2 = file_path + "/resources/nyc_data/nyc_overlap_2_0p9.txt"
    plot_path3 = file_path + "/resources/nyc_data/nyc_overlap_2_0p8.txt"
    datas = []
    smo_datas = []
    datas.append(pd.read_csv(plot_path1))
    datas.append(pd.read_csv(plot_path2))
    datas.append(pd.read_csv(plot_path3))
    smo_inter = 2 * 60 * 60
    for data in datas:
        data["ts"] = data["time"]
        data["round_ts"] = data["ts"] - data["ts"] % smo_inter
        data["r"] = data["overlap"] / (data["lyft"] + data["uber"])
        print (data["r"].mean(), data["r"].max(), data["r"].min(), data["r"].std())
        smo = data.groupby("round_ts").mean()
        smo_datas.append(smo)
    
    # Plot parameter and setting.
    date = [1485925200 + 86400 * 1 * i for i in range(6)]
    date_label = ["02/01", "02/02", "02/03", "02/04", "02/05", "02/06"]
    week = [1485925200 + 86400 * 1 * i for i in range(6)]
    week_label = ["W", "R", "F", "S", "U", "M"]
    ax[subp].set_xlim([1485925200, 1485925200 + 86400 * 6])
    ax[subp].set_xticks([])
    ax[subp].set_yticks([0, 1])
    ax[subp].axvline(x = 1485925200 + 86400 * 4, linewidth = 68, color = "gray", alpha = 0.15)
    
    # 1: overlap
    ax[subp].set_ylim([0, 0.1])
    ax[subp].set_yticks([])
    ax[subp].set_xlabel("NYC (2017)", fontproperties = font)
    ax[subp].text(1485925200 + 7200, 0.08, "(b)", fontproperties = font)
    for data, smo, ls, c, label in zip(datas, smo_datas, ls_list, c_list, label_list):
        ax[subp].plot(data["ts"], data["r"], c = c, alpha = 0.15, label = label + " 5-Min")
        ax[subp].plot(smo["r"], c = c, alpha = 1.0, label = label + " 2-Hour")

    ax[subp].set_xticks(date)
    ax[subp].set_xticklabels(date_label)
    ax_upper = ax[subp].twiny()
    ax_upper.set_xlim([1485925200, 1485925200 + 86400 * 6])
    ax_upper.set_xticks(week)
    ax_upper.set_xticklabels(week_label)


if __name__ == "__main__":
    random.seed(3)
    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    fig_path = file_path + "/results/change_overlap.pdf"
    smo_inter = 2 * 60 * 60
    
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    matplotlib.rcParams.update({'font.size': 10})
    alpha = 0.15
    c_list = ["k", "#1C6C67", "#E70B81"]
    ls_list = ["-", "--", ":"]
    label_list = ["100% Sim.", "90% Sim.", "80% Sim."]
    fig, ax = plt.subplots(nrows = 1, ncols = 2, figsize=(8, 1.08))
    
    plot_sf(0)
    plot_nyc(1)
    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
