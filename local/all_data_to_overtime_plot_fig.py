import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib
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

def plot_sf(subp = 0):
    
    # Read data.
    plot_path = file_path + "/resources/sf_data/sf_overtime_plot_data.json"
    with open(plot_path, "r") as plot_file:
        data = json.loads(plot_file.read().strip("\n"))
    smo_inter = 2 * 60 * 60
    
    # Get supply curve data
    sup = pd.DataFrame.from_dict(data["sup"])
    sup["ts"] = data["index"]
    sup["round_ts"] = sup["ts"] - sup["ts"] % smo_inter
    smo_sup = sup.groupby("round_ts").mean()
    
    # Get demand curve data
    dem = pd.DataFrame.from_dict(data["dem"])
    dem["ts"] = data["index"]
    dem["round_ts"] = dem["ts"] - dem["ts"] % smo_inter
    smo_dem = dem.groupby("round_ts").mean()
    
    # Get price curve data
    pri = pd.DataFrame.from_dict(data["pri"])
    pri["ts"] = data["index"]
    pri["round_ts"] = pri["ts"] - pri["ts"] % smo_inter
    smo_pri = pri.groupby("round_ts").mean()
    
    # Plot parameter and setting.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300",
        "uber-lyft": "#823C74", "uber-taxi": "#8E9034", "lyft-taxi": "#F35F41"}
    date = [1481702400 + 86400 * 1 * i for i in range(6)]
    date_label = ["12/11", "12/12", "12/13", "12/14", "12/15", "12/16"]
    week = [1481702400 + 86400 * 1 * i for i in range(6)]
    week_label = ["W", "R", "F", "S", "U", "M"]
    for i in range(len(ax)):
        ax[i, subp].set_xlim([1481702400, 1481702400 + 86400 * 6])
        ax[i, subp].set_xticks([])
        ax[i, subp].set_yticks([0, 1])
        ax[i, subp].axvline(x = 1481702400 + 86400 * 4, linewidth = 68, color = "gray", alpha = 0.15)

    # 1: Supply
    maxy = 5000
    ax[0, subp].set_ylim([0, maxy * 1.23])
    ax[0, subp].set_yticks([0, 2500, 5000])
    ax[0, subp].set_yticklabels(["0", "2500", "5000"])
    ax[0, subp].set_ylabel("Supply", fontproperties = font)
    ax[0, subp].text(1481702400 + 7200, maxy, "(a)", fontproperties = font)
    for app in ["uber", "lyft", "taxi"]:
        ax[0, subp].plot(sup["ts"], sup[app] * 581, c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[0, subp].plot(smo_sup[app] * 581, c = c_dict[app], alpha = 1.0, label = upperfirst(app))
    
    # 3: Demand
    maxy = 2000
    ax[1, subp].set_ylim([0, maxy * 1.23])
    ax[1, subp].set_yticks([0, 1000, 2000])
    ax[1, subp].set_yticklabels(["0", "1000", "2000"])
    ax[1, subp].set_ylabel("Demand", fontproperties = font)
    ax[1, subp].text(1481702400 + 7200, maxy, "(c)", fontproperties = font)
    for app in ["uber", "lyft", "taxi"]:
        ax[1, subp].plot(dem["ts"], dem[app] * 581, c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[1, subp].plot(smo_dem[app] * 581, c = c_dict[app], alpha = 1.0, label = upperfirst(app))
    
    # 5: Price
    maxy = 3
    ax[2, subp].set_ylim([1, maxy * 1.23])
    ax[2, subp].set_yticks([1, 2, 3])
    ax[2, subp].set_yticklabels(["      1", "2", "3"])
    ax[2, subp].set_ylabel("Price", fontproperties = font)
    ax[2, subp].set_xlabel("SF (2016)", fontproperties = font)
    ax[2, subp].text(1481702400 + 7200, maxy * 1.065, "(e)", fontproperties = font)
    
    ax[2, subp].scatter(-5, -5, c = c_dict["uber"], alpha = 1.0, s = 15, marker = "^", label = "Uber Anomaly")
    ax[2, subp].scatter(-5, -5, c = c_dict["lyft"], alpha = 1.0, s = 15, marker = "^", label = "Lyft Anomaly")
    
    for app in ["uber", "lyft", "taxi"]:
        ax[2, subp].plot(pri["ts"], pri[app], c = c_dict[app], alpha = alpha, label = upperfirst(app) + " 5-Min")
        ax[2, subp].plot(smo_pri[app], c = c_dict[app], alpha = 1.0, label = upperfirst(app) + " 2-Hour")


    leg = ax[2, subp].legend(bbox_to_anchor=(0.0, -1.25, 0, 0), loc = 3, ncol = 4, borderaxespad = 0.)
    leg.get_frame().set_alpha(0)
    ax[2, subp].set_xticks(date)
    ax[2, subp].set_xticklabels(date_label)
    ax_upper = ax[0, subp].twiny()
    ax_upper.set_xlim([1481702400, 1481702400 + 86400 * 6])
    ax_upper.set_xticks(week)
    ax_upper.set_xticklabels(week_label)

def plot_nyc(subp = 1):
    
    # Read data.
    plot_path = file_path + "/resources/nyc_data/nyc_overtime_plot_data.json"
    with open(plot_path, "r") as plot_file:
        data = json.loads(plot_file.read().strip("\n"))
    
    # Get supply curve data
    sup = pd.DataFrame.from_dict(data["sup"])
    sup["ts"] = data["index"]
    sup["round_ts"] = sup["ts"] - sup["ts"] % smo_inter
    smo_sup = sup.groupby("round_ts").mean()
    
    # Get demand curve data
    dem = pd.DataFrame.from_dict(data["dem"])
    dem["ts"] = data["index"]
    dem["round_ts"] = dem["ts"] - dem["ts"] % smo_inter
    smo_dem = dem.groupby("round_ts").mean()
    
    # Get price curve data
    pri = pd.DataFrame.from_dict(data["pri"])
    pri["ts"] = data["index"]
    pri["round_ts"] = pri["ts"] - pri["ts"] % smo_inter
    smo_pri = pri.groupby("round_ts").mean()
    
    # Plot parameter and setting.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300",
        "uber-lyft": "#823C74", "uber-taxi": "#8E9034", "lyft-taxi": "#F35F41"}
    date = [1485925200 + 86400 * 1 * i for i in range(6)]
    date_label = ["02/01", "02/02", "02/03", "02/04", "02/05", "02/06"]
    week = [1485925200 + 86400 * 1 * i for i in range(6)]
    week_label = ["W", "R", "F", "S", "U", "M"]
    for i in range(len(ax)):
        ax[i, subp].set_xlim([1485925200, 1485925200 + 86400 * 6])
        ax[i, subp].set_xticks([])
        ax[i, subp].axvline(x = 1485925200 + 86400 * 4, linewidth = 68, color = "gray", alpha = 0.15)
    sp = 1486352100
    
    # 1: Supply
    maxy = 20000
    ax[0, subp].set_ylim([0, maxy * 1.23])
    ax[0, subp].set_yticks([0, 10000, 20000])
    ax[0, subp].set_yticklabels(["0", "10000", "20000"])
    ax[0, subp].text(1485925200 + 7200, maxy, "(b)", fontproperties = font)
    for app in ["uber", "lyft"]:
        ax[0, subp].plot(sup["ts"], sup[app] * 3818, c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[0, subp].scatter(sp, sup[sup["ts"] == sp][app] * 3818, c = c_dict[app], alpha = 1.0, s = 15, marker = "^")
        ax[0, subp].plot(smo_sup[app] * 3818, c = c_dict[app], alpha = 1.0, label = upperfirst(app))
    
    # 3: Demand
    maxy = 3000
    ax[1, subp].set_ylim([0, maxy * 1.23])
    ax[1, subp].set_yticks([0, 1500, 3000])
    ax[1, subp].set_yticklabels(["0", "1500", "3000"])
    ax[1, subp].text(1485925200 + 7200, maxy, "(d)", fontproperties = font)
    for app in ["uber", "lyft"]:
        ax[1, subp].plot(dem["ts"], dem[app] * 3818, c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[1, subp].scatter(sp, dem[dem["ts"] == sp][app] * 3818, c = c_dict[app], alpha = 1.0, s = 15, marker = "^")
        ax[1, subp].plot(smo_dem[app] * 3818, c = c_dict[app], alpha = 1.0, label = upperfirst(app))
    
    # 5: Price
    maxy = 3
    ax[2, subp].set_ylim([1, maxy * 1.23])
    ax[2, subp].set_yticks([1, 2, 3])
    ax[2, subp].set_yticklabels(["      1", "2", "3"])
    ax[2, subp].set_xlabel("NYC (2017)", fontproperties = font)
    ax[2, subp].text(1485925200 + 7200, maxy * 1.065, "(f)", fontproperties = font)
    for app in ["uber", "lyft"]:
        ax[2, subp].plot(pri["ts"], pri[app], c = c_dict[app], alpha = alpha, label = None)
        ax[2, subp].scatter(sp, pri[pri["ts"] == sp][app], c = c_dict[app], alpha = 1.0, s = 15, marker = "^", label = upperfirst(app) + "Anomaly")
        ax[2, subp].plot(smo_pri[app], c = c_dict[app], alpha = 1.0, label = None)

    ax[2, subp].set_xticks(date)
    ax[2, subp].set_xticklabels(date_label)
    ax_upper = ax[0, subp].twiny()
    ax_upper.set_xlim([1485925200, 1485925200 + 86400 * 6])
    ax_upper.set_xticks(week)
    ax_upper.set_xticklabels(week_label)


if __name__ == "__main__":
    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    fig_path = file_path + "/results/change_overtime.pdf"
    smo_inter = 2 * 60 * 60
    
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    matplotlib.rcParams.update({'font.size': 10})
    alpha = 0.15
    fig, ax = plt.subplots(nrows = 3, ncols = 2, figsize=(8, 3.5))

    plot_sf(0)
    plot_nyc(1)
    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
