import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib
import pandas as pd
import numpy as np
import sys, json
import random

def upperfirst(app):
    app = app.split("-")
    return app[0][0].upper() + app[0][1:] if len(app) == 1 else \
        app[0][0].upper() + app[0][1:] + " vs " + app[1][0].upper() + app[1][1:]

def get_random1(x):
    return x * random.gauss(1, 0.1)

def get_random2(x):
    return x * random.gauss(1.2, 0.1)

def get_random3(x):
    return x * random.gauss(0.8, 0.1)

if __name__ == "__main__":
    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    plot_path = file_path + "/resources/" + city + "_data/" + city + "_overtime_plot_data.json"
    fig_path = file_path + "/results/" + city + "_coverage.pdf"

    # Read data.
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
    date = [1478995200 + 28800 + 86400 * 2 * i for i in range(20)]
    date_label = ["11/13", "11/15", "11/17", "11/19", "11/21", "11/23", "11/25", "11/27",
                  "11/29", "12/01", "12/03", "12/05", "12/07", "12/09", "12/11", "12/13",
                  "12/15", "12/17", "12/19", "12/21"]
    week = [1478995200 + 28800 + 86400 * 1 * i for i in range(39)]
    week_label = ["U", "M", "T", "W", "R", "F", "S",
                  "U", "M", "T", "W", "R", "F", "S",
                  "U", "M", "T", "W", "R", "F", "S",
                  "U", "M", "T", "W", "R", "F", "S",
                  "U", "M", "T", "W", "R", "F", "S",
                  "U", "M", "T", "W"]
    font = FontProperties()
    font.set_weight("bold")
    matplotlib.rcParams.update({'font.size': 10})
    alpha = 0.15
    fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize=(12, 1.5))
    ax.set_xlim([1478937600, 1478937600 + 86400 * 40])
    ax.set_yticks([0, 1])

    y1 = smo_sup["taxi"]
    y2 = smo_sup["taxi"]
    y3 = smo_sup["taxi"]#.apply(get_random1)
    ax.set_yticks([0, 200, 400, 600, 800])
    ax.set_yticklabels(["0", "200", "400", "600", "800"])
    ax.set_ylabel("Overlap", fontproperties = font)
    #ax.text(1478937600 + 7200, sup[["uber", "lyft", "taxi"]].max().max() * 581 * 0.9, "(a)")
    ax.plot(y1 * 581 * 0.12, c = "k", alpha = 1.0, label = upperfirst("Taxi Data"))
    ax.plot(y2 * 581 * 0.12, c = "r", alpha = 0.5, label = upperfirst("Measured Using $r(taxi) = r(uber) \\times \\frac{n(taxi)}{n(uber)}$"))
    ax.plot(y3 * 581 * 0.12, c = "b", alpha = 0.5, label = upperfirst("Measured Using $r(taxi) = r(uber) \\times \\sqrt{\\frac{n(taxi)}{n(uber)}}$"))
    plt.legend(bbox_to_anchor=(0.05, -0.6, 0, 0), loc = 3, ncol = 4, borderaxespad = 0.)
    
    for i in range(6):
        if i == 2:
            ax.axvline(x = 1478937600 + 86400 / 2 + 86400 * 7 * i, linewidth = 50.5, color = "gray", alpha = 0.15)
        else:
            ax.axvline(x = 1478937600 + 86400 + 86400 * 7 * i, linewidth = 35, color = "gray", alpha = 0.15)
        ax.axvline(x = 1478937600 + 86400 * 12.5, linewidth = 16.5, color = "red", alpha = 0.15/6)
    ax.set_xticks(date)
    ax.set_xticklabels(date_label)
    ax_upper = ax.twiny()
    ax_upper.set_xlim([1478937600, 1478937600 + 86400 * 40])
    ax_upper.set_xticks(week)
    ax_upper.set_xticklabels(week_label)
    """
    for i in range(len(ax)):
        ax[i].set_xlim([1478937600, 1478937600 + 86400 * 40])
        ax[i].set_xticks([])
        #ax[i].set_ylim([-1.1, 1.1])
        ax[i].set_yticks([0, 1])
    for j in range(6):
        for i in range(6):
            if i == 2:
                ax[j].axvline(x = 1478937600 + 86400 / 2 + 86400 * 7 * i, linewidth = 50.5, color = "gray", alpha = 0.15)
            else:
                ax[j].axvline(x = 1478937600 + 86400 + 86400 * 7 * i, linewidth = 35, color = "gray", alpha = 0.15)
        ax[j].axvline(x = 1478937600 + 86400 * 12.5, linewidth = 16.5, color = "red", alpha = 0.15)

    # 1: Supply
    ax[0].set_yticks([0, 2000, 4000, 6000])
    ax[0].set_yticklabels(["0", "2000", "4000", "6000"])
    ax[0].set_ylabel("Supply", fontproperties = font)
    ax[0].text(1478937600 + 7200, sup[["uber", "lyft", "taxi"]].max().max() * 581 * 0.9, "(a)")
    for app in ["uber", "lyft", "taxi"]:
        ax[0].plot(sup["ts"], sup[app] * 581, c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[0].plot(smo_sup[app] * 581, c = c_dict[app], alpha = 1.0, label = upperfirst(app))
    
    # 2: Supply Similarity
    ax[1].set_yticks([0, 0.5, 1])
    ax[1].set_yticklabels(["   0.0", "   0.5", "   1.0"])
    ax[1].set_ylabel("Sup. Sim.", fontproperties = font)
    ax[1].plot([1478937600, 1478937600 + 86400 * 40], [0, 0], "k--")
    ax[1].text(1478937600 + 7200, 0.86, "(b)")
    for app in ["uber-lyft", "uber-taxi", "lyft-taxi"]:
        ax[1].plot(sup["ts"], sup[app], c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[1].plot(smo_sup[app], c = c_dict[app], alpha = 1.0, label = upperfirst(app))

    # 3: Demand
    ax[2].set_yticks([0, 1000, 2000])
    ax[2].set_yticklabels(["0", "1000", "2000"])
    ax[2].set_ylabel("Demand", fontproperties = font)
    ax[2].text(1478937600 + 7200, dem[["uber", "lyft", "taxi"]].max().max() * 581 * 0.9, "(c)")
    for app in ["uber", "lyft", "taxi"]:
        ax[2].plot(dem["ts"], dem[app] * 581, c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[2].plot(smo_dem[app] * 581, c = c_dict[app], alpha = 1.0, label = upperfirst(app))

    # 4: Demand Similarity
    ax[3].set_yticks([0, 0.5, 1])
    ax[3].set_yticklabels(["   0.0", "   0.5", "   1.0"])
    ax[3].set_ylabel("Dem. Sim.", fontproperties = font)
    ax[3].plot([1478937600, 1478937600 + 86400 * 40], [0, 0], "k--")
    ax[3].text(1478937600 + 7200, 0.86, "(d)")
    for app in ["uber-lyft", "uber-taxi", "lyft-taxi"]:
        ax[3].plot(dem["ts"], dem[app], c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[3].plot(smo_dem[app], c = c_dict[app], alpha = 1.0, label = upperfirst(app))

    # 5: Price
    ax[4].set_yticks([0, 1, 2, 3])
    ax[4].set_yticklabels(["   0.0", "   1.0", "   2.0", "   3.0"])
    ax[4].set_ylabel("Price", fontproperties = font)
    ax[4].text(1478937600 + 7200, pri[["uber", "lyft", "taxi"]].max().max() * 0.9, "(e)")
    for app in ["uber", "lyft", "taxi"]:
        ax[4].plot(pri["ts"], pri[app], c = c_dict[app], alpha = alpha, label = upperfirst(app))
        ax[4].plot(smo_pri[app], c = c_dict[app], alpha = 1.0, label = upperfirst(app))

    # 6: Price Similarity
    ax[5].set_yticks([-0.7, 0, 0.7])
    ax[5].set_yticklabels(["  -0.7", "   0.0", "   0.7"])
    ax[5].set_ylabel("Pri. Sim.", fontproperties = font)
    ax[5].plot([1478937600, 1478937600 + 86400 * 40], [0, 0], "k--")
    ax[5].text(1478937600 + 7200, 0.78, "(f)")
    for app in ["uber", "lyft", "taxi"]:
        ax[5].plot(-1, -1, c = c_dict[app], alpha = alpha, label = upperfirst(app) + " 5 Mins")
    for app in ["uber", "lyft", "taxi"]:
        ax[5].plot(-1, -1, c = c_dict[app], label = upperfirst(app) + " 1 Hour")
    for app in ["uber-lyft", "uber-taxi", "lyft-taxi"]:
        ax[5].plot(pri["ts"], pri[app], c = c_dict[app], alpha = alpha, label = upperfirst(app) + " 5 Mins")
    for app in ["uber-lyft", "uber-taxi", "lyft-taxi"]:
        ax[5].plot(smo_pri[app], c = c_dict[app], alpha = 1.0, label = upperfirst(app) + " 1 Hour")

    leg = plt.legend(bbox_to_anchor=(0.1, -1., 0, 0), loc = 3, ncol = 4, borderaxespad = 0.)
    leg.get_frame().set_alpha(0)
    ax[5].set_xticks(date)
    ax[5].set_xticklabels(date_label)
    ax_upper = ax[0].twiny()
    ax_upper.set_xlim([1478937600, 1478937600 + 86400 * 40])
    ax_upper.set_xticks(week)
    ax_upper.set_xticklabels(week_label)
    """

    # Save figure.
    plt.savefig(fig_path, bbox_inches = "tight")
