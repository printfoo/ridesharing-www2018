from matplotlib.pyplot import *
from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import sys, json

def get_cdf(t, df, total):
    return (df[df["lifespan"] <= t]["car_id"].sum() + 0.0) / total

def to_time(x, pos):
    return str(x) + "s"

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    idle_path = file_path + "/resources/sf_data/sf_lifespan.csv"
    fig_path = file_path + "/results/lifespan_cdf.pdf"

    # Read file.
    idle_df = pd.read_csv(idle_path)
    idle_df["lifespan"] = idle_df["max(timestamp)"] - idle_df["min(timestamp)"]
    idle_df = idle_df[idle_df["lifespan"] > 0]

    # Uber.
    uber_df = idle_df[idle_df["car_type"] == "UberX"].groupby("lifespan").count()
    uber_df["lifespan"] = uber_df.index
    uber_total = uber_df["car_id"].sum()
    uber_df["cdf"] = uber_df["lifespan"].apply(lambda t: get_cdf(t, uber_df, uber_total))

    # Lyft.
    lyft_df = idle_df[idle_df["car_type"] == "Lyft"].groupby("lifespan").count()
    lyft_df["lifespan"] = lyft_df.index
    lyft_total = lyft_df["car_id"].sum()
    lyft_df["cdf"] = lyft_df["lifespan"].apply(lambda t: get_cdf(t, lyft_df, lyft_total))

    # Taxi.
    taxi_df = idle_df[idle_df["car_type"] == "Taxi"].groupby("lifespan").count()
    taxi_df["lifespan"] = taxi_df.index
    taxi_total = taxi_df["car_id"].sum()
    taxi_df["cdf"] = taxi_df["lifespan"].apply(lambda t: get_cdf(t, taxi_df, taxi_total))

    # Plot and save figure.
    c_dict = {"uber": "#1C6C67", "lyft": "#E70B81", "taxi": "#FFB300"}
    font = FontProperties()
    font.set_weight("bold")
    matplotlib.rcParams.update({"font.size": 10})
    fig, ax = plt.subplots(nrows = 1, ncols = 1, figsize=(6, 1.5))
    ax.set_xlim([0, np.log10(50000000)])
    ax.set_ylim([0, 1.0])
    ax.set_yticks([0.0, 0.5, 1.0])
    ax.set_ylabel("CDF", fontproperties = font)
    ax.set_xticks([0, np.log10(10), np.log10(60), np.log10(600), np.log10(3600), np.log10(86400), np.log10(86400 * 30)])
    ax.set_xticklabels(["1s", "10s", "1m", "10m", "1h", "1d", "30d"])
    ax.set_xlabel("Lifespan", fontproperties = font)
    ax.plot(np.log10(uber_df["lifespan"]), uber_df["cdf"], color = c_dict["uber"], label = "Uber")
    ax.plot(np.log10(lyft_df["lifespan"]), lyft_df["cdf"], color = c_dict["lyft"], label = "Lyft")
    ax.plot(np.log10(taxi_df["lifespan"]), taxi_df["cdf"], color = c_dict["taxi"], label = "Taxi")
    ax.grid(True, zorder = 10)
    print uber_df["cdf"][uber_df["cdf"] < 0.5]
    print lyft_df["cdf"][lyft_df["cdf"] < 0.5]
    print taxi_df["cdf"][taxi_df["cdf"] < 0.5]
    ax.legend(loc = 2, frameon = False)
    plt.savefig(fig_path, bbox_inches = "tight")
