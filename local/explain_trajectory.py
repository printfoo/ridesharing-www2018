import matplotlib.pyplot as plt
import matplotlib
from matplotlib.font_manager import FontProperties
import numpy as np
import sys
matplotlib.rcParams["pdf.fonttype"] = 42
matplotlib.rcParams["ps.fonttype"] = 42

if __name__ == "__main__":
    
    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    fig_path = file_path + "/results/trajectory_explain.pdf"

    # Plot.
    c_dict = {"uber1": "#1C6C67", "uber2": "#000000",
              "lyft": "#E70B81", "taxi": "#FFB300"}
    s = 5
    x = [5,  6,  7,  8,  9,  9.5,  10, 9.5,  9,  8,  7,  6,   5,  4,  3,  3.25, 3.5, 4.75, 6,7.25, 8.5, 9.75, 10, 9, 8, 6.5, 5]
    y = [17, 17.8, 18.5, 18.75, 17.5, 16.25, 15, 13.5, 12, 12, 12, 11.75, 11.5, 10.75, 10, 8.5,  7,   6.75, 6.5, 6.75, 7,   5.5,  4,  3, 2, 1,   0]
    y = [k * 0.9 for k in y]
    x1 = [5,  7,  9,    10, 9,  7,  5,    3,  3.5, 6,   8.5, 10, 8, 5]
    y1 = [17, 18.5, 17.5, 15, 12, 12, 11.5, 10, 7,   6.5, 7, 4, 2, 0]
    y1 = [k * 0.9 for k in y1]
    fig, ax = plt.subplots(nrows = 1, ncols = 3, figsize=(8, 1.5))
    font = FontProperties()
    font.set_weight("bold")
    font.set_size(10)
    matplotlib.rcParams.update({'font.size': 10})
    for an_ax in ax:
        an_ax.set_axis_off()
        an_ax.xaxis.set_major_locator(plt.NullLocator())
        an_ax.yaxis.set_major_locator(plt.NullLocator())
    
    ax[0].plot(x[:9], y[:9], color = c_dict["uber1"], linewidth = 1,
               marker = "o", markersize = s, label = "Uber ID-1 (Available)")
    ax[0].plot(x[13:21], y[13:21], color = c_dict["uber2"], linewidth = 1,
               marker = "^", markersize = s, label = "Uber ID-2 (Available)")
    ax[0].legend(loc = 3, bbox_to_anchor=(0, -0.5, 0, 0), frameon = False)
    ax[0].annotate("Uber Demand", xy = (x[8] - 0.2, y[8] + 0.2), xytext = (x[8] - 4, y[8] + 2),
        arrowprops = dict(arrowstyle='->', connectionstyle="arc3"))
    ax[0].text(2.9, 16.2, "(a)", fontproperties = font)

    ax[1].plot(x[:9], y[:9], color = c_dict["lyft"], linewidth = 1,
               marker = "o", markersize = s, label = "Lyft ID-1 (Available)")
    ax[1].plot([x[8], x[13]], [y[8], y[13]], color = c_dict["lyft"], linewidth = 1,
               marker = "o", markersize = s, linestyle = "--")
    ax[1].plot(x[13:21], y[13:21], color = c_dict["lyft"], linewidth = 1,
               marker = "o", markersize = s)
    ax[1].legend(loc = 3, bbox_to_anchor=(0, -0.5, 0, 0), frameon = False)
    ax[1].annotate("Lyft Demand", xy = (x[8] - 0.2, y[8] + 0.2), xytext = (x[8] - 4, y[8] + 2),
                   arrowprops = dict(arrowstyle='->', connectionstyle="arc3"))
    ax[1].annotate("Split (>60s)", xy = (x[12] + 1.5, y[12] - 0.5), xytext = (x[12] + 2, y[12] - 3),
                   arrowprops = dict(arrowstyle='->', connectionstyle="arc3"))
    ax[1].text(2.9, 16.2, "(b)", fontproperties = font)

    ax[2].plot(x1[:5], y1[:5], color = c_dict["taxi"], linewidth = 1,
               marker = "o", markersize = s, label = "Taxi ID-1 (Available)")
    ax[2].plot([x1[4], x1[5]], [y1[4], y1[5]], color = c_dict["taxi"], linewidth = 1, linestyle = "--")
    ax[2].plot(x1[5:7], y1[5:7], color = c_dict["taxi"], linewidth = 1,
               marker = "o", markersize = s, fillstyle = "none", linestyle = "--", label = "Taxi ID-1 (Occupied)")
    ax[2].plot([x1[6], x1[7]], [y1[6], y1[7]], color = c_dict["taxi"], linewidth = 1, linestyle = "--")
    ax[2].plot(x1[7:11], y1[7:11], color = c_dict["taxi"], linewidth = 1,
               marker = "o", markersize = s)
    ax[2].text(2.9, 16.2, "(c)", fontproperties = font)
    ax[2].legend(loc = 3, bbox_to_anchor=(0, -0.5, 0, 0), frameon = False)
    ax[2].annotate("Taxi Demand", xy = (x1[4] - 0.2, y1[4] + 0.2), xytext = (x1[4] - 4, y1[4] + 2),
                   arrowprops = dict(arrowstyle='->', connectionstyle="arc3"))
    ax[2].annotate("Split (Occupied)", xy = (x[12] + 1.5, y[12] - 0.5), xytext = (x[12] + 2, y[12] - 3),
                   arrowprops = dict(arrowstyle='->', connectionstyle="arc3"))
    
    # Save figure.
    plt.savefig(fig_path, bbox_inches = "tight", pad_inches = 0)
