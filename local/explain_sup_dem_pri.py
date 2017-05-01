import matplotlib.pyplot as plt
import matplotlib
from matplotlib.font_manager import FontProperties
import sys

if __name__ == "__main__":
    
    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    fig_path = file_path + "/results/sup_dem_explain.pdf"

    # Plot.
    font = FontProperties()
    font.set_weight("bold")
    matplotlib.rcParams.update({"font.size": 10})
    fig, ax = plt.subplots(nrows = 1, ncols = 2, figsize=(6, 2.7))
    for i in range(len(ax)):
        ax[i].plot([-0.2, 1.2], [0.75, 0.75], "k--")
        ax[i].set_xlim([-0.2, 1.2])
        ax[i].set_xticks([])
        ax[i].set_xlabel("Quantity", fontproperties = font)
        ax[i].set_ylim([-0.2, 1.2])
        ax[i].text(0.85, 1.02, "Supply", color = "g", alpha = 0.8)
        ax[i].text(0.78, -0.07, "Demand", color = "r", alpha = 0.8)
        ax[i].plot([0, 1], [0, 1], "g")
    ax[0].set_ylabel("Price", fontproperties = font)
    ax[0].plot([0, 1], [1, 0], "r")
    ax[0].plot([0.25, 0.75], [0.75, 0.75], "k", linewidth = 4, alpha = 0.4)
    ax[0].set_yticks([0.75])
    ax[0].set_yticklabels(["$p$"])
    ax[0].annotate("  Safe\nSurplus", xy=(0.27, 0.77), xytext=(0.35, 0.9),
        arrowprops=dict(arrowstyle='->', connectionstyle="arc3"), fontsize = 10)
    ax[0].annotate("      \n       ", xy=(0.73, 0.77), xytext=(0.62, 0.87),
        arrowprops=dict(arrowstyle='->', connectionstyle="arc3"), fontsize = 10)
    ax[1].plot([0, 1], [1, 0], "r--", alpha = 0.8)
    ax[1].plot([0.1, 1.1], [1.1, 0.1], "r", alpha = 0.8)
    ax[1].plot([-0.2, 1.2], [0.85, 0.85], "k--")
    ax[1].plot([0.35, 0.85], [0.85, 0.85], "k", linewidth = 4, alpha = 0.4)
    ax[1].plot([0.45, 0.75], [0.75, 0.75], "k", linewidth = 4, alpha = 0.4)
    ax[1].set_yticks([0.75, 0.85])
    ax[1].set_yticklabels(["$p$", "$p'$"])
    ax[1].text(0.4, 0.9, "   Price\nIncreases")
    ax[1].text(0.27, 0.12, " Demand\nIncreases", color = "r")
    ax[1].annotate("", xy=(0.6, 0.86), xytext=(0.6, 0.75),
        arrowprops=dict(arrowstyle='->', connectionstyle="arc3"))
    ax[1].annotate("", xy=(0.72, 0.52), xytext=(0.59, 0.39),
        arrowprops=dict(arrowstyle='->', connectionstyle="arc3", color = "r"))
        
    # Show figure.
    plt.savefig(fig_path, bbox_inches = "tight")
