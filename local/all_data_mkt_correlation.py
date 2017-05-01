from scipy.stats.stats import pearsonr
import pandas as pd
import sys

def out(x):
    x = x.split("_")
    return x[0][0].upper() + x[0][1:] + " " + x[1][0].upper() + x[1][1:] + "."

def checktime(time):
    time = int(time)
    if (time > 1478937600 + 86400 * 0.9 and time < 1478937600 + 86400 * 2.3) or \
       (time > 1478937600 + 86400 * 12.9  and time < 1478937600 + 86400 * 23.9):
        return False
    return True

if __name__ == "__main__":

    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    all_path = file_path + "/resources/" + city + "_data/" + city + "_all_data.csv"
    results_path = file_path + "/results/" + city + "_mkt_corr.txt"

    # Read and group data.
    data = pd.read_csv(all_path)
    data = data[data["timestamp"].apply(checktime)]
    app_list = ["uber", "lyft", "taxi"]
    fea_list = ["pri", "sup", "dem", "sur"]
    cor_list = [app + "_" + fea for app in app_list for fea in fea_list]
    f = open(results_path, "w")
    f.write(" & Uber & Uber & Uber & Uber & Lyft & Lyft & Lyft & Lyft & Taxi & Taxi & Taxi & Taxi \\\\ \n")
    f.write(" & Pri. & Sup. & Dem. & Sur. & Pri. & Sup. & Dem. & Sur. & Pri. & Sup. & Dem. & Sur. \\\\ \\midrule \n")
    f.write("Raw \\\\ \\midrule \n")

    # Instant correlation.
    df = data
    for xi in cor_list:
        f.write(out(xi))
        for xj in cor_list:
            if xi == xj or xi == "taxi_pri" or xj == "taxi_pri":
                f.write(" & ")
                continue
            d = df[[xi, xj]].dropna()
            d = d[d[xi] > 0]
            d = d[d[xj] > 0]
            corr, p = pearsonr(d[xi], d[xj])
            if p > 0.001:
                f.write(" & $" + "{0:.2f}".format(corr) + "$")
            else:
                f.write(" & $" + "{0:.2f}".format(corr) + "^*" + "$")
        f.write(" \\\\ \\midrule \n")

    f.write("by GeoID \\\\ \\midrule \n")

    # Average correlation.
    df = data.groupby("geo_id").mean()
    for xi in cor_list:
        f.write(out(xi))
        for xj in cor_list:
            if xi == xj or xi == "taxi_pri" or xj == "taxi_pri":
                f.write(" & ")
                continue
            d = df[[xi, xj]].dropna()
            d = d[d[xi] > 0]
            d = d[d[xj] > 0]
            corr, p = pearsonr(d[xi], d[xj])
            if p > 0.001:
                f.write(" & $" + "{0:.2f}".format(corr) + "$")
            else:
                f.write(" & $" + "{0:.2f}".format(corr) + "^*" + "$")
        f.write(" \\\\ \\midrule \n")

    f.write("by Time \\\\ \\midrule \n")

    # Average correlation.
    df = data.groupby("timestamp").mean()
    for xi in cor_list:
        f.write(out(xi))
        for xj in cor_list:
            if xi == xj or xi == "taxi_pri" or xj == "taxi_pri":
                f.write(" & ")
                continue
            d = df[[xi, xj]].dropna()
            d = d[d[xi] > 0]
            d = d[d[xj] > 0]
            corr, p = pearsonr(d[xi], d[xj])
            if p > 0.001:
                f.write(" & $" + "{0:.2f}".format(corr) + "$")
            else:
                f.write(" & $" + "{0:.2f}".format(corr) + "^*" + "$")
        f.write(" \\\\ \\midrule \n")
