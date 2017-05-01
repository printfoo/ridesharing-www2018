from scipy.stats.stats import pearsonr
from scipy import stats
import statsmodels.api as sm
#import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd
import numpy as np
import json, re, os, sys, math, random

def out(x):
    x = x.split("_")
    if len(x) == 1:
        return x[0][0].upper() + x[0][1:3] + "."
    else:
        return x[0][0].upper() + x[0][1:3] + ". " + x[1][0].upper() + x[1][1:3] + "."

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    map_path = file_path + "/resources/sf_block_groups/sf_bg_with_data_acs15.geojson"
    all_data_path = file_path + "/resources/" + city + "_data/" + city + "_group_geo.csv"
    results_path = file_path + "/results/" + city + "_city_corr.txt"

    # Read data.
    map_df = gpd.read_file(map_path)
    data_df = pd.read_csv(all_data_path)
    df = map_df.merge(data_df, on = "geo_id", how = "left")

    # Compute correlation.
    city_box = ["population", "white_ratio", "black_ratio", "asian_ratio", "nati_hawaii_ratio", "amer_indian_ratio", "more_ratio", "hispanic_ratio",
                "income", "house_value", "education", "family_ratio",
                "bus_stop", "parking_meter", "parking_off_street", "time_to_work",
                "by_car_ratio", "by_pub_ratio", "by_taxi_ratio", "by_moto_ratio", "by_bike_ratio", "on_feet_ratio", "at_home_ratio"]
    vfh_box = ["uber_pri", "uber_sup", "uber_dem", "uber_sur", "lyft_pri", "lyft_sup", "lyft_dem", "lyft_sur", "taxi_pri", "taxi_sup", "taxi_dem", "taxi_sur"]
    """
    f = open(results_path, "w")
    f.write(" & Uber & Uber & Uber & Uber & Lyft & Lyft & Lyft & Lyft & Taxi & Taxi & Taxi & Taxi \\\\ \n")
    f.write(" & Pri. & Sup. & Dem. & Sur. & Pri. & Sup. & Dem. & Sur. & Pri. & Sup. & Dem. & Sur. \\\\ \\midrule \n")
    for fea in city_box:
        f.write(out(fea))
        for vfh in vfh_box:
            # Select data.
            a_df = df[[fea, vfh]].dropna()
            x = (a_df[fea] - a_df[fea].mean()) / a_df[fea].std()
            y = (a_df[vfh] - a_df[vfh].mean()) / a_df[vfh].std()
            # OLS
            reg = sm.OLS(y, sm.add_constant(x)).fit()
            const = reg.params[0]
            coef = reg.params[1]
            p = reg.pvalues[1]
            if p > 0.1:
                f.write(" & $" + "{0:.2f}".format(coef) + "$")
            else:
                f.write(" & $" + "{0:.2f}".format(coef) + "^*" + "$")
        f.write(" \\\\ \\midrule \n")
    """

    for xi in city_box:
        for xj in city_box:
            if xi == xj:
                continue
            d = df[[xi, xj]].dropna()
            corr, p = pearsonr(d[xi], d[xj])
            print xi, xj, corr

    # Multi reg.
    table_dict = {}
    for fea in city_box:
        table_dict[fea] = []
    table_dict["r2"] = []
    table_dict["intercept"] = []
    table_dict["sample_size"] = []
    for vfh in vfh_box:
        fea = city_box[:]
        fea.append(vfh)
        a_df = df[fea].dropna()
        y = ((a_df[vfh] - a_df[vfh].mean()) / a_df[vfh].std()).values
        x = ((a_df[city_box] - a_df[city_box].mean()) / a_df[city_box].std()).values
        x = sm.add_constant(x, prepend = False)
        # OLS
        reg = sm.OLS(y, x).fit()
        print reg.model.exog
        print np.linalg.cond(reg.model.exog)
        coef = reg.params[:]
        p = reg.pvalues[:]
        r = reg.rsquared
        for i in range(len(p) - 1):
            table_dict[city_box[i]].append({"coef": coef[i], "p": p[i]})
        table_dict["intercept"].append({"coef": coef[i + 1], "p": p[i + 1]})
        table_dict["r2"].append(r)
        table_dict["sample_size"].append(len(df[fea]))

    # Write.
    f = open(results_path, "w")
    f.write(" & Uber & Uber & Uber & Uber & Lyft & Lyft & Lyft & Lyft & Taxi & Taxi & Taxi & Taxi \\\\ \n")
    f.write(" & Pri. & Sup. & Dem. & Sur. & Pri. & Sup. & Dem. & Sur. & Pri. & Sup. & Dem. & Sur. \\\\ \\midrule \n")
    city_box.append("intercept")
    city_box.append("r2")
    city_box.append("sample_size")
    for fea in city_box:
        f.write(out(fea))
        if fea == "r2":
            for reg in table_dict[fea]:
                if str(reg) != "nan":
                    f.write(" & $" + "{0:.2f}".format(reg) + "$")
                else:
                    f.write(" & ")
        elif fea == "sample_size":
            for reg in table_dict[fea]:
                f.write(" & $" + str(int(reg)) + "$")
        else:
            for reg in table_dict[fea]:
                if str(reg["coef"]) != "nan":
                    if reg["p"] > 0.1:
                        f.write(" & $" + "{0:.2f}".format(reg["coef"]) + "$")
                    elif reg["p"] > 0.05:
                        f.write(" & $\\mathbf{" + "{0:.2f}".format(reg["coef"]) + "^*}$")
                    elif reg["p"] > 0.01:
                        f.write(" & $\\mathbf{" + "{0:.2f}".format(reg["coef"]) + "^**}$")
                    else:
                        f.write(" & $\\mathbf{" + "{0:.2f}".format(reg["coef"]) + "^***}$")
                else:
                    f.write(" & ")
        f.write(" \\\\ \\midrule \n")
