from scipy.stats.stats import pearsonr
import pandas as pd
import numpy as np
import sys, json

if __name__ == "__main__":

    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    dim = sys.argv[2].lower()
    data_path = file_path + "/resources/" + city + "_data/" + city + "_all_data.csv"
    plot_path = file_path + "/resources/" + city + "_data/" + city + "_over" + dim + "_plot_data.json"

    # Read data.
    all_df = pd.read_csv(data_path)

    if dim == "group":
        df = all_df.groupby("geo_id").mean()
        group_path = file_path + "/resources/" + city + "_data/" + city + "_group_geo.csv"
        df.to_csv(group_path)
        sys.exit()

    # Get information.
    index_list = []
    plot_dict = {"pri":{"uber": [], "lyft": [], "taxi": [],
                        "uber-lyft": [], "uber-taxi": [], "lyft-taxi": []},
                 "sup":{"uber": [], "lyft": [], "taxi": [],
                        "uber-lyft": [], "uber-taxi": [], "lyft-taxi": []},
                 "dem":{"uber": [], "lyft": [], "taxi": [],
                        "uber-lyft": [], "uber-taxi": [], "lyft-taxi": []}}
    dim_dict = {"time": "timestamp", "space": "geo_id"}
    for index, df in all_df.groupby(dim_dict[dim]):
        index_list.append(index)
        for fea in plot_dict:
            for app in plot_dict[fea]:
                app = app.split("-")
                if len(app) == 1: # Compute mean for price, supply and demand.
                    meanv = df[app[0] + "_" + fea].mean()
                    plot_dict[fea][app[0]].append(meanv)
                else: # Compute distance for price, supply and demand.
                    adf = df[[app[0] + "_" + fea, app[1] + "_" + fea]].dropna()
                    corr, p = pearsonr(adf[app[0] + "_" + fea], adf[app[1] + "_" + fea])
                    plot_dict[fea][app[0] + "-" + app[1]].append(corr)
    plot_dict["index"] = index_list

    # Write to file.
    with open(plot_path, "w") as plot_file:
        plot_file.write(json.dumps(plot_dict))
        
    

    
