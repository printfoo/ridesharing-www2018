import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon
from geopandas.tools import sjoin
from scipy.stats import pearsonr
import pysal as ps
import numpy as np
import sys

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    map_path = file_path + "/resources/" + city + "_block_groups/" + city + "_bg_with_data_acs15.geojson"
    all_data_path = file_path + "/resources/" + city + "_data/" + city + "_all_group_geo.csv"

    # Read data.
    map_df = gpd.read_file(map_path)
    map_df = map_df.to_crs({"init": "epsg:3857"})
    map_df["area"] = map_df["geometry"].area / 10**6
    data_df = pd.read_csv(all_data_path)
    print data_df.columns
    gdf = map_df.merge(data_df, on = "geo_id", how = "left")
    gdf.index = gdf["geo_id"]
    for fil in ["bus_stop", "parking_meter", "parking_off_street"]:
        gdf[fil] = gdf[fil].fillna(0)
    gdf["uber_eta"] = gdf["uber_eta"] / 60
    gdf["lyft_eta"] = gdf["lyft_eta"] / 60
    gdf["white"] = gdf["white"] / 100
    gdf["income"] = gdf["income"] / 1000
    gdf["dens"] = gdf["population"] / gdf["area"]
    gdf = gdf[["white", "bus_stop", "parking_meter", "parking_off_street"]].dropna()
    print pearsonr(gdf["white"], gdf["bus_stop"])
    print pearsonr(gdf["white"], gdf["parking_meter"])
    print pearsonr(gdf["white"], gdf["parking_off_street"])
    sys.exit()

    # Analysis.
    printer = ["" for i in range(10)]
    rsq = ""
    sampsi = ""
    Ylist = ["uber_sup", "lyft_sup", "taxi_sup",
    "uber_dem", "lyft_dem", "taxi_dem",
    "uber_pri", "lyft_pri", "uber_eta", "lyft_eta"]
    for Y in Ylist:
        Vars = ["geometry"]
        Vars.extend([Y, "dens", "white", "income", "education", "family_ratio",
            "bus_stop", "parking_meter", "parking_off_street"])
        YVar = Vars[1]
        XVars = Vars[2:]
        df = gdf[Vars].dropna()
        w = ps.weights.Queen.from_dataframe(df)
        Y = df[[YVar]].values
        X = df[XVars].values

        try:
            lag = ps.spreg.ML_Lag(Y, X, w, method = "LU", name_y = YVar, name_x = XVars)
            print lag.summary

            for i in range(10):
                if lag.z_stat[i][1] < 0.001:
                    printer[i] += "$\\mathbf{" + str(np.around(lag.betas, decimals=4)[i][0]) + "}$ & $\\mathbf{^{***}}$ & "
                elif lag.z_stat[i][1] < 0.01:
                    printer[i] += "$\\mathbf{" + str(np.around(lag.betas, decimals=4)[i][0]) + "}$ & $\\mathbf{^{**}}$ & "
                elif lag.z_stat[i][1] < 0.05:
                    printer[i] += "$\\mathbf{" + str(np.around(lag.betas, decimals=4)[i][0]) + "}$ & $\\mathbf{^{*}}$ & "
                else:
                    printer[i] += "$" + str(np.around(lag.betas, decimals=4)[i][0]) + "$ & & "

            rsq += str(np.around(lag.pr2, decimals=4)) + " & & "
            sampsi+= str(lag.n) + " & & "

        except:
            for i in range(10):
                printer[i] += "$\\hspace{43pt}$ & & "
            rsq += " & & "
            sampsi+= " & & "
    for string in printer:
        print string[:-2] + "\\\\"
        print ""
    print rsq[:-2] + "\\\\"
    print sampsi[:-2] + "\\\\"

    """
    lag = ps.spreg.ML_Lag(Y, X, w, method = "LU", name_y = YVar, name_x = XVars)
    print lag.summary
    mi = ps.Moran(lag.u, w, two_tailed = False)
    print pd.Series(index=['Morans I','Z-Score','P-Value'],data=[mi.I, mi.z_norm, mi.p_norm])

    b=lag.betas[:-1]
    rho=lag.betas[-1]
    print rho
    btot=b/(float(1)-rho)
    bind=btot-b

    full_eff=pd.DataFrame(np.hstack([b,bind,btot]),index=['Constant']+XVars, columns=['Direct','Indirect','Total'])
    print full_eff

    ols = ps.spreg.OLS(Y, X, w = w, name_y = YVar, name_x = XVars, nonspat_diag = False, spat_diag = True)
    print ols.summary
    mi = ps.Moran(ols.u, w, two_tailed=False)
    print pd.Series(index=['Morans I','Z-Score','P-Value'],data=[mi.I, mi.z_norm, mi.p_norm])
    """

