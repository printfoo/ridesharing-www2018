import geopandas as gpd
import pandas as pd
import numpy as np
import json, sys

def get_num(x):
    try:
        return float(x)
    except ValueError:
        return np.nan

def get_time(r):
    t = {}
    t[7] = r["Estimate; Total: - 5 to 9 minutes"]
    t[12] = r["Estimate; Total: - 10 to 14 minutes"]
    t[17] = r["Estimate; Total: - 15 to 19 minutes"]
    t[22] = r["Estimate; Total: - 20 to 24 minutes"]
    t[27] = r["Estimate; Total: - 25 to 29 minutes"]
    t[32] = r["Estimate; Total: - 30 to 34 minutes"]
    t[37] = r["Estimate; Total: - 35 to 39 minutes"]
    t[42] = r["Estimate; Total: - 40 to 44 minutes"]
    t[52] = r["Estimate; Total: - 45 to 59 minutes"]
    t[75] = r["Estimate; Total: - 60 to 89 minutes"]
    t[100] = r["Estimate; Total: - 90 or more minutes"]
    s = 0.0
    n = 0.0
    for i in t:
        s += t[i] * i
        n += t[i]
    try:
        return s/n
    except ZeroDivisionError:
        return np.nan

def get_education(r):
    e = {}
    e[0] = r["Estimate; Total: - No schooling completed"]
    e[1] = r["Estimate; Total: - Nursery school"]
    e[2] = r["Estimate; Total: - Kindergarten"]
    e[3] = r["Estimate; Total: - 1st grade"]
    e[4] = r["Estimate; Total: - 2nd grade"]
    e[5] = r["Estimate; Total: - 3rd grade"]
    e[6] = r["Estimate; Total: - 4th grade"]
    e[7] = r["Estimate; Total: - 5th grade"]
    e[8] = r["Estimate; Total: - 6th grade"]
    e[9] = r["Estimate; Total: - 7th grade"]
    e[10] = r["Estimate; Total: - 8th grade"]
    e[11] = r["Estimate; Total: - 9th grade"]
    e[12] = r["Estimate; Total: - 10th grade"]
    e[13] = r["Estimate; Total: - 11th grade"]
    e[14] = r["Estimate; Total: - 12th grade, no diploma"]
    e[15] = r["Estimate; Total: - Regular high school diploma"]
    e[16] = r["Estimate; Total: - GED or alternative credential"]
    e[17] = r["Estimate; Total: - Some college, less than 1 year"]
    e[18] = r["Estimate; Total: - Some college, 1 or more years, no degree"]
    e[19] = r["Margin of Error; Total: - Associate's degree"]
    e[20] = r["Margin of Error; Total: - Bachelor's degree"]
    e[21] = r["Margin of Error; Total: - Master's degree"]
    e[22] = r["Estimate; Total: - Professional school degree"]
    e[23] = r["Estimate; Total: - Doctorate degree"]
    s = 0.0
    n = 0.0
    for i in e:
        s += e[i] * i
        n += e[i]
    try:
        return s/n
    except ZeroDivisionError:
        return np.nan

if __name__ == "__main__":

    # Get file path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    bg_path = file_path + "/resources/" + city + "_block_groups/" + city + "_block_groups_2015.geojson"
    data_path = file_path + "/resources/" + city + "_block_groups/" + city + "_bg_with_data_acs15.geojson"

    race_path = file_path + "/resources/" + city + "_acs15/acs15_race.csv"
    hispanic_path = file_path + "/resources/" + city + "_acs15/acs15_hispanic_origin.csv"
    income_path = file_path + "/resources/" + city + "_acs15/acs15_income.csv"
    house_value_path = file_path + "/resources/" + city + "_acs15/acs15_house_value.csv"
    house_type_path = file_path + "/resources/" + city + "_acs15/acs15_household_type.csv"
    time_work_path = file_path + "/resources/" + city + "_acs15/acs15_time_to_work.csv"
    means_work_path = file_path + "/resources/" + city + "_acs15/acs15_means_to_work.csv"
    education_path = file_path + "/resources/" + city + "_acs15/acs15_education.csv"
    open_path = file_path + "/resources/" + city + "_acs15/opendata_with_geoid.csv"

    # Read data.
    bg_gdf = gpd.read_file(bg_path)
    bg_gdf["Id2"] = bg_gdf["GEOID"].astype("int")

    race_df = pd.read_csv(race_path)
    race_df["population"] = race_df["Estimate; Total:"].astype("float")
    race_df["white"] = race_df["Estimate; Total: - White alone"].astype("float")
    race_df["black"] = race_df["Estimate; Total: - Black or African American alone"].astype("float")
    race_df["asian"] = race_df["Estimate; Total: - Asian alone"].astype("float")
    race_df["nati_hawaii"] = race_df["Estimate; Total: - Native Hawaiian and Other Pacific Islander alone"].astype("float")
    race_df["amer_indian"] = race_df["Estimate; Total: - American Indian and Alaska Native alone"].astype("float")
    race_df["other"] = race_df["Estimate; Total: - Some other race alone"].astype("float")
    race_df["more"] = race_df["Estimate; Total: - Two or more races:"].astype("float")
    for fea in ["white", "black", "asian", "nati_hawaii", "amer_indian", "other", "more"]:
        race_df[fea + "_ratio"] = race_df[fea] / race_df["population"]
    
    hispanic_df = pd.read_csv(hispanic_path)
    hispanic_df["_population"] = hispanic_df["Estimate; Total:"].astype("float")
    hispanic_df["hispanic"] = hispanic_df["Estimate; Total: - Hispanic or Latino"].astype("float")
    hispanic_df["hispanic_ratio"] = hispanic_df["hispanic"] / hispanic_df["_population"]
    
    income_df = pd.read_csv(income_path)
    income_df["income"] = income_df["Estimate; Median household income in the past 12 months (in 2015 Inflation-adjusted dollars)"].apply(get_num)

    house_value_df = pd.read_csv(house_value_path)
    house_value_df["house_value"] = house_value_df["Estimate; Median value (dollars)"].apply(get_num)
    
    house_type_df = pd.read_csv(house_type_path)
    house_type_df["house_num"] = house_type_df["Estimate; Total:"].astype("float")
    house_type_df["family"] = house_type_df["Estimate; Family households:"].astype("float")
    house_type_df["nonfamily"] = house_type_df["Estimate; Nonfamily households:"].astype("float")
    for fea in ["family", "nonfamily"]:
        house_type_df[fea + "_ratio"] = house_type_df[fea] / house_type_df["house_num"]
    
    time_work_df = pd.read_csv(time_work_path)
    time_work_df["time_to_work"] = time_work_df.apply(get_time, axis = 1)
    
    means_work_df = pd.read_csv(means_work_path)
    means_work_df["total_to_work"] = means_work_df["Estimate; Total:"].astype("float")
    means_work_df["by_car"] = means_work_df["Estimate; Car, truck, or van: - Drove alone"].astype("float")
    means_work_df["by_carpool"] = means_work_df["Estimate; Car, truck, or van: - Carpooled:"].astype("float")
    means_work_df["by_pub"] = means_work_df["Estimate; Public transportation (excluding taxicab):"].astype("float")
    means_work_df["by_taxi"] = means_work_df["Estimate; Taxicab"].astype("float")
    means_work_df["by_moto"] = means_work_df["Estimate; Motorcycle"].astype("float")
    means_work_df["by_bike"] = means_work_df["Estimate; Bicycle"].astype("float")
    means_work_df["on_feet"] = means_work_df["Estimate; Walked"].astype("float")
    means_work_df["at_home"] = means_work_df["Estimate; Worked at home"].astype("float")
    for fea in ["by_car", "by_carpool", "by_pub", "by_taxi", "by_moto", "by_bike", "on_feet", "at_home"]:
        means_work_df[fea + "_ratio"] = means_work_df[fea] / means_work_df["total_to_work"]
    
    education_df = pd.read_csv(education_path)
    education_df["education"] = education_df.apply(get_education, axis = 1)
    
    open_df = pd.read_csv(open_path)
    open_df["Id2"] = open_df["geoid"]
    if city.lower() == "sf":
        open_df["bus_stop"] = open_df["stop_id"]
        open_df["parking_meter"] = open_df["POST_ID"]
        open_df["parking_off_street"] = open_df["Address"]
    else:
        del open_df["geometry"]
        open_df["bus_stop"] = open_df["stop_id_x"].fillna(0) + open_df["stop_id_y"].fillna(0)
        open_df["parking_meter"] = open_df["sel"].fillna(0)
        open_df["parking_off_street"] = open_df["DCA License Number"].fillna(0)

    # Join data.
    gdf = bg_gdf.merge(race_df, on = "Id2", how = "left") \
        .merge(hispanic_df, on = "Id2", how = "left") \
        .merge(income_df, on = "Id2", how = "left") \
        .merge(house_value_df, on = "Id2", how = "left") \
        .merge(house_type_df, on = "Id2", how = "left") \
        .merge(time_work_df, on = "Id2", how = "left") \
        .merge(means_work_df, on = "Id2", how = "left") \
        .merge(education_df, on = "Id2", how = "left") \
        .merge(open_df, on = "Id2", how = "left")
    gdf["geo_id"] = gdf["GEOID"].astype("int")

    # Save data.
    check_box = ["geo_id", "geometry", "population", "income", "house_value", "education", "bus_stop", "parking_meter", "parking_off_street",
        "white", "black", "asian", "nati_hawaii", "amer_indian", "other", "more",
        "white_ratio", "black_ratio", "asian_ratio", "nati_hawaii_ratio", "amer_indian_ratio", "other_ratio", "more_ratio",
        "hispanic_ratio", "family_ratio", "family", "time_to_work",
        "by_car_ratio", "by_carpool_ratio", "by_pub_ratio", "by_taxi_ratio", "by_moto_ratio", "by_bike_ratio", "on_feet_ratio", "at_home_ratio",
        "by_car", "by_carpool", "by_pub", "by_taxi", "by_moto", "by_bike", "on_feet", "at_home"]
    gdf = gdf[check_box]
    gdf_js = gdf.to_json()
    with open(data_path, "w") as data_file:
        data_file.write(gdf_js)
