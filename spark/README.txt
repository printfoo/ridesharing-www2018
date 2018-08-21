######################################################
###################  Running Guide  ##################
######################################################

1. Copy Uber and Lyft raw response to HDFS and rename as "[APP]_[CITY]_raw_response";
   Copy Taxi clean telemetry to HDFS and rename as "[APP]_[CITY]_clean_telemetry";
   (Parameter: [APP] = "uber" or "lyft" or "taxi", [CITY] = "sf" or "nyc")

################ A. Get Trajectory Data ##############

2. a. Dealing with Uber and Lyft:
   Run raw_response_to_raw_trajectory.py and get "[APP]_[CITY]_raw_trajectory".
   (Parameter: [APP] = "uber" or "lyft", [CITY] = "sf" or "nyc")
   :~$ spark-submit raw_response_to_raw_trajectory.py [APP] [CITY]

   b. Dealing with Taxi:
   Run clean_telemetry_to_clean_trajectory.py and get "[APP]_[CITY]_clean_trajectory".
   (Parameter: [APP] = "taxi", [AREA] = "sf")
   :~$ spark-submit clean_telemetry_to_clean_trajectory.py

3. Run raw_trajectory_to_clean_trajectory.py and get "[APP]_[CITY]_proc_trajectory".
   (Parameter: [APP] = "uber" or "lyft", [AREA] = "dt", "ne", "nw", "sx", "il", "hp" for SF or "lm", "um", "bq", "si", "jk" for NYC)
   :~$ spark-submit raw_trajectory_to_clean_trajectory.py [APP] [AREA]

4. Run raw_trajectory_to_clean_trajectory.py again and get "[APP]_[CITY]_clean_trajectory";
   (Parameter: [APP] = "uber" or "lyft", [AREA] = "all_sf" or "all_nyc")
   :~$ spark-submit raw_trajectory_to_clean_trajectory.py [APP] [AREA]

5. Run clean_trajectory_to_map_trajectory.py and get "map_[CITY]_trajectory".
   (Parameter: [CITY] = "sf" or "nyc")
   :~$ spark-submit clean_trajectory_to_map_trajectory.py [CITY]

6. Run map_trajectory_relabel.py and get relabeled "map_[CITY]_trajectory".
   :~$ spark-submit map_trajectory_relabel.py [CITY]

7. a. Get supply:
   Run map_trajectory_to_supply.py and get "[CITY]_supply_[5/10min].csv".
   :~$ spark-submit map_trajectory_to_supply.py [CITY] [TIME]

   b. Get demand:
   Run map_trajectory_to_demand.py and get "[CITY]_demand_[5/10min].csv".
   :~$ spark-submit map_trajectory_to_demand.py [CITY] [TIME]

   c. Get idle time:
   Run map_trajectory_to_idletime.py and get "[CITY]_idletime.csv".
   :~$ spark-submit map_trajectory_to_idletime.py [CITY]

   d. Get idle area:
   Run map_trajectory_to_idlearea.py and get "[CITY]_idlearea.csv".
   :~$ spark-submit map_trajectory_to_idlearea.py [CITY]

   e. Get lifespan:
   Run map_trajectory_to_lifespan.py and get "[CITY]_lifespan.csv".
   :~$ spark-submit map_trajectory_to_lifespan.py [CITY]

   f. Get statistics:
   Run map_trajectory_to_statistics.py and get "[CITY]_statistics.csv".
   :~$ spark-submit map_trajectory_to_statistics.py [CITY]

################ B. Get Measurement Data ##############

2. Run raw_response_to_raw_measurement.py and get "[APP]_[CITY]_raw_measurement".
   :~$ spark-submit raw_response_to_raw_measurement.py [APP] [CITY]

3. Run raw_measurement_to_map_measurement.py and get "map_[CITY]_measurement".
   :~$ spark-submit raw_measurement_to_map_measurement.py [CITY]

4. Run map_measurement_to_indicator.py and get "[CITY]_indicator_[5/10min].csv".
   :~$ spark-submit map_measurement_to_indicator.py [CITY] [TIME]
