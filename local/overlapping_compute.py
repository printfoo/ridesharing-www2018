import sys, os, json

def parse(line):
    js = json.loads(line)
    for time in js["time_seq"][1:]:
        with open(ovlp_path + js["car_type"] + "/" + str(time), "a") as f2:
                f2.write(str(js["block_seq"][1:]) + "\n")

if __name__ == "__main__":
    # Get path.
    sys_path = sys.path[0]
    sep = sys_path.find("/src")
    file_path = sys_path[0:sep]
    city = sys.argv[1].lower()
    prep_path = file_path + "/resources/" + city + "_data/" + city + "_overlap_prep/"
    ovlp_path = file_path + "/resources/" + city + "_data/" + city + "_overlap/"
    try:
        os.mkdir(ovlp_path)
    except:
        pass
    try:
        os.mkdir(ovlp_path + "UberX/")
    except:
        pass
    try:
        os.mkdir(ovlp_path + "Lyft/")
    except:
        pass
    try:
        os.mkdir(ovlp_path + "Taxi/")
    except:
        pass

    # Read and parse data.
    for _, _, files in os.walk(prep_path):
        for file in files:
            if "part" not in file:
                continue
            f = open(prep_path + file, "r")
            while True:
                line = f.readline().strip("\n")
                if not line:
                    print(file)
                    f.close()
                    break
                parse(line)
