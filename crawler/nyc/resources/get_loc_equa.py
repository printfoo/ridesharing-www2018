if __name__ == "__main__":

    while True:

        loc1 = input("1st Location:\t")
        loc2 = input("2nd Location:\t")

        lat1 = float(loc1[0])
        lng1 = float(loc1[1])
        lat2 = float(loc2[0])
        lng2 = float(loc2[1])

        k = (lat1 - lat2) / (lng1 - lng2)
        b = lat1 - k * lng1

        disp = str(round(k,5)) + " * lng - lat + " + str(round(b,5)) + " > 0"
        print disp
