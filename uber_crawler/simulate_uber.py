import sys
from UberSimulator import UberSimulator

uberrep = UberSimulator(user = sys.argv[1],
                        email = sys.argv[2],
                        password = sys.argv[3],
                        mobile = sys.argv[4],
                        mode = sys.argv[5], 
                        port = int(sys.argv[6]),
                        longitude = float(sys.argv[7]),
                        latitude = float(sys.argv[8]),
                        mov_dir = sys.argv[9])
uberrep.run()
