from LatLon import LatLon, Latitude, Longitude

class Distance(object):
    def __init__(self, longitude, latitude):
        self.lon = longitude
        self.lat = latitude

    def move(self, lon_delta, lat_delta):
        self.lon += lon_delta
        self.lat += lat_delta

    def distance(self, lon_new, lat_new, ellipse = 'sphere'):
        old_latlon = LatLon(Latitude(self.lat), Longitude(self.lon))
        new_latlon = LatLon(Latitude(lat_new), Longitude(lon_new))
        return new_latlon.distance(old_latlon, ellipse = ellipse)
