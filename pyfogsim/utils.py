import math


def haversine_distance(origin, destination):
    """Haversine formula to calculate the distance between two lat/long points on a sphere """
    radius = 6371.0  # FAA approved globe radius in km

    dlat = math.radians(destination[0] - origin[0])
    dlon = math.radians(destination[1] - origin[1])

    a = math.sin(dlat / 2.0) * math.sin(dlat / 2.0) + math.cos(math.radians(origin[0])) * math.cos(math.radians(destination[0])) * math.sin(
        dlon / 2.0
    ) * math.sin(dlon / 2.0)

    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = radius * c

    return d  # distance in km
