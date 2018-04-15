#!/usr/bin/env python3

from shapely.geometry import Polygon, Point

def points_to_region(x, y, regions, polygons):
    """
    DO NOT call this function directly
    """
    point = Point(x, y)

    index = -1
    for i in range(len(regions)): # len(regions) == len(polygons)
        polygon = polygons[i]
        if polygon.contains(point):
            index = i
            break

    if index == -1:
        return 0

    return regions[i]

def resolve_spatial_zip(line, x, y, zip_codes, polygons):
    """
    line = one line/row of data
    x = index of latitude attribute in line
    y = index of longitude attribute in line
    zip_codes = regions created by init_mapping()
    polygons = polygons created by init_mapping()
    """
    # if missing any coordinate, default region to 0
    if not line[x] or not line[y]:
        # print("no coords")
        line.append(0)
        return line

    lat = float(line[x])
    lng = float(line[y])
    spatial_res = points_to_region(lat, lng, zip_codes, polygons)
    line.append(spatial_res) # spatial res added to the end
    return line

def resolve_spatial_nbhd(line, x, y, nbhds, polygons):
    """
    line = one line/row of data
    x = index of latitude attribute in line
    y = index of longitude attribute in line
    nbhds = regions created by init_mapping()
    polygons = polygons created by init_mapping()
    """
    #TODO
    pass

def resolve_spatial_nta(line, x, y, ntas, polygons):
    """
    line = one line/row of data
    x = index of latitude attribute in line
    y = index of longitude attribute in line
    ntas = regions created by init_mapping()
    polygons = polygons created by init_mapping()
    """
    #TODO
    pass

def resolve_spatial_ct(line, x, y, cts, polygons):
    """
    line = one line/row of data
    x = index of latitude attribute in line
    y = index of longitude attribute in line
    cts = regions created by init_mapping()
    polygons = polygons created by init_mapping()
    """
    #TODO
    pass
