#!/usr/bin/env python3

import os
import sys
import resolutions_temporal as tr
import resolutions_spatial as sr

from csv import reader
from pyspark import SparkContext, SparkConf
from shapely.geometry import Polygon, Point

regions = []
polygons = []
HEADERS = []
DEFAULTS = []

def init_mapping(file):
    """
    Initialize regions and polygons with the file (e.g. zipcode.txt) !! this is just the zipcode file.
    """
    coords = []
    with open(file, "r") as f:
        line = f.readline()

        while line:
            zip_code = int(line)
            skip = f.readline() # all 1s
            num_coords = int(f.readline())

            regions.append(zip_code)

            for i in range(num_coords):
                x, y = f.readline().strip().split(" ") # space delimited
                """IMPORTANT: 2nd coord(lat) first"""
                coords.append((float(y), float(x)))

            if not coords:
                sys.exit("Something went wrong with init_mapping")

            polygon = Polygon(coords)
            polygons.append(polygon)

            coords = []
            line = f.readline()
    # for testing
    # return regions, polygons

def init_header_and_default(file):
    """
    Initialize dataset header and default values
    """
    with open(file, "r") as f:
        header = f.readline()
        default = f.readline()

    # extend! don't assign
    HEADERS.extend(header.strip().split(","))
    DEFAULTS.extend(default.strip().split(","))

    if len(HEADERS) != len(DEFAULTS):
        print(len(HEADERS), len(DEFAULTS))
        sys.exit("Something wrong with init_header_and_default. Header file should have header and default values")
    # for testing
    # return HEADERS, DEFAULTS

def parser_count(line):
    """
    Parser for count aggregation ONLY
    """
    # careful with the 2. this depends on how many spatial + tempora keys are appended to the end of the line
    key_t, key_s = line[-2:]
    key = str(key_t) + '|' + str(key_s)
    values = line[:-2]
    valid_values = [1 if x!=y else 0 for x, y in zip(values, DEFAULTS)]

    return (key, valid_values)

def seq_func(template, row):
    """
    template = new template
    row = a row worth of original data
    """
    return [x + y for x, y in zip(template, row)]

def comb_func(part1, part2):
    """
    part1 = partition 1
    part2 = partition 2
    """
    return [x + y for x, y in zip(part1, part2)]

def preprocess(uri):

    sc = SparkContext.getOrCreate()
    ds_rdd = sc.textFile(uri, 1).mapPartitions(lambda x: reader(x))

    # Filter out header if present. No better way it seems. Check the first element.
    filtered_ds = ds_rdd.filter(lambda line: line[0] != HEADERS[0])

    """DO TEMPORAL FIRST"""

    # Temporal resolution
    """ NEED TO FIND A BETTER WAY - GET TEMPORAL INDEX FROM SparkConf """
    temporal_index = 0
    # Turn into lambda function so to pass in temporal index argument
    ds_with_temp = filtered_ds.map(lambda line: tr.resolve_temporal_date(line, temporal_index))

    # Spatial resolution
    """ NEED TO FIND A BETTER WAY - GET SPATIAL INDEX FROM SparkConf"""
    # Turn into lambda function so to pass in temporal index argument
    spatial_index1 = 4
    spatial_index2 = 5
    ds_with_spat_temp = ds_with_temp.map(lambda line: sr.resolve_spatial_zip(line, spatial_index1, spatial_index2, regions, polygons))

    zero_values = [0] * len(HEADERS)
    temp = ds_with_spat_temp.map(parser_count) \
                            .aggregateByKey(zero_values, seq_func, comb_func) \
                            .sortByKey() \
                            .coalesce(1) \
                            .saveAsTextFile("test.out")
    # print(temp)
    sc.stop()


if __name__ == "__main__":
    uri = sys.argv[1]
    init_mapping('./data/zipcode.txt')
    init_header_and_default('./data/header_collisions.csv')
    preprocess(uri)
