#!/usr/bin/env python3

import os
import string
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

    header = [x.strip(string.punctuation) for x in header.strip().split(",")]
    default = [x.strip(string.punctuation) for x in default.strip().split(",")]

    # extend! don't assign
    HEADERS.extend(header)
    DEFAULTS.extend(default)

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
    key = (key_t, key_s)
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

def formatter(line):
    """
    key = (temporal_string, region_int)
    return: the final output = temp_res,spatial_res,values
    """
    key = line[0]
    values = line[1]

    key_out = key[0] + "," + str(key[1])
    values_out = ",".join([str(val) for val in values])
    out = key_out + "," + values_out
    return out

def preprocess(uri, conf):
    sc = SparkContext.getOrCreate(conf)
    ds_rdd = sc.textFile(uri, 1).mapPartitions(lambda x: reader(x))

    # Filter out header if present. No better way it seems. Check the first element.
    filtered_ds = ds_rdd.filter(lambda line: line[0] != HEADERS[0])

    """DO TEMPORAL FIRST"""
    # Temporal resolution
    conf = sc.getConf()
    ti_date = int(conf.get("ti_date"))
    ti_time = int(conf.get("ti_time"))

    if conf.get("t_res") == "date":
        # Turn into lambda function so to pass in temporal index argument
        ds_with_temp = filtered_ds.map(lambda line: tr.resolve_temporal_date(line, ti_date))
    else:
        ds_with_temp = filtered_ds.map(lambda line: tr.resolve_temporal_datetime(line, ti_date, ti_time))

    # Spatial resolution
    lat = int(conf.get("si_lat"))
    lng = int(conf.get("si_lng"))
    # Turn into lambda function so to pass in temporal index argument
    ds_with_spat_temp = ds_with_temp.map(lambda line: sr.resolve_spatial_zip(line, lat, lng, regions, polygons))

    out_dir = conf.get("out_dir")
    zero_values = [0] * len(HEADERS)
    temp = ds_with_spat_temp.map(parser_count) \
                            .aggregateByKey(zero_values, seq_func, comb_func) \
                            .sortByKey() \   # need to rethink sort or not
                            .coalesce(1) \
                            .map(formatter) \
                            .saveAsTextFile(out_dir)
    # print(temp)
    sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(1)

    uri = sys.argv[1]
    header_file = sys.argv[2]
    init_mapping('./data/zipcode.txt')
    init_header_and_default(header_file)

    """ THESE WILL BE PASSED IN LATER
        more potential options to explore
            .setExecutorEnv()
            .setSparkHome()
    """
    conf = SparkConf()
    conf.setMaster("local") \
        .setAppName("cs6513 project") \
        .set("region_file", "./data/zipcode.txt") \
        .set("ti_date", 1) \
        .set("ti_time", 1) \
        .set("si_lat", 6) \
        .set("si_lng", 5) \
        .set("t_res", "date") \
        .set("s_res", "zip") \
        .set("out_dir", "taxi.out")

    preprocess(uri, conf)
