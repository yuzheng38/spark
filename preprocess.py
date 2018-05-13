#!/usr/bin/env python3

import argparse
import os
import string
import sys
import resolutions_temporal as tr
import resolutions_spatial as sr
import scalar_functions as fn

from csv import reader
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from shapely.geometry import Polygon, Point

""" broadcast these """
regions = []
polygons = []
HEADERS = []
DEFAULTS = []

unique_params_indices = []
numeric_params_indices = []

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
                sys.exit("Something went wrong with init_mapping. Regions was not initiated correctly")

            polygon = Polygon(coords)
            polygons.append(polygon)

            coords = []
            line = f.readline()
    # for testing
    # return regions, polygons

def init_header_and_default(file, conf):
    """
    Initialize dataset header and default values
    """
    with open(file, "r") as f:
        header = f.readline()
        default = f.readline()

    header = [x.strip(string.punctuation) for x in header.strip().split(",")]
    default = [x.strip(string.punctuation) for x in default.strip().split(",")]

    HEADERS.extend(header)
    DEFAULTS.extend(default)

    print(HEADERS)
    print(DEFAULTS)

    if len(HEADERS) != len(DEFAULTS):
        print(len(HEADERS), len(DEFAULTS))
        sys.exit("Something wrong with init_header_and_default. Header file should have header and default values")

def identify_aggregations(header):
    for i in range(len(header) - 1):
        if "id" in header[i] or "key" in header[i] or "name" in header[i] or "type" in header[i]:
            unique_params_indices.append(i)
            continue
        numeric_params_indices.append(i)

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
    # parse and persist
    parsed = ds_with_spat_temp.map(lambda line: fn.default_parser(line, unique_params_indices, numeric_params_indices, DEFAULTS))
                              # .persist(storageLevel=StorageLevel(True, True, False, False, 1))


    spark = SparkSession.builder.getOrCreate()
    schema = ["temp_res", "spt_res"] + HEADERS
    parsed_df = spark.createDataFrame(parsed, schema=schema)
    parsed_df = parsed_df.limit(40)

    # initiate output DF with record count
    output = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spt_res]).count()

    # select and handle categorical attributes
    cat_col_names = [HEADERS[i] for i in unique_params_indices]
    for name in cat_col_names:
        col = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spt_res]).agg(F.approx_count_distinct(name).alias(name+"_uniq"))
        output = output.join(col, ["temp_res", "spt_res"])

    # select and handle numeric attributes
    num_col_names = [HEADERS[i] for i in numeric_params_indices]
    for name in num_col_names:
        col = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spt_res]).agg(F.mean(name).alias(name+"_avg"))
        output = output.join(col, ["temp_res", "spt_res"])

    output.limit(40).show()

    out_dir = conf.get("output")
    out_dir = "aggregates/" + out_dir

    sc.stop()

def read_args():
    """
    Argument parser for datasets and their corresponding headers
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-input", dest="input_paths", nargs=1, required=True,
                            type=str, help="input dataset")

    arg_parser.add_argument("-header", dest="header_paths", nargs=1, required=True,
                            type=str, help="input dataset header file")

    # arg_parser.add_argument("-")

    args = arg_parser.parse_args()
    ds1, ds2 = args.input_paths
    h1, h2 = args.header_paths
    return ds1, ds2, h1, h2


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(1)

    uri = sys.argv[1]
    header_file = sys.argv[2]


    """ THESE WILL BE PASSED IN LATER
        more potential options to explore
            .setExecutorEnv()
            .setSparkHome()
    """
    conf = SparkConf()
    conf.setMaster("local") \
        .setAppName("cs6513_project_preprocess") \
        .set("region_file", "./data/zipcode.txt") \
        .set("ti_date", 1) \
        .set("ti_time", 1) \
        .set("si_lat", 5) \
        .set("si_lng", 6) \
        .set("t_res", "date") \
        .set("s_res", "zip") \
        .set("output", "citi")


    init_mapping('./data/zipcode.txt')
    init_header_and_default(header_file, conf)
    identify_aggregations(HEADERS)
    #
    print("unique_params_indices: " + str(unique_params_indices))
    print("numeric_params_indices: " + str(numeric_params_indices))

    preprocess(uri, conf)
