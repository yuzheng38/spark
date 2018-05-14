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
from pyspark.sql.types import *
from shapely.geometry import Polygon, Point

regions = []
polygons = []
temp_idx = []
temp_idx_format = []
spt_idx = []
redundant_idx = []

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

def drop_null_cols(df, thresh):
    total_cnt = df.count()

    null_cols = []
    for col in df.columns:
        null_pcnt = df.filter(df[col].isNull()).count() / total_cnt
        if null_pcnt > thresh:
            null_cols.append(col)

    for nc in null_cols:
        df = df.drop(nc)

    return df

def preprocess(uri, conf):
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # this will load all the datasets matching the wild card uri
    df = spark.read.format("csv").options(header="true",inferschema="true").load(uri)
    df.printSchema() # for testing

    # parse the temporal attributes if NOT already inferred
    tp_i = temp_idx[0]
    tp_f = temp_idx_format[0]
    columns = df.columns
    if df.dtypes[tp_i][1] != 'date':
        temp_col = columns[tp_i]
        df = df.withColumn(temp_col, F.to_date(df[temp_col], tp_f))

    df = df.withColumnRenamed(temp_col, "temp_res")

    # parse the spatial attributes if NOT already inferred
    lat, lng = spt_idx[:2]
    lat_col = columns[lat]
    lng_col = columns[lng]
    if df.dtypes[lat][1] != 'double':
        df = df.withColumn(lat_col, df[lat_col].cast('double'))
    if df.dtypes[lng][1] != 'double':
        df = df.withColumn(lng_col, df[lng_col].cast('double'))

    rdd_temp = df.rdd.zipWithIndex()
    df_temp = spark.createDataFrame(rdd_temp, ["_1", "_2"], samplingRatio=0.1)

    # map spatial attributes to zip codes
    spat_rdd = df.select([lat_col, lng_col]) \
                    .rdd \
                    .map(lambda x: [x[lat_col], x[lng_col]]) \
                    .map(lambda coords: sr.resolve_spatial_zip(coords, 0, 1, regions, polygons)) \
                    .zipWithIndex()

    # join with original dataframe
    df_spat = spark.createDataFrame(spat_rdd, ["spat_res", "_2"])

    joined_df = df_temp.join(df_spat, "_2", "inner").drop(df_temp._2).drop(df_spat._2)
    joined_df = joined_df.selectExpr("_1.*", "spat_res")

    # DO THIS AT THE END!!!
    joined_df = drop_null_cols(joined_df, 0.8)

    # write preprocessed dataset
    out_dir = spark.conf.get("output")
    out_dir = "preprocess/" + out_dir
    # print(joined_df.count())
    # joined_df.show()
    print("output directory is: " + out_dir)
    joined_df.write.csv(out_dir, header=True)

    spark.stop()

def read_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-input", dest="input_file", nargs=1, required=True,
                            type=str, help="input dataset")

    arg_parser.add_argument("-output", dest="output_dir", nargs=1, required=True,
                            type=str, help="output directory")

    arg_parser.add_argument("-region", dest="region_file", nargs=1, required=True,
                            type=str, help="region file")

    arg_parser.add_argument("-temp_index", dest="temp_index", nargs=1, required=True,
                            type=int, help="temporal index")

    arg_parser.add_argument("-temp_format", dest="temp_format", nargs=1, required=True,
                            type=str, help="temporal attribute format")

    arg_parser.add_argument("-spt_indices", dest="spt_indices", nargs='+', required=True,
                            type=int, help="lat lng")

    args = arg_parser.parse_args()
    return args

def setup():
    args = read_args()
    uri = args.input_file[0]
    out_dir = args.output_dir[0]
    region_file = args.region_file[0]
    temp_idx.extend(args.temp_index)
    temp_idx_format.extend(args.temp_format)
    spt_idx.extend(args.spt_indices)

    init_mapping(region_file)

    # set up SparkConf()
    conf = SparkConf()
    conf.setAppName("CS6513 project preprocess") \
        .setMaster("local[4]") \
        .set("output", out_dir)

    return uri, conf

if __name__ == "__main__":
    uri, conf = setup()

    print("temp_idx: " + str(temp_idx))
    print("temp_idx_format: " + str(temp_idx_format))
    print("spt_idx: " + str(spt_idx))

    preprocess(uri, conf)
