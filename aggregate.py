#!/usr/bin/env python3

import argparse
import os
import string
import scalar_functions as fn
import sys

from csv import reader
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

unique_params_indices = []
numeric_params_indices = []

def identify_aggregations(header):
    for i in range(len(header) - 1):
        if "id" in header[i] or "key" in header[i] or "name" in header[i] or "type" in header[i]:
            unique_params_indices.append(i)
            continue

        if header[i] == "temp_res" or header[i] == "spat_res":
            continue

        numeric_params_indices.append(i)

def aggregate(uri, conf):
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    parsed_df = spark.read.format('csv').options(header=True,inferschema=True).load(uri)

    columns = parsed_df.columns
    identify_aggregations(columns)

    print("unique_params_indices: " + str(unique_params_indices))
    print("numeric_params_indices: " + str(numeric_params_indices))

    # initiate output DF with record count
    cnt_col_name = spark.conf.get("output") + "_count"
    output = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spat_res]) \
                      .count() \
                      .alias(cnt_col_name)

    # select and handle categorical attributes
    cat_col_names = [columns[i] for i in unique_params_indices]
    for name in cat_col_names:
        col = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spat_res]) \
                       .agg(F.approx_count_distinct(name).alias(name+"_uniq"))

        output = output.join(col, ["temp_res", "spat_res"], 'left')

    # select and handle numeric attributes
    num_col_names = [columns[i] for i in numeric_params_indices]
    # fill in NaN numeric values to avoid error
    parsed_df = parsed_df.fillna(0, subset=num_col_names)

    for name in num_col_names:
        col = parsed_df.groupBy([parsed_df.temp_res, parsed_df.spat_res]) \
                       .agg(F.mean(name).alias(name+"_avg"))

        output = output.join(col, ["temp_res", "spat_res"], 'left')

    # write aggregated dataset
    out_dir = spark.conf.get("output")
    out_dir = "aggregtes/" + out_dir

    output.printSchema()
    output.show(40)
    print(output.count())

    # output.write.csv(out_dir, header=True)
    spark.stop()


def read_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-input", dest="input_file", nargs=1, required=True,
                            type=str, help="preprocessed input dataset")

    arg_parser.add_argument("-output", dest="output_dir", nargs=1, required=True,
                            type=str, help="aggregate output directory")

    args = arg_parser.parse_args()
    return args

def setup():
    args = read_args()
    uri = args.input_file[0]
    out_dir = args.output_dir[0]

    conf = SparkConf()
    conf.setAppName("CS6513 project preprocess") \
        .set("output", out_dir)

    return uri, conf


if __name__ == "__main__":
    uri, conf = setup()
    aggregate(uri, conf)
