#!/usr/bin/env python3

import argparse
import numpy as np
import pandas as pd
import os
import string
import sys

from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

def correlate(uri1, uri2, conf):
    spark = SparkSession.builder \
                        .config(conf=conf) \
                        .getOrCreate()

    df1 = spark.read.format("csv").options(header=True, inferschema=True).load(uri1)
    df2 = spark.read.format("csv").options(header=True, inferschema=True).load(uri2)

    df1.printSchema()
    df2.printSchema()

    """
    For Spearman, a rank correlation, we need to create an RDD[Double] for each column and sort it
    in order to retrieve the ranks and then join the columns back into an RDD[Vector], which is fairly costly.
    Cache the input Dataset before calling corr with method = ‘spearman’ to avoid recomputing the common lineage.
    """
    # join 2 datasets and ignore first resolution columns
    joined = df1.join(df2, ["temp_res", "spat_res"], 'inner')

    feature_types = joined.dtypes[2:]
    # print(feature_types)

    # drop non numeric features just in case
    num_feature_types = filter(lambda t: t[1] == "int" or t[1] == "double" or t[1] == "float", feature_types)
    features = [f_t[0] for f_t in num_feature_types]
    # print(features)

    joined = joined.select(features)
    joined.printSchema()

    # assemble the Vectors for Correlation.corr(), np.array is equivalent to dense venctors
    vecAssembler = VectorAssembler(
        inputCols=features,
        outputCol="features"
    )
    joinedVec = vecAssembler.transform(joined)
    spearmanCorr = Correlation.corr(joinedVec, 'features', method='spearman').collect()[0][0]

    # turn into pandas dataframe
    spearmanCorr = spearmanCorr.toArray()
    print(spearmanCorr)

    # prepare and write correlation result
    out_dir = spark.conf.get("output")
    out_dir = "correlations/" + out_dir
    print("output directory is: " + out_dir)

    pandasDF = pd.DataFrame(spearmanCorr, index=features, columns=features)
    pandasDF.to_csv(out_dir)

    spark.stop()

def read_args():
    """
    Argument parser for datasets and their corresponding headers
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-input", dest="input_files", nargs="+", required=True,
                            type=str, help="2 aggregated datasets")

    arg_parser.add_argument("-output", dest="output_dir", nargs=1, required=True,
                            type=str, help="correlation output directory")

    args = arg_parser.parse_args()
    return args

def setup():
    args = read_args()
    uri1, uri2 = args.input_files
    out_dir = args.output_dir[0]

    conf = SparkConf()
    conf.setAppName("CS6513 project correlation") \
        .set("output", out_dir)

    return uri1, uri2, conf

if __name__ == "__main__":
    uri1, uri2, conf = setup()
    correlate(uri1, uri2, conf)
