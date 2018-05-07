#!/usr/bin/env python3

import argparse
import os
import string
import sys

from csv import reader
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

HEADER1 = ["temp_res", "spatial_res"]
HEADER2 = ["temp_res", "spatial_res"]

def read_args():
    """
    Argument parser for datasets and their corresponding headers
    """
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-input", dest="input_paths", nargs=2, required=True,
                            type=str, help="2?? preprocessed datasets")

    arg_parser.add_argument("-header", dest="header_paths", nargs=2, required=True,
                            type=str, help="2?? dataset header files")

    args = arg_parser.parse_args()
    ds1, ds2 = args.input_paths
    h1, h2 = args.header_paths
    return ds1, ds2, h1, h2

def init_headers(h1, h2):
    with open(h1, "r") as f:
        header1 = f.readline()

    header1 = [x.strip(string.punctuation) for x in header1.strip().split(",")]
    HEADER1.extend(header1)

    with open(h2, "r") as f:
        header2 = f.readline()

    header2 = [x.strip(string.punctuation) for x in header2.strip().split(",")]
    HEADER2.extend(header2)


def correlate(ds1, ds2, conf):
    spark = SparkSession.builder \
                        .config(conf=conf) \
                        .getOrCreate()

    # sc = SparkContext.getOrCreate(conf)
    # rdd1 = sc.textFile(ds1, 1).mapPartitions(lambda x: reader(x))
    # rdd2 = sc.textFile(ds2, 1).mapPartitions(lambda x: reader(x))
    #
    # df1 = spark.createDataFrame(rdd1, HEADER1)
    # df2 = spark.createDataFrame(rdd2, HEADER2)

    df1 = spark.read.format("csv").options(header='false', inferschema='true').load(ds1)

    """THINK OF A WAY TO RENAME THE COLUMNS FOR EACH DATASET"""
    df2 = spark.read.format("csv").options(header='false', inferschema='true').load(ds2)

    print(df1.take(10))
    print(df2.take(10))

    df1.printSchema()
    df2.printSchema()

    """
    For Spearman, a rank correlation, we need to create an RDD[Double] for each column and sort it
    in order to retrieve the ranks and then join the columns back into an RDD[Vector], which is fairly costly.
    Cache the input Dataset before calling corr with method = ‘spearman’ to avoid recomputing the common lineage.
    """

    # # join the 2 datasets
    # joined = df1.join(df2, ['temp_res', 'spatial_res'], 'inner')
    #
    # # now we have the joined result. next step is to assemble the vector for Correlation.corr()
    # # we wanna ignore the first 2 columns (temp-spatial key)
    # input_columns = joined.columns[2:]
    #
    # assembler = VectorAssembler(
    #     inputCols=input_columns,
    #     outputCol="features"
    # )
    #
    # assembled_output = assembler.transform(joined)
    # print("Assembled columns")
    # print(assembled_output.take(10))
    #
    # # print(joined.take(10))
    # # print(joined.printSchema())
    # spearmanCorr = Correlation.corr(assembled_output, "features", method='spearman').collect()[0][0]
    # print(str(spearmanCorr))
    #
    # selected = joined.select(input_columns).take(10)
    # print(selected)


if __name__ == "__main__":

    ds1, ds2, h1, h2 = read_args()

    # initiate my SparkConf object
    conf = SparkConf()
    conf.setMaster("local") \
        .setAppName("cs6513_project_correlation") \
        .set("testkey", "testval") \

    init_headers(h1, h2)
    correlate(ds1, ds2, conf)
