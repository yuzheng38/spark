#!/usr/bin/env python3

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from preprocess import init_header_and_default

def test_basic():

    HEADERS = """DATE,TIME,BOROUGH,ZIP CODE,LATITUDE,LONGITUDE,LOCATION,ON STREET NAME,CROSS STREET NAME,OFF STREET NAME,NUMBER OF PERSONS INJURED,NUMBER OF PERSONS KILLED,NUMBER OF PEDESTRIANS INJURED,NUMBER OF PEDESTRIANS KILLED,NUMBER OF CYCLIST INJURED,NUMBER OF CYCLIST KILLED,NUMBER OF MOTORIST INJURED,NUMBER OF MOTORIST KILLED,CONTRIBUTING FACTOR VEHICLE 1,CONTRIBUTING FACTOR VEHICLE 2,CONTRIBUTING FACTOR VEHICLE 3,CONTRIBUTING FACTOR VEHICLE 4,CONTRIBUTING FACTOR VEHICLE 5,UNIQUE KEY,VEHICLE TYPE CODE 1,VEHICLE TYPE CODE 2,VEHICLE TYPE CODE 3,VEHICLE TYPE CODE 4,VEHICLE TYPE CODE 5"""
    DEFAULTS = ["", "", "", "", "", "", "", "", "", "", "0", "0", "0", "0", "0", "0", "0", "0", "Unspecified", "", "", "", "", "", "", "", "", "", ""]

    header, default = init_header_and_default('../data/header_collisions.csv')

    split_headers = HEADERS.split(",")
    assert(split_headers == header)
    assert(DEFAULTS == default)
    # print(split_headers)
    # print(default)

def test_basic_citibike():
    headers = '"tripduration","starttime","stoptime","start station id","start station name","start station latitude","start station longitude","end station id","end station name","end station latitude","end station longitude","bikeid","usertype","birth year","gender"'
    defaults = ",,,,,,,,,,,,,,"

    citi_header, citi_default = init_header_and_default("../data/header_citibike.csv")
    headers = headers.split(",")
    defaults = defaults.split(",")

    # print(citi_default)
    # print(defaults)
    # print(headers)
    # print(citi_header)
    assert(citi_header == headers)
    assert(citi_default == defaults)

def test_basic_taxi():
    headers = "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,pickup_longitude,pickup_latitude,RateCodeID,store_and_fwd_flag,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount"
    defaults = ",,,,,,,,,,,,,,,,,,"

    taxi_header, taxi_default = init_header_and_default("../data/header_taxi.csv")
    headers = headers.split(",")
    defaults = defaults.split(",")

    # print(headers)
    # print(taxi_header)
    assert(taxi_header == headers)
    assert(taxi_default == defaults)

if __name__ == "__main__":
    # test_basic()
    # print("{} test_basic() passed".format(__file__))

    # test_basic_citibike()
    # print("{} test_basic_citibike() passed".format(__file__))

    test_basic_taxi()
    print("{} test_basic_taxi() passed".format(__file__))
