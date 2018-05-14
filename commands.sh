###################
localcommands
##################

# run preprocess on citibike
python3 preprocess.py -input data/201501-citibike-tripdata-test.csv -output citibike201501_test -region data/zipcode.txt -temp_index 1 -temp_format "MM/dd/yyyy HH:mm" -spt_indices 5 6
# run aggregate on citibike
python3 aggregate.py -input preprocess/citibike201402 -output citibike201402
# run correlate on collisions and citibike
python3 correlate.py -input aggregates/citibike201501_test aggregates/taxi201501_test -output citibike_taxi_201501_test

# run preprocess on collisions
python3 preprocess.py -input data/NYPD_Motor_Vehicle_Collisions_test.csv -output collisions[year] -region data/zipcode.txt -temp_index 0 -temp_format "MM/dd/yyyy" -spt_indices 4 5
# run preprocess on taxi !!!!! 6 5
python3 preprocess.py -input data/yellow_tripdata_2015-01_test.csv -output taxi201501_test -region data/zipcode.txt -temp_index 1 -temp_format "yyyy-MM-dd HH:mm:SS" -spt_indices 6 5



####################
spark-submit example commands
####################
--conf spark.driver.memory=2g --conf spark.executor.memory=2g
--py-files libgeos.zip --py-files shapely.zip

# preprocess on citibike
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
preprocess.py -input data/citibike/201403-citibike-tripdata.csv -output citibike201403 -region data/zipcode.txt -temp_index 1 -temp_format "yyyy-MM-dd HH:mm:SS" -spt_indices 5 6

# for citibike 2015

# aggregate citibike example
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python aggregate.py -input preprocess/citibike2015 -output citibike2015


# preprocess collisions
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python \
preprocess.py -input data/collision/NYPD_Motor_Vehicle_Collisions.csv -output collisions -region data/zipcode.txt -temp_index 0 -temp_format "MM/dd/yyyy" -spt_indices 4 5
# aggregate
