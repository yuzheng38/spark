###################
localcommands
##################

# run preprocess on citibike
python3 preprocess.py -input data/2014-02-citibike-trip-data-test.csv -output citibike201402 -region data/zipcode.txt -temp_index 1 -temp_format "yyyy-MM-dd HH:mm:SS" -spt_indices 5 6
# run aggregate on citibike
python3 aggregate.py -input preprocess/citibike201402 -output citibike201402
# run correlate on collisions and citibike
python3 correlate.py -input aggregates/citibike201402 aggregates/collisions -output citibike201402_collisions

# run preprocess on collisions
python3 preprocess.py -input data/NYPD_Motor_Vehicle_Collisions_test.csv -output collisions[year] -region data/zipcode.txt -temp_index 0 -temp_format "MM/dd/yyyy" -spt_indices 4 5
# run preprocess on taxi !!!!! 6 5
python3 preprocess.py -input data/yellow_tripdata_2015-01_test.csv -output taxi[year] -region data/zipcode.txt -temp_index 1 -temp_format "yyyy-MM-dd HH:mm:SS" -spt_indices 6 5



####################
spark-submit commands
####################

# preprocess on citibike
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python --conf spark.driver.memory=2g --conf spark.executor.memory=2g \
preprocess.py -input data/citibike/2014-02-citibike-trip-data-test.csv -output citibike[year] -region data/zipcode.txt -temp_index 1 -temp_format "yyyy-MM-dd HH:mm:SS" -spt_indices 5 6

# preprocess collisions
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python spark.driver.memory=2g spark.executor.memory=2g \
preprocess.py --input data/collision/2014-12-citibike-trip-data-test.csv -output citibike[year] -region data/zipcode.txt -temp_index 1 -temp_format "yyyy-MM-dd HH:mm:SS" -spt_indices 5 6
