Written in Python 3.5, in the Spark 2.2.0 and Hadoop 2.6.0 environment. This project also depends on a third-party Python module, named Shapely (version 1.6.0), for spatial resolution.


### Program Structure
/---<br/>
  |--- <code>data</code> - directory where raw datasets reside<br/>
  |--- <code>preprocess</code> directory to where preprocessed datasets are saved<br/>
  |--- <code>aggregates</code> directory to where aggregated datasets are saved<br/>
  |--- <code>correlations</code> directory to where correlation results are saved<br/>
  |--- <code>preprocess.py</code><br/>
  |--- <code>aggregate.py</code><br/>
  |--- <code>correlate.py</code><br/>
  |--- <code>resolutions_spatial.py</code><br/>


### Process flow
A high level process flow is summarized below in three main steps:
>    1. Preprocessing
>    2. Aggregation
>    3. Correlation Calculation

#### 1. preprocess.py
Raw datasets reside <code>/data</code> directory.

The main functionalities of preprocess.py are to parse the datasets into an appropriate format, and at the same time, perform temporal and spatial resolutions. Datasets are read as SparkSession dataframes, and schemas are inferred. However, the schema is often not correct. So additional handling is coded in preprocess.py.

The preprocess job begins by initializing two objects - polygons and region mappings. They are created and kept in memory for spatial resolution translation purpose. Currently our code supports spatial resolution at the zip code level.  We keep a file (<code>zipcode.txt</code>) that contains zip codes in New York City and their corresponding bounding latitude and longitude. Each zip code and its coordinates are used to create a polygon, then added to a <code>(zip code, polygon)</code> mapping table in memory. Polygons are created with the help of the Shapely python module. After each row of the dataset is parsed, it's transformed by a spatial resolution function which uses the mapping table. More spatial resolutions can be added later.

Temporal resolution is done using pyspark functions. We currently support temporal resolution at date level. Because date/datetime format varies between datasets, even within the same dataset but across different months, we have the user provide the format string before starting the preprocess job. More broader or granular temporal resolutions can be derived from these two during aggregation step, if needed.

Arguments are passed via command line as application arguments. Specifically, indices of the following attributes in the dataset:

* temporal - date/datetime
* spatial - latitude
* spatial - longitude

After temporal and spatial resolutions are done, columns which contain more than 80% null values are dropped from the dataset.

The preprocessed dataset is saved to the <code>./preprocess</code> directory for aggregation.


#### 2. aggregate.py
Three aggregations are baselined for each spatial/temporal group:
* Count of records
* Average of numeric attributes
* Unique count of categorical values

Below are the general steps for aggregation:

1. Preprocessed dataset is read in from the <code>./preprocess</code> directory. Schema and header are inferred.

2. We use some simple conditions to automatically identify the appropriate aggregations for each attribute. For example, if a column name contains "type", the a unique count of its categorical values is performed.

3. The dataset is grouped by temporal-spatial resolutions, then one of the three aggregations is applied to each of the columns.

The aggregated output is then written to the <code>./aggregates</code> directory for correlation calculation.


#### 3. correlate.py
Spearman correlation is calculated using the Correlation class from pyspark.ml.stat module. Spearman correlation is calculated for only 2 datasets at a time. Below are the general steps:

1. Two datasets are read in from the <code>./aggregates</code> directory and joined based on temporal and spatial resolution attributes.

2. Non-numeric features are excluded from spearman correlation calculation just as a safe measure, although the number of non-numeric features should be minimal now.

3. Joined data frame is then vectorized using VectorAssembler to prepare the data frame for correlation.

4. Correlation result is returned as a DenseMatrix. We turned the result matrix into a pandas data frame, added header and index names, and write the pandas data frame as a csv file.

Correlation results are saved to the <code>./correlations</code> folder.
