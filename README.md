Written in Python 3.5, in the Spark 2.2.0 and Hadoop 2.6.0 environment. This project also depends on a third-party Python module, named Shapely (version 1.6.0), for spatial resolution.

Reasons for choosing Spark over MapReduce and Spark SQL:
> performance improvement brought by in-memory computation
> even though Spark SQL provides the convenience of coding in SQL. We prefer flexibility to decompose the process into small units of Transformations and Actions. We get a simpler design and better code readability.

A high level process flow is summarized below in three main steps:
>    1. Preprocessing
>    2. Aggregation
>    3. Correlation Calculation

###1. Preprocess
The main functionalities of the preprocess Spark job are to parse the datasets into an appropriate format, and at the same time, perform temporal and spatial resolutions. In order to properly parse each row of the dataset, we created a header file for each dataset that contains the dataset header and corresponding default values. After datasets and their headers/default files are placed in the designated <code>/data</code> directory, the preprocess job can be started.

It begins by initializing dataset header and default values (which later will be broadcast variables). Both are kept in memory for parsing purpose. We need the header to filter our the first row (i.e. header) of the whole dataset, and possibly for output formatting (TBD). Then each row of the dataset is parsed against the default values to filter out invalid attributes. The reason for this is that there are rows with a variable number of null or null-equivalent attributes in each dataset.

The preprocess job also initializes two other objects (which later will also be broadcast variables) - polygons and region mappings. They are created and kept in memory for spatial resolution translation purpose. Currently our code supports spatial resolution at the zip code level.  As an example, we have a file (<code>zipcode.txt</code>) that contains zip codes in New York City and their corresponding bounding latitude and longitude. Each zip code and its coordinates are used to create a polygon, then added to a <code>(zip code, polygon)</code> mapping table in memory. Polygons are created with the help of the Shapely python module. After each row of the dataset is parsed, it's transformed by a spatial resolution function which uses the mapping table. More spatial resolutions will be added later in the project.

Temporal resolution is done using native Python modules. We currently have different temporal resolution functions for date-only and date-time, and support various formats. More granular temporal resolutions can be derived from these two during aggregation step.

#### Note:
Arguments are passed via SparkConf. Arguments other than the above are needed mainly for spatial and temporal resolution. Specifically, indices of the following attributes in the dataset:

* temporal - date/datetime
* temporal - time
* spatial - latitude
* spatial - longitude

Additionally, we need the desired spatial and temporal resolutions, such as spatial='zip', temporal='date' to be passed in via SparkConf.

#### Next goals:
* accept SparkConf arguments from CLI
* enable connection with Amazon S3 instances, where some of the datasets are hosted.
* possibly develop various input parsers to account for different formats in other datasets
* develop more spatial [, temporal] resolution functions

###2. Aggregation
With parsing and transformation done, data is ready for aggregation.

Each dataset is aggregated by key, using a specific scalar function. Currently, we count the number of valid attributes for each temporal and spatial resolution combination. Later in the project, we will implement more scalar functions, such as mean, max, and so on. The aggregated output is then written as a text file for correlation calculation.

Note: as of now, this step is built into the preprocess step. It will become its own module when more scalar functions are added.

#### Next goals:
* modularize aggregation from preprocessing
* develop more scalar functions.

###3. Correlation Calculation (Work in Progress)
Correlation calculation will be written as a separate Spark job that depends on the output of the Aggregation step.

#### Next goals:
* finish the current goal. :expressionless:
