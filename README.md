Written in Python 3.5, in the Spark 2.2.0 and Hadoop 2.6.0 environment. This project also depends on a third-party Python module, named Shapely (version 1.6.0), for spatial resolution.

A high level process flow is summarized below in three main steps:
>    1. Preprocessing
>    2. Aggregation
>    3. Correlation Calculation

##### Preprocess
The main functionalities of the preprocess Spark job are to parse the datasets into an appropriate format, and at the same time, perform temporal and spatial resolutions. In order to properly parse each row of the dataset, we created a header file for each dataset that contains the dataset header and corresponding default values. After datasets and their headers/default files are placed in the designated <code>/data</code> directory, the preprocess job can be started.

It begins by initializing dataset header and default values (which later will be broadcast variables). Both are kept in memory for parsing purpose. We need the header to filter our the first row (i.e. header) of the whole dataset, and possibly for output formatting (TBD). Then each row of the dataset is parsed against the default values to filter out invalid attributes. The reason for this is that there are rows with a variable number of null or null-equivalent attributes in each dataset.

The preprocess job also intializes two other objects (which later will also be broadcast variables) - polygons and region mappings. They are created and kept in memory for spatial resolution translation purpose. Currently our code supports spatial resolution at the zip code level.  As an example, we have a file (zipcode.txt) that contains zip codes in New York City and their corresponding bounding latitude and longitude. Each zip code and its coordinates are used to create a polygon, then added to a (zip code, polygon) mapping table in memory. Polygons are created with the help of the Shapely python module. After each row of the dataset is parsed, it's transformed by a spatial resolution function which uses the mapping table. More spatial resolutions will be added later in the project.

Temporal resolution is done using native Python modules. We currently have different temporal resolution functions for date-only and date-time, and support various formats. More granular temporal resolutions can be derived from these two during aggregation step.

With parsing and transformation done, data is ready for aggregation.

##### Aggregation
Each dataset is aggregated by key, using a specific scalar function. Currently, we count the number of valid attributes for each temporal and spatial resolution combination. Later in the project, we will implement more scalar functions, such as mean, max, and so on. The aggregated output is then written as a text file for correlation calculation.

Note: as of now, this step is built into the preprocess step. It will become its own module when more scalar functions are added.

##### Correlation Calculation (Work in Progress)
Correlation calculation will be written as a separate Spark job that depends on the output of the Aggregation step.
