# Spark Assignment
Airline on-time Performance Analysis

## Project Overview
According to the US Federal Aviation Administration (FAA), a flight is considered delayed when it arrives 15 minutes later than its scheduled time. 
With large amounts of flight performance data made publicly available, the assignment postulates that spark data analytics could help gain analytical insight into the causes, trends and comparisons associated with flight delays.
I intend to use only a smaller set of data for this assignment due to the git file size limit. However, the overall dataset for a year is close to 3 GB and hence this is an interesting analytical problem to be solved at scale using Spark analytics.

## Data Overview
Several data sources shall be used for this project. The primary dataset regarding on-time flight performance is from Bureau of Transportation Statistics (BTS). Statistical computing are few other sources of information to augment the primary data set.

| Dataset Source        | Description           | Format  | Size (rows, columns, file size)|
| ------------- |:-------------:| -----:|-----:|
| www.transtats.bts.gov      | This is the primary dataset: On time flight performance for 2019 | CSV |149033, 110, 39MB |
| http://stat-computing.org/dataexpo/2009/plane-data.csv      | Plane Data      |   CSV |5029, 9, 420 KB |
| http://stat-computing.org/dataexpo/2009/carriers.csv | Aircraft carrier      |    CSV |1491, 2, 44 KB |


## Analytical questions
All of the analytical questions are based on three dataframes that map to the three csv files described in the data overview section.<br/>
airlineDataDF - Represents the dataframe created from airline performance csv <br/>
carrierDataDF - Represents the dataframe created from carrier csv<br/>
planeDataDF - Represents the dataframe created from plane csv<br/>
Methods indicated in the analytical questions are from this spark API
https://spark.apache.org/docs/2.4.5/api/scala/index.html#org.apache.spark.sql.Dataset

There are about 118 columns in the original csv. However, not all is required for the analysis and most of them have null values.
We will have to do some data cleansing by dropping columns.

### What is the percentage delay types by total delays?
Step 1: A job is created to read the parquet file from the disk<br/>
Step 2: A job is run for the first count operation on the entire dataframe, this step took the longest<br/>
Step 3: Multiple jobs are created when we execute the count function on the filtered dataframes <br/><br/>

![DF Caching](data/problem1_with_cache.png)

### What is the min/max/average delays for an airline in a month and year?
Group by multiple columns such as airline type, month or year and then apply aggregation function to calculate min,max, and average.<br/>
I will make use of groupBy(..,..) delay type and month/year columns and agg(..) shall be used to generate min, max, average stats.

### Did privately managed airlines perform better than publicly traded ones?

I have utilized the UDF (user defined function) to generate a new column "ownership" based on a custom function airline_ownership(..) <br/>
A filter(..) operation is then applied to filter by ownership = 'Public' or ownership = 'Private'.
Finally, a count is done on the filtered dataset to compare delay counts.

### What delay type is most common at each airport?
Filter airlineDataDF by airport and then group by delay type to count delay types for each airport
I will make use of filter(..) by column and groupBy(..,..) function for this implementation.

### Did airlines with modernized fleet perform better?
Step 1: A job is created to read the parquet file from the disk<br/>
Step 2: A job is run for the first count operation after the filter query on the dataframe. The job has two tasks that run in parallel to complete the task<br/>
Step 3: Step 2 is repeated for the second dataframe<br/><br/>
![DF Caching](data/problem5_with_cache.png)


## Code Overview

### Test

`Assignment2Test.scala` is a test for the Spark driver. 


## Running Tests

### From Intellij

Right click on `Assignment2Test` and choose `Run 'Assignment2Test'`

### From the command line

On Unix systems, test can be run:

```shell script
$ ./sbt test
```

or on Windows systems:

```shell script
C:\> ./sbt.bat test
```

