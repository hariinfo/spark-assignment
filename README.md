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
Filter airlineDataDF by delay type and count by each delay type and then use the count of total delays to arrive at the percentage breakdown. <br/>
I will make use of filter(..) by column and count() functions of dataframe for this implementation.

### What is the min/max/average delays for an airline in a month and year?
Group by multiple columns such as airline type, month or year and then apply aggregation function to calculate min,max, and average.<br/>
I will make use of groupBy(..,..) delay type and month/year columns and agg(..) shall be used to generate min, max, average stats.

### Did privately managed airlines perform better than publicly traded ones?
Reporting_Airline from airline performance dataframe should be used to join with carriers dataframe.
Introduce a new column with a flag to indicate private/public airline.
Group the data based on the airline code and ownership type (public/private)
Generate the ratio of delays by public vs privately traded airlines.

withColumn(..) shall be used to add a new column based on certain logic to determine public/private airline
groupBy(..,..) shall be used to group the records based on multiple columns and finally
agg(..) shall be used to generate min, max, average stats.

### What delay type is most common at each airport?
Filter airlineDataDF by airport and then group by delay type to count delay types for each airport
I will make use of filter(..) by column and groupBy(..,..) function for this implementation.

### Did airlines with modernized fleet perform better?
Aircraft tail number (Tail_Number) should be used to join the two data sets across airline performance and plane-data.
The combined dataframe represents the airline performance information along with the fleet details.
I will have to come up with a classification based on the aircraft manufacturing year to categorize new vs old fleet before performing analytics
filter(..) - To filter the modernized fleet and then use join(..) to combine the dataframes. 

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

