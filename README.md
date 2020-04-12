# Spark Assignment

## Project Overview
According to the US Federal Aviation Administration (FAA), a flight is considered delayed when it arrives 15 minutes later than its scheduled time. Unfortunately, since there are no clear guidelines on how and when passengers get compensated in the event of delays, it is left to the discretion of the respective airlines on how they deal with it. With large amounts of flight performance data made publicly available, the article postulates that data analytics could help address such challenges

## Data Overview
Several data sources shall be used for this project. The primary dataset regarding on-time flight performance is from Bureau of Transportation Statistics (BTS). Yahoo finance, Kaggle, and Statistical computing are few other sources of information to augment the primary data set.

| Dataset Source        | Description           | Format  | Size (rows, columns, file size)|
| ------------- |:-------------:| -----:|-----:|
| www.transtats.bts.gov      | This is the primary dataset: On time flight performance for 2017 | CSV |5475253, 110, 2.38 GB |
| http://stat-computing.org/dataexpo/2009/plane-data.csv      | Plane Data      |   CSV |5029, 9, 420 KB |
| http://stat-computing.org/dataexpo/2009/carriers.csv | Aircraft carrier      |    CSV |1491, 2, 44 KB |


## Analytical questions

### What is the percentage delay types by total delays
### What is the min/max/average delays for an airline in a month and year
### Were there any specific airport with maximum delays on a given day
### What is the min/max and average time between delays by delay type on a given day
### What delay type is most common at each airport


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

