# sf_crime
Udacity Spark Streaming

Please find the Screenshots in `screenshots.zip` and note that I did add a screenshot of the SQL-Tab of the Spark-UI instead of the deprecated Streaming tab.

Answers to the questions:
1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Without any parameter tuning, the throuput of my application is 
```JSON
  "numInputRows" : 200,
  "inputRowsPerSecond" : 15.85791309863622,
  "processedRowsPerSecond" : 19.201228878648234,
  "durationMs" : {
    "addBatch" : 10236,
    "getBatch" : 0,
    "getEndOffset" : 1,
    "queryPlanning" : 77,
    "setOffsetRange" : 2,
    "triggerExecution" : 10416,
    "walCommit" : 52
  }
```
Reducing `spark.sql.shuffle.partitions` and `spark.default.parallelism` to `1` and  (since this "cluster" has only one executor) lead to
```JSON
  "numInputRows" : 200,
  "inputRowsPerSecond" : 14.726456078344746,
  "processedRowsPerSecond" : 293.6857562408223,
  "durationMs" : {
    "addBatch" : 445,
    "getBatch" : 0,
    "getEndOffset" : 1,
    "queryPlanning" : 72,
    "setOffsetRange" : 7,
    "triggerExecution" : 681,
    "walCommit" : 90
  }
```
Setting `spark.executor.memory` and `spark.driver.memory` to `4g` led to an further (small) speedup:
```JSON
  "numInputRows" : 200,
  "inputRowsPerSecond" : 15.006002400960385,
  "processedRowsPerSecond" : 300.7518796992481,
  "durationMs" : {
    "addBatch" : 444,
    "getBatch" : 1,
    "getEndOffset" : 0,
    "queryPlanning" : 65,
    "setOffsetRange" : 8,
    "triggerExecution" : 665,
    "walCommit" : 85
  }
```
Setting `spark.streaming.kafka.minRatePerPartition` to `400` led to 
```JSON
  "numInputRows" : 200,
  "inputRowsPerSecond" : 6.666888896296544,
  "processedRowsPerSecond" : 396.03960396039605,
  "durationMs" : {
    "addBatch" : 320,
    "getBatch" : 0,
    "getEndOffset" : 0,
    "queryPlanning" : 57,
    "setOffsetRange" : 3,
    "triggerExecution" : 505,
    "walCommit" : 70
  }
```
2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

As seen above, adapting the properties that are related to the actual cluster size were the most efficient.

After that, setting `spark.streaming.kafka.minRatePerPartition` was very efficient. I tried different values, but `400` seems to be a sweet spot.

As learned in the course, increasing `spark.executor.memory` and `spark.driver.memory` often have an effect, but since the data handled here is rather small, this effect was not too big.
