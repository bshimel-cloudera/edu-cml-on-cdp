# # Summarizing and Grouping DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to summarize, group, and pivot data in a DataFrame.

# * Summarizing data with aggregate functions
# * Grouping data
# * Pivoting data


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the enhanced (joined) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")

# Since we will be querying the `rides` DataFrame many times, let us persist
# it in memory to improve performance:
rides.persist()


# ## Summarizing data with aggregate functions

# Spark provides a number of summarization (aggregate) functions.  For example,
# the `describe` method provides basic summary statistics:
rides.describe("distance").show()

# Use the `count`, `countDistinct`, and `approx_count_distinct` functions to
# compute various column counts:
from pyspark.sql.functions import count, countDistinct, approx_count_distinct
rides.select(count("*"), count("distance"), countDistinct("distance"), approx_count_distinct("distance")).show()

# **Note:** The `count` function returns the number of rows with non-null values.

# **Note:** Use `count(lit(1))` rather than `count(1)` as an alternative to `count("*")`.

# The `agg` method returns the same results and can be applied to grouped data:
rides.agg(count("*"), count("distance"), countDistinct("distance"), approx_count_distinct("distance")).show()

# Use the `sum` and `sumDistinct` functions to compute various column sums:
from pyspark.sql.functions import sum, sumDistinct
rides.agg(sum("distance"), sumDistinct("distance")).show()

# **Question:** When would one use the `sumDistinct` function?

# Spark SQL provides a number of summary statistics:
from pyspark.sql.functions import mean, stddev, variance, skewness, kurtosis
rides.agg(mean("distance"), stddev("distance"), variance("distance"), skewness("distance"), kurtosis("distance")).show()

# **Note:** `mean` is an alias for `avg`, `stddev` is an alias for the sample
# standard deviation `stddev_samp`, and `variance` is an alias for the sample
# variance `var_samp`.  The population standard deviation and population
# variance are available via `stddev_pop` and `var_pop`, respectively.

# Use the `min` and `max` functions to compute the minimum and maximum, respectively:
from pyspark.sql.functions import min, max
rides.agg(min("distance"), max("distance")).show()

# Use the `first` and `last` functions to compute the first and last values, respectively:
from pyspark.sql.functions import first, last
rides \
  .orderBy("distance") \
  .agg(first("distance", ignorenulls=False), last("distance", ignorenulls=False)) \
  .show()

# **Note:** Null values sort before valid numerical values.

# Use the `corr`, `covar_samp`, or `covar_pop` functions to measure the linear
# association between two columns:
from pyspark.sql.functions import corr, covar_samp, covar_pop
rides \
  .agg(corr("distance", "duration"), covar_samp("distance", "duration"), covar_pop("distance", "duration")) \
  .show()

# The `collect_list` and `collect_set` functions return a column of array type:
from pyspark.sql.functions import collect_list, collect_set
rides.agg(collect_set("service")).show(truncate=False)

# **Note:** `collect_list` does not remove duplicates and will return a very
# long array in this case.


# ## Grouping data

# Use the `agg` method with the `groupBy` (or `groupby`) method to refine your
# analysis:
rides \
  .groupBy("rider_student") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .show()

# You can use more than one column in the `groupBy` method:
rides \
  .groupBy("rider_student", "service") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()

# Use the `rollup` method to get some subtotals:
rides \
  .rollup("rider_student", "service") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()

# Use the `grouping` function to identify grouped rows:
from pyspark.sql.functions import grouping
rides \
  .rollup("rider_student", "service") \
  .agg(grouping("rider_student"), grouping("service"), count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()

# Use the `cube` method to get all subtotals:
rides \
  .cube("rider_student", "service") \
  .agg(count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()

# Use the `grouping_id` function to identify grouped rows:
from pyspark.sql.functions import grouping_id
rides \
  .cube("rider_student", "service") \
  .agg(grouping_id("rider_student", "service"), count("*"), count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student", "service") \
  .show()


# ## Pivoting data

# The following use case is common:
rides.groupBy("rider_student", "service").count().orderBy("rider_student", "service").show()

# The `crosstab` method can be used to present this result in a pivot table:
rides.crosstab("rider_student", "service").show()

# We can also use the `pivot` method to produce a cross-tabulation:
rides.groupBy("rider_student").pivot("service").count().show()

# We can also perform other aggregations:
rides.groupBy("rider_student").pivot("service").mean("distance").show()

rides.groupBy("rider_student").pivot("service").agg(mean("distance")).show()

# You can explicitly choose the values that are pivoted to columns:
rides.groupBy("rider_student").pivot("service", ["Car", "Grand"]).agg(mean("distance")).show()

# Additional aggregation functions produce additional columns:
rides.groupBy("rider_student").pivot("service", ["Car"]).agg(count("distance"), mean("distance")).show()


# ## Exercises

# (1) Who are DuoCar's top 10 riders in terms of number of rides taken?

# (2) Who are DuoCar's top 10 drivers in terms of total distance driven?

# (3) Compute the distribution of cancelled rides.

# (4) Compute the distribution of ride star rating.
# When is the ride star rating missing?

# (5) Compute the average star rating for each level of car service.
# Is the star rating correlated with the level of car service?


# ## References

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)

# [Spark Python API - pyspark.sql.GroupedData
# class](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData)


# ## Cleanup

# Unpersist the DataFrame:
rides.unpersist()

# Stop the SparkSession:
spark.stop()
