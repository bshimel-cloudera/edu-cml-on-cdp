# # User-Defined Functions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to create and apply user-defined functions.


# ## User-Defined Functions

# * It is relatively easy to create and apply a user-defined function (UDF)
#   * Define a Python function that operates on a row of data
#   * Register the Python function as a UDF and specify the return type
#   * Apply the UDF as if it were a built-in function

# * Python and any required packages must be installed on the worker nodes
#   * It is possible to distribute required packages via Spark

# * Built-in functions are more efficient than user-defined functions
#   * Use built-in functions when available
#   * Create user-defined functions only when necessary

# * User-defined function are inefficient because of the following:
#   * A Python process must be started alongside each executor
#   * Data must be converted between Java and Python types
#   * Data must be transferred between the Java and Python processes

# * To improve the performance of a UDF:
#   * Use the [Apache Arrow](https://arrow.apache.org/) platform 
#   * Use a vectorized UDF (see the `pandas_udf` function)
#   * Rewrite the UDF in Scala or Java


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw ride data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)

# Cast `date_time` to a timestamp:
from pyspark.sql.functions import col
rides_clean = rides.withColumn("date_time", col("date_time").cast("timestamp"))


# ## Example 1: Hour of Day

# Define the Python function:
import datetime
def hour_of_day(timestamp):
  return timestamp.hour

# **Note:** The Spark `TimestampType` corresponds to Python `datetime.datetime`
# objects.

# Test the Python function:
dt = datetime.datetime(2017, 7, 21, 5, 51, 10)
hour_of_day(dt)

# Register the Python function as a UDF:
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
hour_of_day_udf = udf(hour_of_day, returnType=IntegerType())

# **Note:** We must explicitly specify the return type otherwise it defaults
# to `StringType`.

# Apply the UDF:
rides_clean \
  .select("date_time", hour_of_day_udf("date_time")) \
  .show(5, truncate=False)

# Use the UDF to compute the number of rides by hour of day:
rides_clean \
  .select(hour_of_day_udf("date_time").alias("hour_of_day")) \
  .groupBy("hour_of_day") \
  .count() \
  .orderBy("hour_of_day") \
  .show(25)


# ## Example 2: Great-Circle Distance

# The [great-circle
# distance](https://en.wikipedia.org/wiki/Great-circle_distance) is the
# shortest distance between two points on the surface of a sphere.  In this
# example we create a user-defined function to compute the [haversine
# approximation](https://en.wikipedia.org/wiki/Haversine_formula) to
# the great-circle distance between the ride origin and destination.

# Define the haversine function (based on the code at
# [rosettacode.org](http://rosettacode.org/wiki/Haversine_formula#Python)):
from math import radians, sin, cos, sqrt, asin
def haversine(lat1, lon1, lat2, lon2):
  """
  Return the haversine approximation to the great-circle distance between two
  points (in meters).
  """
  R = 6372.8 # Earth radius in kilometers
 
  dLat = radians(lat2 - lat1)
  dLon = radians(lon2 - lon1)

  lat1 = radians(lat1)
  lat2 = radians(lat2)
 
  a = sin(dLat / 2.0)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2.0)**2
  c = 2.0 * asin(sqrt(a))
 
  return R * c * 1000.0

# **Note:** We have made some minor changes to the code to make it integer
# proof.

# Test the Python function:
haversine(36.12, -86.67, 33.94, -118.40)  # = 2887259.9506071107:

# Register the Python function as a UDF:
from pyspark.sql.types import DoubleType
haversine_udf = udf(haversine, returnType=DoubleType())

# Apply the haversine UDF:
distances = rides \
  .withColumn("haversine_approximation", haversine_udf("origin_lat", "origin_lon", "dest_lat", "dest_lon")) \
  .select("distance", "haversine_approximation")
distances.show(5)

# We expect the haversine approximation to be less than the ride distance:
distances \
  .select((col("haversine_approximation") > col("distance")).alias("haversine > distance")) \
  .groupBy("haversine > distance") \
  .count() \
  .show()

# The null values correspond to cancelled rides:
rides.filter(col("cancelled") == 1).count()

# The true values reflect the fact that the haversine formula is only an
# approximation to the great-circle distance:
distances.filter(col("haversine_approximation") > col("distance")).show(5)


# ## Exercises

# (1) Create a UDF that extracts the day of the week from a timestamp column.
# **Hint:** Use the
# [weekday](https://docs.python.org/2/library/datetime.html#datetime.datetime.weekday)
# method of the Python `datetime` class.  

# (2) Use the UDF to compute the number of rides by day of week.

# (3) Use the built-in function `dayofweek` to compute the number of rides by day of week.


# ## References

# [Python API - datetime
# module](https://docs.python.org/2/library/datetime.html)

# [Spark Python API -
# pyspark.sql.functions.udf](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.udf)

# [Spark Python API -
# pyspark.sql.functions.pandas_udf](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

# [Cloudera Engineering Blog - Working with UDFs in Apache
# Spark](https://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/)

# [Cloudera Engineering Blog - Use your favorite Python library on PySpark
# cluster with Cloudera Data Science
# Workbench](https://blog.cloudera.com/blog/2017/04/use-your-favorite-python-library-on-pyspark-cluster-with-cloudera-data-science-workbench/)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
