# # User-Defined Functions - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


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


# ## Exercises

# (1) Create a UDF that extracts the day of the week from a timestamp column.
# **Hint:** Use the
# [weekday](https://docs.python.org/2/library/datetime.html#datetime.datetime.weekday)
# method of the Python `datetime` class.  

# Define the Python function:
import datetime
def day_of_week(timestamp):
  return timestamp.weekday()

# Register the Python function as a UDF: 
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
day_of_week_udf = udf(day_of_week, returnType=IntegerType())

# Apply the UDF:
rides_clean \
  .select("date_time", day_of_week_udf("date_time")) \
  .show(5, truncate=False)


# (2) Use the UDF to compute the number of rides by day of week.

rides_clean \
  .withColumn("day_of_week", day_of_week_udf("date_time")) \
  .groupBy("day_of_week") \
  .count() \
  .orderBy("day_of_week") \
  .show()

# **Note:** Day 0 represents Monday.


# (3) Use the built-in function `dayofweek` to compute the number of rides by
# day of week.

from pyspark.sql.functions import dayofweek
rides_clean \
  .withColumn("dayofweek", dayofweek("date_time")) \
  .groupBy("dayofweek") \
  .count() \
  .orderBy("dayofweek") \
  .show()

# **Note:** Day 1 represents Sunday.


# ## Cleanup

# Stop the SparkSession:
spark.stop()
