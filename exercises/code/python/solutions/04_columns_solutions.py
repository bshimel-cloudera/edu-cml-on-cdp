# # Transforming DataFrame Columns - Solutions

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

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# Read the raw data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)


# ## Exercises

# (1) Extract the hour of day and day of week from `rides.date_time`.

from pyspark.sql.functions import hour, dayofweek
rides \
  .withColumn("hour_of_day", hour("date_time")) \
  .withColumn("day_of_week", dayofweek("date_time")) \
  .select("date_time", "hour_of_day", "day_of_week") \
  .show(5)

# (2) Convert `rides.duration` from seconds to minutes.

from pyspark.sql.functions import col, round
rides \
  .withColumn("duration_in_minutes", round(col("duration") / 60, 1)) \
  .select("duration", "duration_in_minutes") \
  .show(5)

# (3) Convert `rides.cancelled` to a Boolean column.

# Using the `cast` method:
rides \
  .withColumn("cancelled", col("cancelled").cast("boolean")) \
  .select("cancelled") \
  .show(5)

# Using a Boolean expression:
rides \
  .withColumn("cancelled", col("cancelled") == 1) \
  .select("cancelled") \
  .show(5)

# (4) Create an integer column named `five_star_rating` that is 1.0 if the ride
# received a five-star rating and 0.0 otherwise.

# Using a Boolean expression and the `cast` method:
rides \
  .withColumn("five_star_rating", (col("star_rating") > 4.5).cast("double")) \
  .select("star_rating", "five_star_rating") \
  .show(10)

# Using the `when` function and the `when` and `otherwise` methods:
from pyspark.sql.functions import when
rides \
  .withColumn("five_star_rating", when(col("star_rating").isNull(), None).when(col("star_rating") == 5, 1.0).otherwise(0.0)) \
  .select("star_rating", "five_star_rating") \
  .show(10)

# **Note:** Beware of null values when generating new columns.

# (5) Create a new column containing the full name for each driver.

from pyspark.sql.functions import concat_ws
drivers \
  .withColumn("full_name", concat_ws(" ", "first_name", "last_name")) \
  .select("first_name", "last_name", "full_name") \
  .show(5)

# (6) Create a new column containing the average star rating for each driver.

drivers \
  .withColumn("star_rating", round(col("stars") / col("rides"), 2)) \
  .select("rides", "stars", "star_rating") \
  .show(5)

# (7) Find the rider names that are most similar to `Brian`.  **Hint:** Use the
# [Levenshtein](https://en.wikipedia.org/wiki/Levenshtein_distance) function.

from pyspark.sql.functions import lit, levenshtein
riders \
  .select("first_name") \
  .distinct() \
  .withColumn("distance", levenshtein(col("first_name"), lit("Brian"))) \
  .sort("distance") \
  .show()


# ## Cleanup

# Stop the SparkSession:
spark.stop()
