# # Summarizing and Grouping DataFrames - Solutions

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

# Read the enhanced (joined) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")

# Since we will be querying the `rides` DataFrame many times, let us persist
# it in memory to improve performance:
rides.persist()


# ## Exercises

# (1) Who are DuoCar's top 10 riders in terms of number of rides taken?

from pyspark.sql.functions import col
rides \
  .filter(col("cancelled") == False) \
  .groupBy("rider_id") \
  .count() \
  .orderBy(col("count").desc()) \
  .show(10)

# (2) Who are DuoCar's top 10 drivers in terms of total distance driven?

from pyspark.sql.functions import sum
rides \
  .groupBy("driver_id") \
  .agg(sum("distance").alias("sum_distance")) \
  .orderBy(col("sum_distance").desc()) \
  .show(10)

# (3) Compute the distribution of cancelled rides.

rides.groupBy("cancelled").count().show()

# (4) Compute the distribution of ride star rating.
# When is the ride star rating missing?

rides.groupBy("star_rating").count().orderBy("star_rating").show()

# The star rating is missing when a ride is cancelled:
rides.crosstab("star_rating", "cancelled").orderBy("star_rating_cancelled").show()

# (5) Compute the average star rating for each level of car service.
# Is the star rating correlated with the level of car service?

rides.groupBy("service").mean("star_rating").show()


# ## Cleanup

# Unpersist the DataFrame:
rides.unpersist()

# Stop the SparkSession:
spark.stop()
