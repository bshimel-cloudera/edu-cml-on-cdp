# # Combining and Splitting DataFrames - Solutions

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

# Read the clean data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
drivers = spark.read.parquet(S3_ROOT + "/duocar/clean/drivers/")
reviews = spark.read.parquet(S3_ROOT + "/duocar/clean/ride_reviews/")


# ## Exercises

# (1) Join the `rides` DataFrame with the `reviews` DataFrame.  Keep only those
# rides that have a review.

# Count the number of reviews before the join:
reviews.count()

# Perform a right outer join:
rides_with_reviews = rides.join(reviews, rides.id == reviews.ride_id, "right_outer")

# Count the number of reviews after the join:
rides_with_reviews.count()

# Print the schema:
rides_with_reviews.printSchema()

# (2) How many drivers have not provided a ride?

# Get the driver IDs from `drivers` DataFrame:
id_from_drivers = drivers.select("id")

# Get the driver IDs from `rides` DataFrame:
id_from_rides = rides.select("driver_id").withColumnRenamed("driver_id", "id")

# Find lazy drivers using a left anti join:
lazy_drivers1 = id_from_drivers.join(id_from_rides, "id", "left_anti")
lazy_drivers1.count()
lazy_drivers1.orderBy("id").show(5)

# Find lazy drivers using a subtraction:
lazy_drivers2 = id_from_drivers.subtract(id_from_rides)
lazy_drivers2.count()
lazy_drivers2.orderBy("id").show(5)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
