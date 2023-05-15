# # Transforming DataFrames - Solutions

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

# (1) Replace the missing values in `rides.service` with the string `Car`.

rides.select("service").distinct().show()
rides_filled = rides.fillna("Car", subset=["service"])
rides_filled.select("service").distinct().show()

# (2) Rename `rides.cancelled` to `rides.canceled`.

rides_renamed = rides.withColumnRenamed("cancelled", "canceled")
rides_renamed.printSchema()

# (3) Sort the `rides` DataFrame in descending order with respect to
# `driver_id` and ascending order with respect to `date_time`.

rides_sorted = rides.sort(rides.driver_id.desc(), "date_time")
rides_sorted.select("driver_id", "date_time").show()

# (4) Create an approximate 20% random sample of the `rides` DataFrame.

rides.count()
rides_sampled = rides.sample(withReplacement=False, fraction=0.2, seed=31416)
rides_sampled.count()

# (5) Remove the driver's name from the `drivers` DataFrame.

drivers_fixed = drivers.drop("first_name", "last_name")
drivers_fixed.printSchema()

# (6) How many drivers have signed up?  How many female drivers have signed up?
# How many non-white, female drivers have signed up?

drivers.count()
drivers.filter(drivers.sex == "female").count()
drivers.filter(drivers.ethnicity != "White").filter(drivers.sex == "female").count()
drivers.filter(((drivers.ethnicity != "White") | (drivers.ethnicity.isNull())) & (drivers.sex == "female")).count()


# ## Cleanup

# Stop the SparkSession:
spark.stop()
