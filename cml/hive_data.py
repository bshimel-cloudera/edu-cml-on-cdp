# Script to make Hive tables from the clean and joined DuoCar data

# Copyright 2017 Cloudera, Inc.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import cml.data_v1 as cmldata
import sys

print("Executing: " + __file__)

CONNECTION_NAME = sys.argv[1]
HDFS_ROOT = sys.argv[2]

print("CONNECTION_NAME = " + CONNECTION_NAME)
print("HDFS_ROOT = " + HDFS_ROOT)

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Create duocar database:
try:
  spark.sql("DROP DATABASE IF EXISTS duocar CASCADE")
except:
  print("Error dropping duocar database.")
  
spark.sql("CREATE DATABASE duocar")
spark.sql("USE duocar")


# Create drivers table:
spark.sql("DROP TABLE IF EXISTS drivers")
drivers = spark.read.parquet(HDFS_ROOT + "/duocar/clean/drivers/")
drivers_fixed = drivers\
  .withColumn("birth_date", col("birth_date").cast("timestamp"))\
  .withColumn("start_date", col("start_date").cast("timestamp"))
drivers_fixed.write.format("hive").saveAsTable("drivers")


# Create riders table:
spark.sql("DROP TABLE IF EXISTS riders")
riders = spark.read.parquet(HDFS_ROOT + "/duocar/clean/riders/")
riders_fixed = riders\
  .withColumn("birth_date", col("birth_date").cast("timestamp"))\
  .withColumn("start_date", col("start_date").cast("timestamp"))
riders_fixed.write.format("hive").saveAsTable("riders")


# Create rides table:
spark.sql("DROP TABLE IF EXISTS rides")
rides = spark.read.parquet(HDFS_ROOT + "/duocar/clean/rides/")
rides.write.format("hive").saveAsTable("rides")


# Create ride_reviews table:
spark.sql("DROP TABLE IF EXISTS ride_reviews")
ride_reviews = spark.read.parquet(HDFS_ROOT + "/duocar/clean/ride_reviews/")
ride_reviews.write.format("hive").saveAsTable("ride_reviews")


# Create joined table:
spark.sql("DROP TABLE IF EXISTS joined")
joined = spark.read.parquet(HDFS_ROOT + "/duocar/joined/")
joined_fixed = joined\
	.withColumn("driver_birth_date", col("driver_birth_date").cast("timestamp"))\
	.withColumn("driver_start_date", col("driver_start_date").cast("timestamp"))\
	.withColumn("rider_birth_date", col("rider_birth_date").cast("timestamp"))\
	.withColumn("rider_start_date", col("rider_start_date").cast("timestamp"))
joined_fixed.write.format("hive").saveAsTable("joined")


spark.stop()
