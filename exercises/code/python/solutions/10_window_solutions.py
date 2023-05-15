# # Window Functions - Solutions

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

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Exercises

# (1) What is the average time between rides for each driver?

# Define the window specification:
from pyspark.sql.window import Window
driver_ws = Window.partitionBy("driver_id").orderBy("date_time")

# Generate date and time of previous ride, number of days between current ride
# and previous ride, and mean days between rides for each driver:
from pyspark.sql.functions import count, datediff, lag, mean
driver_ride_days = rides \
  .withColumn("date_time_previous", lag("date_time").over(driver_ws)) \
  .withColumn("days_between_rides", datediff("date_time", "date_time_previous")) \
  .groupBy("driver_id") \
  .agg(count("*").alias("num_rides"), mean("days_between_rides").alias("mean_days_between_rides"))

# List busy and not-so-busy drivers:
driver_ride_days.orderBy("mean_days_between_rides").show(10)
driver_ride_days.orderBy("mean_days_between_rides", ascending=False).show(10)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
