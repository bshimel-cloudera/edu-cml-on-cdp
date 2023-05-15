# # Complex Types - Solutions

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
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)


# ## Exercises

# (1) Create an array called `home_array` that includes the driver's home
# latitude and longitude.

from pyspark.sql.functions import array

drivers_array = drivers \
  .withColumn("home_array", array("home_lat", "home_lon"))

drivers_array \
  .select("home_lat", "home_lon", "home_array") \
  .show(5, False)

# (2) Create a map called `name_map` that includes the driver's first and last
# name.

from pyspark.sql.functions import lit, create_map

drivers_map = drivers \
  .withColumn("name_map", create_map(lit("first"), "first_name", lit("last"), "last_name"))
  
drivers_map \
  .select("first_name", "last_name", "name_map") \
  .show(5, False)

# (3) Create a struct called `name_struct` that includes the driver's first
# and last name.

from pyspark.sql.functions import col, struct
drivers_struct = drivers \
  .withColumn("name_struct", struct(col("first_name").alias("first"), col("last_name").alias("last")))

from pyspark.sql.functions import to_json
drivers_struct \
  .select("first_name", "last_name", "name_struct", to_json("name_struct")) \
  .show(5, False)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
