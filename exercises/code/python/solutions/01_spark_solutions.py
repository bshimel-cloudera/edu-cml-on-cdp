# # Running a Spark Application from CML - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# Set environment variables
from env import S3_ROOT, S3_HOME, CONNECTION_NAME
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# ## Exercises

# (1) Create a new `SparkSession` and configure Spark to run locally with one
# thread.

# Use the cmldata module to create a data connection and spark session
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# (2) Read the raw driver data from HDFS.

drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)

# (3) Examine the schema of the drivers DataFrame.

drivers.printSchema()

# (4) Count the number of rows of the drivers DataFrame.

drivers.count()

# (5) Examine a few rows of the drivers DataFrame.

drivers.head(5)

# (6) **Bonus:** Repeat exercises (2)-(5) with the raw rider data.

riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)
riders.printSchema()
riders.count()
riders.head(5)

# (7) **Bonus:** Repeat exercises (2)-(5) with the raw ride review data.
# **Hint:** Verify the file format before reading the data.

reviews = spark.read.csv(S3_ROOT + "/duocar/raw/ride_reviews/", sep="\t", header=False, inferSchema=True)
reviews.printSchema()
reviews.count()
reviews.show(5)

# (8) Stop the SparkSession.

spark.stop()
