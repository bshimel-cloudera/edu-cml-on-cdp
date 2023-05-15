# # Transforming DataFrames - Supplement

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Contents

# * Selecting columns using Python list comprehension
# * Replacing valid values with null values


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw data from HDFS:

rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)


# ## Selecting columns using Python list comprehension

# Select the first three columns:
cols = riders.columns[0:3]
riders.select(cols).show(5)

# Select a subset of columns:
cols = [riders.columns[i] for i in [0, 3, 4]]
riders.select(cols).show(5)

# Select all string columns:
cols = [x[0] for x in riders.dtypes if x[1] == 'string']
riders.select(cols).show(5)


# ## Replacing valid values with null values

# Consider the following DataFrame:
df = spark.createDataFrame([(-9, ), (0, ), (1, ), (-9, ), (1, ), (0, )], ["value"])
df.show()

# Suppose that the value `-9` represents a missing value.  Then use the
# `replace` method to change these values to null values:
df.replace(-9, None).show()


# ## Cleanup

# Stop the SparkSession:
spark.stop()
