# # Inspecting a Spark DataFrame - Solutions

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


# ## Exercises

# (1) Read the raw driver data into a Spark DataFrame called `drivers`.

drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)

# (2) Examine the inferred schema.  Do the data types seem appropriate?

drivers.printSchema()

# (3) Verify the integrity of the putative primary key `id`.

from pyspark.sql.functions import count, countDistinct
drivers.select(count("*"), count("id"), countDistinct("id")).show()

# (4) Inspect `birth_date`.  What data type did Spark infer?

drivers.select("birth_date").show(5)

# Spark inferred the data type to be a date AND time even though the column
# represents a pure date.  This is likely to ensure compatibility with Apache
# Impala, which does not support pure dates at this time.

# (5) Determine the unique values of `student`.  What type of variable do you
# think `student` is?

drivers.select("student").distinct().show()

# (6) Count the number of drivers by `vehicle_make`.  What is the most popular
# make?

drivers.groupBy("vehicle_make").count().show(n=30)

# (7) Compute basic summary statistics on the `rides` column.  How does the
# mean number of rides compare to the median?

drivers.describe("rides").show()
drivers.approxQuantile("rides", probabilities=[0.0, 0.25, 0.5, 0.75, 1.0], relativeError=1e-5)

# (8) **Bonus:** Inspect additional columns of the `drivers` DataFrame.

# (9) **Bonus:** Inspect the raw rider data.


# ## Cleanup

# Stop the SparkSession:
spark.stop()
