# # Exploring and Visualizing DataFrames - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Setup

# Import useful packages, modules, classes, and functions:
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Load the enhanced ride data from HDFS:
rides_sdf = spark.read.parquet(S3_ROOT + "/duocar/joined_all")

# Create a random sample and load it into a pandas DataFrame:
rides_pdf = rides_sdf.sample(withReplacement=False, fraction=0.01, seed=12345).toPandas()


# ## Exercises

# (1) Look for variables that might help us predict ride duration.

# Does the ride duration depend on the day of the week?

# Produce a summary table:
result1 = rides_sdf \
  .groupBy(F.dayofweek("date_time").alias("day_of_week")) \
  .agg(F.count("duration"), F.mean("duration")) \
  .orderBy("day_of_week")
result1.show()

# Plot the summarized data:
result1_pdf = result1.toPandas()
sns.barplot(x="day_of_week", y="avg(duration)", data=result1_pdf)

# (2) Look for variables that might help us predict ride rating.

# Do elite vehicles get higher ratings?

# Produce a summary table:
result2 = rides_sdf \
  .groupBy("vehicle_elite") \
  .agg(F.count("star_rating"), F.mean("star_rating")) \
  .orderBy("vehicle_elite")
result2.show()

# Plot the summarized data:
result2_pdf = result2.toPandas()
sns.barplot(x="vehicle_elite", y="avg(star_rating)", data=result2_pdf)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
