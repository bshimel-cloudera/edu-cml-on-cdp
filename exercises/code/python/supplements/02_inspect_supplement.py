# # Inspecting a Spark SQL DataFrame - Supplement

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Contents

# * Plotting with pandas
# * Automating the inspection process


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw ride data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)


# ## Plotting with pandas

# ### Bar chart
rides.groupby("service").count().toPandas().plot(x="service", y="count", kind="bar")

# ### Horizontal Bar chart
rides.groupby("service").count().toPandas().plot(x="service", y="count", kind="barh")

# ### Pie chart
rides.groupby("service").count().toPandas().plot(x="service", y="count", kind="pie")

# ### Histogram
rides.select("distance").toPandas().plot(kind="hist")

# ### Density plot
rides.select("distance").toPandas().plot(kind="density")

# ### Box plot
rides.select("distance").toPandas().plot(kind="box")

# ### Scatter plot
rides \
  .select("distance", "duration") \
  .toPandas() \
  .plot(x="distance", y="duration", kind="scatter")
  
# ### Hexagonal bin plot
rides \
  .select("distance", "duration") \
  .toPandas() \
  .plot(x="distance", y="duration", kind="hexbin")


# ## Automating the inspection process

# Count the number of missing (null) and distinct values for each column:

def inspect(df):
  from pyspark.sql.functions import col
  df.persist()
  print("%-20s\t%10s\t%10s" % ("Column", "# Missing", "# Distinct"))
  for c in df.columns:
    n_missing = df.filter(col(c).isNull()).count()
    n_distinct = df.select(c).distinct().count()
    print("%-20s\t%10s\t%10s" % (c, n_missing, n_distinct))
  df.unpersist()
inspect(rides)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
