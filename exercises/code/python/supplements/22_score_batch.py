# # Batch Scoring using CML Jobs

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# Run this script as a CML Job to demonstrate batch scoring.

# **Important:** Run `22_deploy_udf.py` before running this script.


# ## Setup

import pickle
import pandas as pd

from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType


# ## Load the model

with open("ir_model.pickle", "rb") as f:
  ir_model = pickle.load(f)


# ## Define a Python wrapper function

def predict_duration(distance):
  """
  Predict ride distance (seconds) from ride duration (meters).

  Ride distance must be a pandas Series.
  
  Ride duration will be a pandas Series.
  
  Null values are mapped to null values.
  """ 
  # Identify valid values:
  valid_values = ~distance.isnull()
  
  # Propagate any null values to predicted duration:
  duration = distance.copy()
  
  # Compute predicted duration for valid values:
  if len(distance[valid_values] > 0):
    duration[valid_values] = ir_model.predict(distance[valid_values])
  
  # Return predicted duration as a pandas Series:
  return duration


# ## Perform batch scoring

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined_all")

# Register the Python wrapper function as a Spark UDF:
predict_duration_udf = pandas_udf(predict_duration, returnType=DoubleType(), functionType=PandasUDFType.SCALAR)

# Generate predictions:
predictions = rides.withColumn("predicted_duration", predict_duration_udf("distance"))

# Compute summary statistics on the predictions:
predictions.select("predicted_duration").summary().show()

# Save the predictions to HDFS:
predictions \
  .select("distance", "predicted_duration") \
  .write.csv(S3_HOME + "/data/predictions", mode="overwrite", sep="\t", header=True)

# Stop the SparkSession:
spark.stop()


# ## References

# [CML Documentation - Jobs](https://docs.cloudera.com/machine-learning/cloud/jobs-pipelines/topics/ml-creating-a-job-c.html)
