# # Applying a scikit-learn Model to a Spark DataFrame

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# We might want to use a machine learning algorithm that is not supported by
# the Spark MLlib library.  In this case we can use our favorite Python package
# to develop our machine learning model on a sample of data (on a single
# machine) and then use a Spark UDF to apply the model to the full data (in our
# Hadoop environment).  In this module we use the scikit-learn package to
# rebuild the isotonic regression model we built earlier using Spark MLlib.  We
# then use a pandas UDF to apply the model to a Spark DataFrame.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ## Prepare the data using Spark

# We want to prepare our data and generate our features using Spark.
# Otherwise, we will have to replicate any changes that we make to our sample
# data on our full data before we can apply our machine learning model in
# Spark.

# ### Start a SparkSession

import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# ### Load the data

rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
rides.printSchema()

# ### Prepare the regression data

# Select the feature and the label and filter out the cancelled rides:
regression_data = rides.select("distance", "duration").where("cancelled = FALSE")


# ## Build a scikit-learn model

# We will use the [scikit-learn](https://scikit-learn.org/stable/) package to
# develop an [isotonic
# regression](https://en.wikipedia.org/wiki/Isotonic_regression) model.

# ### Select a sample of data and load it into a pandas DataFrame

modeling_data = regression_data \
  .sample(withReplacement=False, fraction=0.1, seed=12345) \
  .toPandas()

# ### Plot the data

modeling_data.plot(x="distance", y="duration", kind="scatter", color="orange")

# ### Specify the features and label

features = modeling_data['distance']
label = modeling_data['duration']

# ### Create train and test sets

from sklearn.model_selection import train_test_split
features_train, features_test, label_train, label_test = train_test_split(features, label, test_size=0.3, random_state=42)

# ### Specify and fit an isotonic regression model

from sklearn.isotonic import IsotonicRegression
ir = IsotonicRegression(out_of_bounds="clip")
ir.fit(features_train, label_train)

# **Note:** The `out_of_bounds` argument determines how feature values outside
# the range of training values are handled during prediction.

# ### Evaluate the isotonic regression model

from math import sqrt
from sklearn.metrics import mean_squared_error

# Apply the model to the train and test datasets:
label_train_predicted = ir.predict(features_train)
label_test_predicted = ir.predict(features_test)

# Compute the RMSE on the train and test datasets:
sqrt(mean_squared_error(label_train, label_train_predicted))
sqrt(mean_squared_error(label_test, label_test_predicted))

# **Note:** These RMSE values are consistent with our results using Spark
# MLlib.

# ### Plot the isotonic regression model

def plot_model():
  fig = plt.figure()
  plt.scatter(features_test, label_test, s=12, c="orange", alpha=0.25)
  # Use plt.plot rather than plt.step since sklearn does linear interpolation.
  plt.plot(ir.f_.x, ir.f_.y, color="black")
  plt.title("Isotonic Regression on Test Set")
  plt.xlabel("Distance (m)")
  plt.ylabel("Duration (s)")
plot_model()

# ### Save the model for later use in CML Native Workbench

# Use the pickle package to serialize the model on disk:
import pickle
with open("ir_model.pickle", "wb") as f:
  pickle.dump(ir, f)


# ## Apply the model using a Spark UDF

# We will use the more efficient pandas UDF, which processes multiple rows of
# data rather than a single row of data, to apply our model to a Spark
# DataFrame.  The Spark executors must have access to our model.  We can let
# Spark automatically *broadcast* the model to the executors or we can manually
# broadcast it.

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType

# ### Automatically broadcast the model to the executors

# Define and register our pandas UDF:
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def predict_udf(x):
  return pd.Series(ir.predict(x))

# Normally our UDF will take multiple columns representing multiple features
# (or a single column representing a feature vector) and it will reshape the
# input into a form appropriate for the specific model predict method.

# The Spark driver will distribute the *closure* of the Python function to the
# Spark executors.  The closure consists of the function and its environment.
# In this case the closure includes the `ir` instance and its `predict` method.

# Use the UDF to apply the model to our Spark DataFrame:
regression_data.withColumn("duration_predicted", predict_udf("distance")).show(5)

# **Note:** You will have to deal with null (missing) values before calling the
# UDF or handle them within the UDF.

# **Important:** This code will fail when running Spark via YARN if `pyarrow`
# is not installed on all the worker nodes.

# ### Manually broadcast the model to the executors

# Rather than let the Spark driver distribute our model, we can explicitly
# broadcast it to the Spark executors:
bc_ir_model = spark.sparkContext.broadcast(ir)

# Define and register our pandas UDF:
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def bc_predict_udf(x):
  return pd.Series(bc_ir_model.value.predict(x))

# Use the UDF to apply the model to our Spark DataFrame:
data_with_prediction = regression_data.withColumn("duration_predicted", bc_predict_udf("distance"))
data_with_prediction.show(5)

# ### Evaluate the model

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="duration_predicted", labelCol="duration", metricName="rmse")
evaluator.evaluate(data_with_prediction)


# ## Exercises

# None


# ## References

# [scikit-learn Documentation - IsotonicRegression
# class](https://scikit-learn.org/stable/modules/generated/sklearn.isotonic.IsotonicRegression.html?highlight=isotonic#sklearn-isotonic-isotonicregression)

# [Spark Python API - pandas_udf
# function](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.functions.pandas_udf)

# [Spark Programming Guide - Broadcast
# Variables](https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#broadcast-variables)


# ## Cleanup

spark.stop()
