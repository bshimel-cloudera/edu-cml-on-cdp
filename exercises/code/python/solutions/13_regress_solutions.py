# # Fitting and Evaluating Regression Models - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * A regression algorithm is a supervised learning algorithm.
#   * The inputs are called *features*
#   * The output is called the *label*

# * A regression model provides a prediction of a continuous numerical label.

# * Spark MLlib provides several regression algorithms:
#   * Linear Regression (with Elastic Net, Lasso, and Ridge Regression)
#   * Isotonic Regression
#   * Decision Tree
#   * Random Forest
#   * Gradient-Boosted Trees

# * Spark MLlib also provides regression algorithms for special types of
# continuous numerical labels:
#   * Generalized Regression
#   * Survival Regression

# * Spark MLlib requires the features to be assembled into a vector of doubles column.


# ## Scenario

# We will build a regression model to predict the duration of a ride from
# the distance of the ride.  We can then use this regression model in our
# mobile application to provide a real-time estimate of arrival time.
# In the demonstration, we will use linear regression.  In the exercise, we will
# use isotonic regression.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ## Start a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the (clean) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
rides.printSchema()


# ## Prepare the regression data

# Select the feature and the label and filter out the cancelled rides:
regression_data = rides.select("distance", "duration").filter("cancelled = 0")


# ## Plot the data

# Plot the ride duration versus the ride distance on a random sample of rides
# using pandas:
regression_data \
  .sample(withReplacement=False, fraction=0.1, seed=12345) \
  .toPandas() \
  .plot.scatter(x="distance", y="duration")


# ## Assemble the feature vector

# Import the `VectorAssembler` class from the `pyspark.ml.feature` module:
from pyspark.ml.feature import VectorAssembler

# Create an instance of the `VectorAssembler` class:
assembler = VectorAssembler(inputCols=["distance"], outputCol="features")

# Call the `transform` method to assemble the feature vector:
assembled = assembler.transform(regression_data)

# Examine the transformed DataFrame:
assembled.printSchema()
assembled.show(5)

# **Note:** The `VectorAssembler` instance is an example of a Spark MLlib
# `Transformer`.  It takes a DataFrame as input and returns a DataFrame as
# output via the `transform` method.


# ## Create a train and test set

# Use the `randomSplit` method to create random partitions of the data:
(train, test) = assembled.randomSplit(weights=[0.7, 0.3], seed=23451)

# We will fit the regression model on the `train` DataFrame
# and evaluate it on the `test` DataFrame.


# ## Specify a linear regression model

# Import the `LinearRegression` class from the `pyspark.ml.regression` module:
from pyspark.ml.regression import LinearRegression

# Create an instance of the `LinearRegression` class:
lr = LinearRegression(featuresCol="features", labelCol="duration")

# Examine additional hyperparameters;
print(lr.explainParams())


# ## Fit the linear regression model

# Call the `fit` method to fit the linear regression model on the `train` data:
lr_model = lr.fit(train)

# The `fit` method returns an instance of the `LinearRegressionModel` class:
type(lr_model)

# **Note:** The `LinearRegression` instance is an example of a Spark MLlib
# `Estimator`.  It takes a DataFrame as input and returns a `Transformer` as
# output via the `fit` method.


# ## Examine the model parameters

# Access the `intercept` and `coefficients` attributes to get the intercept and
# slope (for each feature) of the linear regression model:
lr_model.intercept
lr_model.coefficients

# The slope is stored as a `DenseVector`.  Call the `toArray` method to convert
# the DenseVector to a NumPy array:
lr_model.coefficients.toArray()


# ## Examine various model performance measures

# The `summary` attribute is an instance of the `LinearRegressionTrainingSummary` class:
type(lr_model.summary)

# It contains a number of model performance measures:
lr_model.summary.r2
lr_model.summary.rootMeanSquaredError


# ## Examine various model diagnostics

# The `summary` attribute also contains various model diagnostics:
lr_model.summary.coefficientStandardErrors
lr_model.summary.tValues
lr_model.summary.pValues

# **Important:** The first element of each list corresponds to the slope and
# the last element corresponds to the intercept.

# **Note:** The summary attribute contains additional useful information.
  

# ## Apply the linear regression model to the test data

# Use the `transform` method to apply the linear regression model to the `test`
# DataFrame:
predictions = lr_model.transform(test)

# The `transform` method adds a column to the DataFrame with the predicted
# label:
predictions.printSchema()
predictions.show(5)


# ## Evaluate the linear regression model on the test data

# Import the `RegressionEvaluator` class from the `pyspark.ml.evaluation` module:
from pyspark.ml.evaluation import RegressionEvaluator

# Create an instance of the `RegressionEvaluator` class:
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="duration", metricName="r2")

# Call the `explainParams` method to see other metrics:
print(evaluator.explainParams())

# Use the `evaluate` method to compute the metric on the `predictions` DataFrame:
evaluator.evaluate(predictions)

# Use the `setMetricName` method to change the metric:
evaluator.setMetricName("rmse").evaluate(predictions)

# **Note:** You can also use the `evaluate` method of the `LinearRegressionModel` class.


# ## Plot the linear regression model

def plot_lr_model():
  pdf = predictions.sample(withReplacement=False, fraction= 0.1, seed=34512).toPandas()
  plt.scatter("distance", "duration", data=pdf)
  plt.plot("distance", "prediction", color="black", data=pdf)
  plt.xlabel("Distance (m)")
  plt.ylabel("Duration (s)")
  plt.title("Linear Regression Model")
plot_lr_model()


# ## Exercises

# In the following exercises we use *isotonic regression* to fit a monotonic
# function to the data.

# (1)  Import the `IsotonicRegression` class from the regression module.

from pyspark.ml.regression import IsotonicRegression

# (2)  Create an instance of the `IsotonicRegression` class.  Use the same
# features and label that we used for our linear regression model.

ir = IsotonicRegression(featuresCol="features", labelCol="duration")
print(ir.explainParams())

# (3)  Fit the isotonic regression model on the train data.  Note that this
# will produce an instance of the `IsotonicRegressionModel` class.

ir_model = ir.fit(train)
type(ir_model)

# (4)  The model parameters are available in the `boundaries` and `predictions`
# attributes of the isotonic regression model.  Print these attributes.

ir_model.boundaries
ir_model.predictions

# (5) Apply the isotonic regression model to the train data using the
# `transform` method.

predictions_train = ir_model.transform(train)

# (6) Use the `RegressionEvaluator` to compute the RMSE on the train data.

evaluator.evaluate(predictions_train)

# (7) Repeat (5) and (6) on the test data.  Compare the RMSE for the isotonic
# regression model to the RMSE for the linear regression model.

predictions_test = ir_model.transform(test)
evaluator.evaluate(predictions_test)

# (8) Bonus: Plot the isotonic regression model.  In particular, plot the
# `predictions` attribute versus the `boundaries` attribute.  You must convert
# each attribute from a Spark `DenseVector` to a NumPy array using the
# `toArray` method.

def plot_ir_model(predictions):
  pdf = predictions.sample(withReplacement=False, fraction=0.1, seed=34512).toPandas()
  plt.scatter("distance", "duration", data=pdf)
  plt.plot(ir_model.boundaries.toArray(), ir_model.predictions.toArray(), color="black")
  plt.xlabel("Distance (m)")
  plt.ylabel("Duration (s)")
  plt.title("Isotonic Regression Model")
plot_ir_model(predictions_test)


# ## References

# [Spark Documentation - Classification and regression](https://spark.apache.org/docs/latest/ml-classification-regression.html)

# [Spark Python API - pyspark.ml.feature module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature)

# [Spark Python API - pyspark.ml.regression module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.regression)

# [Spark Python API - pyspark.ml.evaluation module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation)


# ## Stop the SparkSession

spark.stop()
