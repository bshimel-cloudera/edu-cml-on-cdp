# # Tuning Algorithm Hyperparameters Using Grid Search

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# Most machine learning algorithms have a set of user-specified parameters that
# govern the behavior of the algorithm.  These parameters are called
# *hyperparameters* to distinguish them from the model parameters such as the
# intercept and coefficients in linear and logistic regression.  In this module
# we show how to use grid search and cross validation in Spark MLlib to
# determine a reasonable regularization parameter for [$l1$ lasso linear
# regression](https://en.wikipedia.org/wiki/Lasso_%28statistics%29).


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# **Important:**  Run the `15_classify.py` script before loading the data.

# Read the modeling data from HDFS:
rides = spark.read.parquet(S3_HOME + "/data/modeling_data")
rides.show(5)


# ## Create train and test data

(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Requirements for hyperparameter tuning

# We need to specify four components to perform hyperparameter tuning using
# grid search:
# * Estimator (i.e. machine learning algorithm)
# * Hyperparameter grid
# * Evaluator
# * Validation method


# ## Specify the estimator

# In this example we will use $l1$ (lasso) linear regression as our estimator:
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol="features", labelCol="star_rating", elasticNetParam=1.0)

# Use the `explainParams` method to get the full list of hyperparameters:
print(lr.explainParams())

# Setting `elasticNetParam=1.0` corresponds to $l1$ (lasso) linear regression.
# We are interested in finding a reasonable value of `regParam`.


# ## Specify the hyperparameter grid

# Use the
# [ParamGridBuilder](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.ParamGridBuilder)
# class to specify a hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
regParamList = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
grid = ParamGridBuilder().addGrid(lr.regParam, regParamList).build()

# The resulting object is simply a list of parameter maps:
grid

# Rather than specify `elasticNetParam` in the `LinearRegression` constructor, we can specify it in our grid:
grid = ParamGridBuilder().baseOn({lr.elasticNetParam: 1.0}).addGrid(lr.regParam, regParamList).build()
grid


# ## Specify the evaluator

# In this case we will use
# [RegressionEvaluator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.RegressionEvaluator)
# as our evaluator and specify root-mean-squared error as the metric:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="star_rating", metricName="rmse")


# ## Tune the hyperparameters using holdout cross-validation

# In most cases, holdout cross-validation will be sufficient.  Use the
# [TrainValidationSplit](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplit)
# class to specify holdout cross-validation:
from pyspark.ml.tuning import TrainValidationSplit
tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, trainRatio=0.75, seed=54321)

# For each combination of the hyperparameters, the linear regression will be
# fit on a random %75 of the `train` DataFrame and evaluated on the remaining
# %25. 

# Use the `fit` method to find the best set of hyperparameters:
%time tvs_model = tvs.fit(train)

# The resulting model is an instance of the
# [TrainValidationSplitModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.TrainValidationSplitModel)
# class:
type(tvs_model)

# The cross-validation results are stored in the `validationMetrics` attribute:
tvs_model.validationMetrics

# These are the RMSE for each set of hyperparameters.  Smaller is better.

def plot_holdout_results(model):
  plt.plot(regParamList, model.validationMetrics)
  plt.title("Hyperparameter Tuning Results")
  plt.xlabel("Regularization Parameter")
  plt.ylabel("Validation Metric")
  plt.show()
plot_holdout_results(tvs_model)

# In this case the `bestModel` attribute is an instance of the
# [LinearRegressionModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.LinearRegressionModel)
# class:
type(tvs_model.bestModel)

# **Note:** The model is rerun on the entire train dataset using the best set of hyperparameters.

# The usual attributes and methods are available:
tvs_model.bestModel.intercept
tvs_model.bestModel.coefficients
tvs_model.bestModel.summary.rootMeanSquaredError
tvs_model.bestModel.evaluate(test).r2


# ## Tune the hyperparameters using k-fold cross-validation

# For small or noisy datasets, k-fold cross-validation may be more appropriate.
# Use the
# [CrossValidator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidator)
# class to specify the k-fold cross-validation:
from pyspark.ml.tuning import CrossValidator
cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3, seed=54321)
%time cv_model = cv.fit(train)

# The result is an instance of the
# [CrossValidatorModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.tuning.CrossValidatorModel)
# class:
type(cv_model)

# The cross-validation results are stored in the `avgMetrics` attribute:
cv_model.avgMetrics
def plot_kfold_results(model):
  plt.plot(regParamList, model.avgMetrics)
  plt.title("Hyperparameter Tuning Results")
  plt.xlabel("Regularization Parameter")
  plt.ylabel("Validation Metric")
  plt.show()
plot_kfold_results(cv_model)

# The `bestModel` attribute contains the model based on the best set of
# hyperparameters.  In this case, it is an instance of the
# `LinearRegressionModel` class:
type(cv_model.bestModel)

# Compute the performance of the performance of the best model on the test
# dataset:
cv_model.bestModel.evaluate(test).r2


# ## Exercises

# (1) Maybe our regularization parameters are too large.  Rerun the
# hyperparameter tuning with regularization parameters [0.0, 0.002, 0.004, 0.006,
# 0.008, 0.01].

# (2) Create a parameter grid that searches over `elasticNetParam` as well as
# `regParam`.


# ## References

# [Spark Documentation - Model Selection and hyperparameter
# tuning](http://spark.apache.org/docs/latest/ml-tuning.html)

# [Spark Python API - pyspark.ml.tuning
# module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.tuning)


# ## Stop the SparkSession

spark.stop()

