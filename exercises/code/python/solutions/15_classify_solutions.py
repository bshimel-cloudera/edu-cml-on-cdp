# # Fitting and Evaluating Classification Models - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * A classification algorithm is a supervised learning algorithm
#   * The inputs are called *features*
#   * The output is called the *label*

# * A classification model provides a prediction of a categorical label
#   * Binary classification - two categories
#   * Multiclass classification - three or more categories

# * Spark MLlib provides several classification algorithms:
#   * Logistic Regression (with Elastic Net, Lasso, and Ridge Regression)
#   * Decision Tree
#   * Random Forest
#   * Gradient-Boosted Trees
#   * Multilayer Perceptron (Neural Network)
#   * Linear Support Vector Machine (SVM)
#   * Naive Bayes

# * Spark MLlib also provides a meta-algorithm for constructing multiclass
# classification models from binary classification models:
#   * One-vs-Rest

# * Spark MLlib requires the features to be assembled into a vector of doubles column

# * Spark MLlib requires the label to be a zero-based index


# ## Scenario

# In this module we will model the star rating of a ride as a function of
# various attributes of the ride.  Rather than treat the star rating in its
# original form, we will create a binary label that is true if the rating is
# five stars and false otherwise.  We will use [logistic
# regression](https://en.wikipedia.org/wiki/Logistic_regression) to construct a
# binary classification model.  The general workflow will be similar for other
# classification algorithms, although the particular details will vary.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql.functions import col


# ## Start a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )


# ## Load the data

# Read the enhanced (joined) ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Preprocess the modeling data

# A cancelled ride does not have a star rating.  Use the
# [SQLTransformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.SQLTransformer)
# to filter out the cancelled rides:
from pyspark.ml.feature import SQLTransformer
filterer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0")
filtered = filterer.transform(rides)

# **Note:** `__THIS__` is a placeholder for the DataFrame passed into the `transform` method.


# ## Generate label

# We can treat `star_rating` as a continuous numerical label or an ordered
# categorical label:
filtered.groupBy("star_rating").count().orderBy("star_rating").show()

# Rather than try to predict each value, let us see if we can distinguish
# between five-star and non-five-star ratings.  We can use the
# [Binarizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Binarizer)
# to create our binary label:
from pyspark.ml.feature import Binarizer
converted = filtered.withColumn("star_rating", col("star_rating").cast("double"))
binarizer = Binarizer(inputCol="star_rating", outputCol="high_rating", threshold = 4.5)
labeled = binarizer.transform(converted)
labeled.crosstab("star_rating", "high_rating").show()

# **Note:** `Binarizer` does not like integer values, thus we had to convert to doubles.


# ## Extract, transform, and select features

# Create function to explore features:
def explore(df, feature, label, plot=True):
  from pyspark.sql.functions import count, mean
  aggregated = df.groupby(feature).agg(count(label), mean(label)).orderBy(feature)
  aggregated.show()
  if plot == True:
    pdf = aggregated.toPandas()
    pdf.plot.bar(x=pdf.columns[0], y=pdf.columns[2], capsize=5)

# **Feature 1:** Did the rider review the ride?
engineered1 = labeled.withColumn("reviewed", col("review").isNotNull().cast("int"))
explore(engineered1, "reviewed", "high_rating")

# **Note:** The `avg(high_rating)` gives the observed fraction of a high ratings.

# **Feature 2:** Does the year of the vehicle matter?
explore(labeled, "vehicle_year", "high_rating")

# **Note:** The rider is more likely to give a high rating when the car is
# newer.  We will treat this variable as a continuous feature.

# **Feature 3:** What about the color of the vehicle?
explore(labeled, "vehicle_color", "high_rating")

# **Note:** The rider is more likely to give a high rating if the car is
# black and less likely to give a high rating if the car is yellow.

# The classification algorithms in Spark MLlib do not accept categorical
# features in this form, so let us convert `vehicle_color` to a set of dummy
# variables. First, we use
# [StringIndexer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StringIndexer)
# to convert the string codes to numeric codes:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_indexed")
indexer_model = indexer.fit(engineered1)
list(enumerate(indexer_model.labels))
indexed = indexer_model.transform(engineered1)
indexed.select("vehicle_color", "vehicle_color_indexed").show(5)

# Then we use
# [OneHotEncoderEstimator](https://spark.apache.org/docs/2.4.8/api/python/pyspark.ml.html?highlight=onehotencoderestimator#pyspark.ml.feature.OneHotEncoderEstimator)
# to generate a set of dummy variables:
from pyspark.ml.feature import OneHotEncoderEstimator
encoder = OneHotEncoderEstimator(inputCols=["vehicle_color_indexed"], outputCols=["vehicle_color_encoded"])
encoder_model = encoder.fit(indexed)
encoded = encoder_model.transform(indexed)
encoded.select("vehicle_color", "vehicle_color_indexed", "vehicle_color_encoded").show(5)

# **Note:** `vehicle_color_encoded` is stored as a `SparseVector`.

# Now we can (manually) select our features and label:
selected = encoded.select("reviewed", "vehicle_year", "vehicle_color_encoded", "star_rating", "high_rating")
features = ["reviewed", "vehicle_year", "vehicle_color_encoded"]

# The machine learning algorithms in Spark MLlib expect the features to be
# collected into a single column, so we use
# [VectorAssembler](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.VectorAssembler)
# to assemble our feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=features, outputCol="features")
assembled = assembler.transform(selected)
assembled.head(5)

# **Note:** `features` is stored as a `SparseVector`.

# Save data for subsequent modules:
assembled.write.parquet(S3_HOME + "/data/modeling_data", mode="overwrite")

# **Note:** We are saving the data to our user directory in HDFS.


# ## Create train and test sets

# We will fit our model on the train DataFrame and evaluate our model on the
# test DataFrame:
(train, test) = assembled.randomSplit([0.7, 0.3], 12345)

# **Important:**  Weights must be doubles.


# ## Specify a logistic regression model

# Use the
# [LogisticRegression](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegression)
# class to specify a logistic regression model:
from pyspark.ml.classification import LogisticRegression
log_reg = LogisticRegression(featuresCol="features", labelCol="high_rating")

# Use the `explainParams` method to get a full list of hyperparameters:
print(log_reg.explainParams())


# ## Fit the logistic regression model

# Use the `fit` method to fit the logistic regression model on the train DataFrame:
log_reg_model = log_reg.fit(train)

# The result is an instance of the
# [LogisticRegressionModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.LogisticRegressionModel)
# class:
type(log_reg_model)


# ## Examine the logistic regression model

# The model parameters are stored in the `intercept` and `coefficients` attributes:
log_reg_model.intercept
log_reg_model.coefficients

# The `summary` attribute is an instance of the
# [BinaryLogisticRegressionTrainingSummary](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.BinaryLogisticRegressionTrainingSummary)
# class:
type(log_reg_model.summary)

# We can query the iteration history:
log_reg_model.summary.totalIterations
log_reg_model.summary.objectiveHistory

# and plot it too:
def plot_iterations(summary):
  plt.plot(summary.objectiveHistory)
  plt.title("Training Summary")
  plt.xlabel("Iteration")
  plt.ylabel("Objective Function")
  plt.show()

plot_iterations(log_reg_model.summary)

# We can also query the model performance, in this case, the area under the ROC curve:
log_reg_model.summary.areaUnderROC

# and plot the ROC curve:
log_reg_model.summary.roc.show(5)

def plot_roc_curve(summary):
  roc_curve = summary.roc.toPandas()
  plt.plot(roc_curve["FPR"], roc_curve["FPR"], "k")
  plt.plot(roc_curve["FPR"], roc_curve["TPR"])
  plt.title("ROC Area: %s" % summary.areaUnderROC)
  plt.xlabel("False Positive Rate")
  plt.ylabel("True Positive Rate")
  plt.show()

plot_roc_curve(log_reg_model.summary)


# ## Evaluate model performance on the test set

# We have been assessing the model performance on the train DataFrame.  We
# really want to assess it on the test DataFrame.

# **Method 1:** Use the `evaluate` method of the `LogisticRegressionModel` class

test_summary = log_reg_model.evaluate(test)

# The result is an instance of the
# [BinaryLogisticRegressionSummary](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.classification.BinaryLogisticRegressionSummary)
# class:
type(test_summary)

# It has attributes similar to those of the
# `BinaryLogisticRegressionTrainingSummary` class:
test_summary.areaUnderROC
plot_roc_curve(test_summary)

# **Method 2:** Use the `evaluate` method of the
# [BinaryClassificationEvaluator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.evaluation.BinaryClassificationEvaluator)
# class

# Generate predictions on the test DataFrame:
test_with_prediction = log_reg_model.transform(test)
test_with_prediction.show(5)

# **Note:** The resulting DataFrame includes three types of predictions.  The
# `rawPrediction` is a vector of log-odds, `prediction` is a vector or
# probabilities `prediction` is the predicted class based on the probability
# vector.

# Create an instance of `BinaryClassificationEvaluator` class:
from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol="high_rating", metricName="areaUnderROC")
print(evaluator.explainParams())
evaluator.evaluate(test_with_prediction)

# Evaluate using another metric:
evaluator.setMetricName("areaUnderPR").evaluate(test_with_prediction)


# ## Exercises

# In the exercises we add another feature to the classification model and
# determine if it improves the model performance.

# (1) Consider the `encoded` DataFrame.  Use the `explore` function to
# determine if `vehicle_noir` is a promising feature.

explore(encoded, "vehicle_noir", "high_rating")

# (2) Reassemble the feature vector and include `vehicle_noir`.

features = ["reviewed", "vehicle_year", "vehicle_color_encoded", "vehicle_noir"]
assembler = VectorAssembler(inputCols=features, outputCol="features")
assembled = assembler.transform(encoded)

# (3) Create new train and test datasets.

(train, test) = assembled.randomSplit([0.7, 0.3], 23451)

# (4) Refit the logistic regression model on the train dataset.

log_reg_model = log_reg.fit(train)

# (5) Apply the refit logistic model to the test dataset.

predictions = log_reg_model.transform(test)

# (6) Compute the AUC on the test dataset.

evaluator.setMetricName("areaUnderROC").evaluate(predictions)

# (7) We committed a cardinal sin of machine learning above.  What was it?

# We assessed the potential of `vehicle_noir` on all the data, which includes
# our test dataset.  This compromises the integrity of the test dataset.  Spark
# MLlib Pipelines will allow us to address this situation.


# ## References

# [Spark Documentation - Classification and regression](https://spark.apache.org/docs/latest/ml-classification-regression.html)

# [Spark Python API - pyspark.ml.feature module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.feature)

# [Spark Python API - pyspark.ml.classification module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.classification)

# [Spark Python API - pyspark.ml.evaluation module](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml.evaluation)


# ## Stop the SparkSession 

spark.stop()
