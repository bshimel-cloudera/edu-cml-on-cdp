# # Working with Machine Learning Pipelines - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# In the previous modules we established a workflow in which we loaded some
# data; preprocessed the data; extracted, transformed, and selected features;
# and fit and evaluated a machine learning model.  In this module we show how
# we can encapsulate this workflow into a [Spark MLlib
# Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#module-pyspark.ml)
# that we can reuse in our development process or production environment.


# ## Setup

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# ## Create a SparkSession

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

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined_all/")


# ## Create the train and test sets

# Create the train and test sets *before* specifying the pipeline:
(train, test) = rides.randomSplit([0.7, 0.3], 12345)


# ## Specify the pipeline stages

# A *Pipeline* is a sequence of stages that implement a data engineering or
# machine learning workflow.  Each stage in the pipeline is either a
# *Transformer* or an *Estimator*.  Recall that a
# [Transformer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Transformer)
# takes a DataFrame as input and returns a DataFrame as output.  Recall that an
# [Estimator](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Estimator)
# takes a DataFrame as input and returns a Transformer (e.g., model) as output.
# We begin by specifying the stages in our machine learning workflow.

# Filter out the cancelled rides:
from pyspark.ml.feature import SQLTransformer
filterer = SQLTransformer(statement="SELECT * FROM __THIS__ WHERE cancelled == 0")

# Generate the reviewed feature:
extractor = SQLTransformer(statement="SELECT *, review IS NOT NULL AS reviewed FROM __THIS__")

# Index `vehicle_color`:
from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="vehicle_color", outputCol="vehicle_color_indexed")

# Encode `vehicle_color_indexed`:
from pyspark.ml.feature import OneHotEncoderEstimator
encoder = OneHotEncoderEstimator(inputCols=["vehicle_color_indexed"], outputCols=["vehicle_color_encoded"])

# Select and assemble the features:
from pyspark.ml.feature import VectorAssembler
features = ["reviewed", "vehicle_year", "vehicle_color_encoded", "CloudCover"]
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Specify the estimator (i.e., classification algorithm):
from pyspark.ml.classification import RandomForestClassifier
classifier = RandomForestClassifier(featuresCol="features", labelCol="star_rating", seed=23451)
print(classifier.explainParams())

# Specify the hyperparameter grid:
from pyspark.ml.tuning import ParamGridBuilder
paramGrid = ParamGridBuilder() \
  .addGrid(classifier.maxDepth, [5, 10, 20]) \
  .addGrid(classifier.numTrees, [20, 50, 100]) \
  .addGrid(classifier.subsamplingRate, [0.5, 1.0]) \
  .build()

# Specify the evaluator:
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="star_rating", metricName="accuracy")

# **Note:** We are treating `star_rating` as a multiclass label.

# Specify the validator:
from pyspark.ml.tuning import TrainValidationSplit
validator = TrainValidationSplit(estimator=classifier, estimatorParamMaps=paramGrid, evaluator=evaluator, seed=34512)


# ## Specify the pipeline

# A
# [Pipeline](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.Pipeline)
# itself is an `Estimator`:
from pyspark.ml import Pipeline
stages = [filterer, extractor, indexer, encoder, assembler, validator]
pipeline = Pipeline(stages=stages)


# ## Fit the pipeline model

# The `fit` method produces a
# [PipelineModel](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.PipelineModel),
# which is a `Transformer`:
%time pipeline_model = pipeline.fit(train)


# ## Inspect the pipeline model

# Access the stages of a `PipelineModel` instance using the `stages` attribute:
pipeline_model.stages

# ### Inspect the string indexer

indexer_model = pipeline_model.stages[2]
type(indexer_model)
indexer_model.labels

# ### Inspect the validator model

validator_model = pipeline_model.stages[5]
type(validator_model)
validator_model.validationMetrics

# ### Inspect the best random forest classifier

best_model = validator_model.bestModel
type(best_model)

# Inspect the best hyperparameters:
validator_model.bestModel._java_obj.getMaxDepth()
validator_model.bestModel.getNumTrees
validator_model.bestModel._java_obj.getSubsamplingRate()

# **Note:** We have to access the values for `maxDepth` and `subsamplingRate`
# from the underlying Java object.

# Plot the feature importances:
def plot_feature_importances(fi):
  fi_array = fi.toArray()
  plt.figure()
  sns.barplot(list(range(len(fi_array))), fi_array)
  plt.title("Feature Importances")
  plt.xlabel("Feature")
  plt.ylabel("Importance")
plot_feature_importances(validator_model.bestModel.featureImportances)


# ## Save and load the pipeline model

# Save the pipeline model object to our local directory in HDFS:
pipeline_model.write().overwrite().save(S3_HOME + "/models/pipeline_model")

# **Note**: We can use Hue to explore the saved object.

# We can also use the following convenience method if we do not need to
# overwrite an existing model:
#```python
#pipeline_model.save(S3_HOME + "/models/pipeline_model")
#```

# Load the pipeline model object from our local directory in HDFS:
from pyspark.ml import PipelineModel
pipeline_model_loaded = PipelineModel.read().load(S3_HOME + "/models/pipeline_model")

# We can also use the following convenience method:
#```python
#pipeline_model_loaded = PipelineModel.load(S3_HOME + "/models/pipeline_model")
#```

# Save the underlying Java object to get around an issue with saving
# `TrainValidationSplitModel()` objects:
pipeline_model._to_java().write().overwrite().save(S3_HOME + "/models/pipeline_model")


# ## Apply the pipeline model

# Use the `transform` method to apply the `PipelineModel` to the test set:
classified = pipeline_model_loaded.transform(test)
classified.printSchema()


# ## Evaluate the pipeline model

# Generate a confusion matrix:
classified \
  .groupBy("prediction") \
  .pivot("star_rating") \
  .count() \
  .orderBy("prediction") \
  .fillna(0) \
  .show()

# Evaluate the random forest model:
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="star_rating", metricName="accuracy")
evaluator.evaluate(classified)

# Compare to the baseline prediction (always predict five-star rating):
from pyspark.sql.functions import lit
classified_with_baseline = classified.withColumn("prediction_baseline", lit(5.0))
evaluator.setPredictionCol("prediction_baseline").evaluate(classified_with_baseline)

# Our random forest classifier is doing no better than always predicting a
# five-star rating.  We can try to improve our model by adding more features,
# experimenting with additional hyperparameter combinations, and exploring
# other machine learning algorithms.


# ## Exercises

# (1) Import the
# [RFormula](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RFormula)
# class from the `pyspark.ml.feature` module.

from pyspark.ml.feature import RFormula

# (2) Create an instance of the `RFormula` class with the R formula
# `star_rating ~ reviewed + vehicle_year + vehicle_color`.

rformula = RFormula(formula = "star_rating ~ reviewed + vehicle_year + vehicle_color")

# (3) Specify a pipeline consisting of the `filterer`, `extractor`, and the
# RFormula instance specified above.

pipeline = Pipeline(stages=[filterer, extractor, rformula])

# (4) Fit the pipeline on the `train` DataFrame.

pipeline_model = pipeline.fit(train)

# (5) Use the `save` method to save the pipeline model to the
# `models/my_pipeline_model` directory in HDFS.

pipeline_model.write().overwrite().save(S3_HOME + "/models/my_pipeline_model")

# (6) Import the `PipelineModel` class from the `pyspark.ml` package.

from pyspark.ml import PipelineModel

# (7) Use the `load` method of the `PipelineModel` class to load the saved
# pipeline model.

pipeline_model_loaded = PipelineModel.load(S3_HOME + "/models/my_pipeline_model")
                                           
# (8) Apply the loaded pipeline model to the test set and examine the resulting
# DataFrame.

test_transformed = pipeline_model_loaded.transform(test)
test_transformed.printSchema()
test_transformed.select("features", "label").show(truncate=False)


# ## References

# [Spark Documentation - ML Pipelines](http://spark.apache.org/docs/latest/ml-pipeline.html)

# [Spark Python API - pyspark.ml package](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html)

# [Spark Python API - MLReader class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.util.MLReader)

# [Spark Python API - MLWriter class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.util.MLWriter)


# ## Stop the SparkSession

spark.stop()
