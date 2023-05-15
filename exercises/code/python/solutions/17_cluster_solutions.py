# # Fitting and Evaluating Clustering Models - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * Clustering algorithms are unsupervised learning algorithms

# * A clustering model groups observations that are similar in some sense based
# on a feature vector

# * The number of clusters is a hyperparameter

# * A common use case for clustering is customer segmentation

# * Clustering is as much an art as a science

# * Spark MLlib provides a few clustering algorithms:
#   * K-means
#   * Bisecting K-means
#   * Gaussian mixture model
#   * Latent Dirichlet allocation


# ## Scenario

# In this demonstration we use a Gaussian mixture model to cluster the student
# riders by their home latitude and longitude.


# ## Setup

# Import useful packages, modules, classes, and functions:

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium

from pyspark.sql.functions import col


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the clean rider data from HDFS:
riders = spark.read.parquet(S3_ROOT + "/duocar/clean/riders/")


# ## Preprocess the data

# Select the student riders:
students = riders.filter(col("student") == True)


# ## Print and plot the home coordinates

# Print the home coordinates:
students.select("home_lat", "home_lon").show(10)

# Plot the home coordinates:
def plot_data(df):
  # Create a map of Fargo, North Dakota, USA:
  m = folium.Map(location=[46.8772222, -96.7894444], zoom_start=13)
  # Add the home coordinates:
  rows = df.select("home_lat", "home_lon").collect()
  for row in rows:
    folium.CircleMarker(location=[row["home_lat"], row["home_lon"]], radius=2, fill=True).add_to(m)
  # Return the map:
  return(m)
plot_data(students)


# ## Extract, transform, and select the features

# We would normally scale our features to have the same units using a feature
# transformation such as the `StandardScaler`.  However, since latitude and
# longitude are already in similar scales, we can proceed.

# Select home latitude and longitude as the features:
selected = ["home_lat", "home_lon"]

# Assemble the feature vector:
from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=selected, outputCol="features")
assembled = assembler.transform(students)


# ## Fit a Gaussian mixture model

# Specify a Gaussian mixture model with two clusters:
from pyspark.ml.clustering import GaussianMixture
gm = GaussianMixture(featuresCol="features", k=2, seed=12345)

# Examine the hyperparameters:
print(gm.explainParams())

# Fit the Gaussian mixture model:
gmm = gm.fit(assembled)
type(gmm)


# ## Examine the Gaussian mixture model

# Examine the mixing weights:
gmm.weights

# Examine the (multivariate) Gaussian distributions:
gmm.gaussiansDF.head(5)

# Examine the model summary:
gmm.hasSummary

# Examine the cluster sizes:
gmm.summary.clusterSizes

# Examine the predictions DataFrame:
gmm.summary.predictions.printSchema()
gmm.summary.predictions.select("features", "prediction", "probability").head(10)


# ## Evaluate the Gaussian mixture model

# Extract the log-likelihood from the summary object:
gmm.summary.logLikelihood

# Use the `ClusteringEvaluator` to compute the [silhouette
# measure](https://en.wikipedia.org/wiki/Silhouette_%28clustering%29):
from pyspark.ml.evaluation import ClusteringEvaluator
evaluator = ClusteringEvaluator()
evaluator.evaluate(gmm.summary.predictions)


# ## Plot the clusters

def plot_clusters(gmm):
  # Specify a color palette:
  colors = ["blue", "orange", "green", "red"]
  # Create a map of Fargo, North Dakota, USA:
  m = folium.Map(location=[46.8772222, -96.7894444], zoom_start=13)
  # Add the home coordinates:
  rows = gmm.summary.predictions.select("home_lat", "home_lon", "prediction").collect()
  for row in rows:
    folium.CircleMarker(location=[row["home_lat"], row["home_lon"]], radius=2, color=colors[row["prediction"]], fill=True).add_to(m)
  # Add the cluster centers (Gaussian means):
  centers = gmm.gaussiansDF.collect()
  for (i, center) in enumerate(centers):
    folium.CircleMarker(location=center["mean"], color=colors[i]).add_to(m)
  # Return the map:
  return(m)
plot_clusters(gmm)


# ## Explore the cluster profiles

# Print the distribution of `gender` by cluster:
gmm.summary.predictions \
  .groupBy("prediction", "gender") \
  .count() \
  .orderBy("prediction", "gender") \
  .show()

# Plot the distribution of `gender` by cluster:

pdf = gmm.summary.predictions.fillna("missing", subset=["gender"]).toPandas()
sns.countplot(data=pdf, x="prediction", hue="gender", hue_order=["female", "male", "missing"])


# ## Save and load the Gaussian mixture model

# Save the Gaussian mixture model to HDFS and overwrite any existing directory:
gmm.write().overwrite().save(S3_HOME + "/models/gmm")

# The following shortcut will save the model and return an error if the directory exists:
#```python
#gmm.save("models/gmm")
#```

# Load the Gaussian mixture model from HDFS:
from pyspark.ml.clustering import GaussianMixtureModel
gmm_loaded = GaussianMixtureModel.read().load(S3_HOME + "/models/gmm")

# Alternatively, use the following convenience method:
#```python
#gmm_loaded = GaussianMixtureModel.load("models/gmm")
#```


# Apply the loaded Gaussian mixture model:
clustered = gmm_loaded.transform(assembled)
clustered.printSchema()


# ## Exercises

# (1) Specify a Gaussian mixture model with three clusters.

gm3 = GaussianMixture(featuresCol="features", k=3, seed=12345)

# (2) Fit the Gaussian mixture model on the `assembled` DataFrame.

gmm3 = gm3.fit(assembled)

# (3) Examine the Gaussian mixture model parameters stored in
# the `weights` and `GaussiansDF` attributes.

gmm3.weights
gmm3.gaussiansDF.head(5)

# (4) Plot the Gaussian mixture model using the `plot_clusters` function.

plot_clusters(gmm3)

# (5) Apply the Gaussian mixture model to the `assembled` DataFrame using the `transform` method:

predictions = gmm3.transform(assembled)
predictions.printSchema()
predictions.select("features", "prediction", "probability").show(10, truncate=False)

# (6) Print the distribution of `gender` by cluster.

predictions.crosstab("prediction", "gender").orderBy("prediction_gender").show()

# (7) **Bonus:** Plot the distribution of `gender` by cluster.

pdf = predictions.select("gender", "prediction").fillna("missing", subset=["gender"]).toPandas()
sns.countplot(data=pdf, x="prediction", hue="gender", hue_order=["female", "male", "missing"])


# ## References

# [Wikipedia - Cluster analysis](https://en.wikipedia.org/wiki/Cluster_analysis)

# [Spark Documentation - Clustering](http://spark.apache.org/docs/latest/ml-clustering.html)

# [Spark Python API - pyspark.ml.clustering.GaussianMixture class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixture)

# [Spark Python API - pyspark.ml.clustering.GaussianMixtureModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixtureModel)

# [Spark Python API - pyspark.ml.clustering.GaussianMixtureSummary class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.GaussianMixtureSummary)


# ## Stop the SparkSession

spark.stop()
