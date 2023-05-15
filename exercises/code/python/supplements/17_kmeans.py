# # Building and evaluating clustering models

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


## Introduction

# In this module we use the k-means clustering functionality in Spark MLlib to
# look for structure in the ride data.


# ## Setup

# Import useful packages, modules, classes, and functions:
import pandas as pd
import folium

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Load the enhanced ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Preprocess the data

# Remove the cancelled rides:
rides_filtered = rides.filter(rides.cancelled == 0)


# ## Extract, transform, and select the features

# Let us try to cluster rides by origin and destination coordinates:
features_selected = ["origin_lat", "origin_lon", "dest_lat", "dest_lon"]

# Assemble the feature vector:
from pyspark.ml.feature import VectorAssembler
va = VectorAssembler(inputCols=features_selected, outputCol="features")
rides_assembled = va.transform(rides_filtered)

# Standardizing the feature vector ensures that the features are in the same
# scale and each feature is given a fair chance to contribute to the clustering
# model:
from pyspark.ml.feature import StandardScaler
ss = StandardScaler(inputCol="features", outputCol="features_scaled", withMean=False, withStd=True)
rides_standardized = ss.fit(rides_assembled).transform(rides_assembled)

# **Note:** We are running the `StandardScaler` after the `VectorAssembler`.
# This is more efficient in this example since `StandardScaler` will
# standardize the entire feature vector in one operation.

# **Note:** Given that latitude and longitude are in similar scales,
# standardization may be superfluous in this case.
rides_standardized.describe("origin_lat", "origin_lon", "dest_lat", "dest_lon").show()

# Spark MLlib does not provide a transformer to unscale the features.  In order
# to create meaningful plots below, we will proceed with unscaled features.


# ## Specify and fit a k-means model

# Use the `KMeans` class constructor to specify a k-means model:
from pyspark.ml.clustering import KMeans
kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=3)
type(kmeans)

# Use the `explainParams` method to get a full list of the arguments:
print(kmeans.explainParams())

# Use the `fit` method to fit the k-means model:
kmeans_model = kmeans.fit(rides_standardized)
type(kmeans_model)

# **Note:** Euclidean distance may not be appropriate in this case.


# ## Evaluate the k-means model

# Compute cluster costs:
kmeans_model.computeCost(rides_standardized)

# **Note:** The `computeCost` is generally not informative on its own.  It is
# more useful when comparing multiple clustering models.

# Print out the cluster centroids:
kmeans_model.clusterCenters()

if kmeans_model.hasSummary:
  
  # Print cluster sizes:
  print(kmeans_model.summary.clusterSizes)
  
  # Show predictions:
  kmeans_model.summary.predictions.printSchema()
  kmeans_model.summary.predictions.select("features", "cluster").show(5, truncate=False)
  
# Plot cluster centers:
center_map = folium.Map(location=[46.8772222, -96.7894444])
for cluster in kmeans_model.clusterCenters():
  # Plot marker at origin.
  folium.Marker([cluster[0], cluster[1]], icon=folium.Icon(color="green")).add_to(center_map)
  # Plot marker at destination.
  folium.Marker([cluster[2], cluster[3]], icon=folium.Icon(color="red")).add_to(center_map)
  # Plot line between origin and destination.
  folium.PolyLine([[cluster[0], cluster[1]], [cluster[2], cluster[3]]]).add_to(center_map)
center_map


# ## Explore cluster profiles

# Understanding a clustering model is an exercise in data analysis and
# visualization.  We can bring our full toolkit to bear.

# Get original DataFrame with cluster ids:
predictions = kmeans_model.summary.predictions

# Explore a categorical variable:
predictions \
  .crosstab("cluster", "service") \
  .orderBy("cluster_service") \
  .show()

# Explore a continuous variable:
from pyspark.sql.functions import count, mean, stddev
predictions \
  .groupBy("cluster") \
  .agg(count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("cluster") \
  .show()

# Let us explore the cluster profiles more visually.

# Create a random sample and read it into a pandas DataFrame:
clustered = predictions.sample(withReplacement=False, fraction=0.01).toPandas()

# Import the seaborn package:
import seaborn as sns

# Plot the distribution of `service` for each cluster:
sns.countplot(x="cluster", hue="service", data=clustered)

# Plot the distribution of `distance` for each cluster:
sns.boxplot(x="cluster", y="distance", data=clustered)


# ## Exercises

# (1) Experiment with different values of k (number of clusters).

# (2) Experiment with different sets of features.

# (3) Explore other clustering algorithms. 


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Wikipedia - Cluster analysis](https://en.wikipedia.org/wiki/Cluster_analysis)

# [Spark Documentation - Clustering](http://spark.apache.org/docs/latest/ml-clustering.html)

# [Spark Python API - KMeans class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.KMeans)

# [Spark Python API - KMeansModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.KMeansModel)

