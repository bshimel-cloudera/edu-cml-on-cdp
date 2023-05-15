# # Exploring and Visualizing DataFrames - Supplement

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Contents

# * Other ways to visualize a categorical-continuous pair
# * Exploring more than two variables
# * Plot the longest ride


# ## Setup

# Import useful packages, modules, classes, and functions:
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


# ## Other ways to visualize a categorical-continuous pair

# Generate sample data:
sample_pdf = rides_sdf \
  .select("rider_student", "distance") \
  .sample(False, 0.01, 12345) \
  .toPandas()

# A bar plot:
sns.barplot(x="rider_student", y = "distance", data=sample_pdf)

# A point plot:
sns.pointplot(x="rider_student", y = "distance", data=sample_pdf)

# A strip plot:
sns.stripplot(x="rider_student", y="distance", data=sample_pdf, jitter=True)

# A swarm plot:
sns.swarmplot(x="rider_student", y="distance", data=sample_pdf)

# A letter value plot:
sns.lvplot(x="rider_student", y="distance", data=sample_pdf)

# A box plot:
sns.boxplot(x="rider_student", y="distance", data=sample_pdf)

# A violin plot:
sns.violinplot(x="rider_student", y="distance", data=sample_pdf)


# ## Exploring more than two variables

# There are numerous ways to explore more than two variables.  The appropriate
# table or plot depends on the variable types and particular question you are
# trying to answer.  We highlight a few common approaches below.

# ### N-way summary tables

# We can use grouping and aggregate functions in Spark to produce summaries.

# **Example:** Three categorical variables
rides_sdf \
  .groupby("rider_student", "rider_gender", "service") \
  .count() \
  .show()

# **Example:** Two categorical variables and one continuous variable
rides_sdf \
  .cube("rider_student", "rider_gender") \
  .agg(F.grouping_id(), F.mean("distance"), F.stddev("distance")) \
  .orderBy("rider_student", "rider_gender") \
  .show()

# **Example:** Two categorical variables and two continuous variables
rides_sdf \
  .groupBy("rider_student", "rider_gender") \
  .agg(F.corr("distance", "duration")) \
  .orderBy("rider_student") \
  .show()
  
# ### Faceted plots

# Generally, carefully crafted visualizations are more enlightening.  Before we
# produce more visualizations, let us fill in the missing values for rider_gender
# using pandas functionality:
rides_pdf["rider_gender"] = rides_pdf["rider_gender"].fillna("missing")

# **Question:** Does this syntax look somewhat familiar?

# **Example:** Three categorical variables

# Specify the desired order of the categories:
order = ["Car", "Noir", "Grand", "Elite"]

# In CML, it is best to encapsulated multi-layered plots within functions:
def tmp_plot():
  g = sns.FacetGrid(data=rides_pdf, row="rider_gender", col="rider_student", margin_titles=True)
  g = g.map(sns.countplot, "service", order=order)
tmp_plot()

# **Example:** Two categorical variables and one continuous variable
def tmp_plot():
  g = sns.FacetGrid(data=rides_pdf, row="rider_gender", col="rider_student", margin_titles=True)
  g = g.map(plt.hist, "distance")
tmp_plot()

# **Example:** Two categorical variables and two continuous variables

# We can use `FacetGrid` to explore the relationship between two continuous
# variables as a function of two categorical variables.  For example, let us
# explore the relationship of ride distance and ride duration as a function of
# rider gender and student status:
def tmp_plot():  # Wrap plot build into function for CML
  g = sns.FacetGrid(data=rides_pdf, row="rider_gender", col="rider_student", margin_titles=True)
  g = g.map(plt.scatter, "distance", "duration")
tmp_plot()


# ## Plot the longest ride

# Get the route coordinates of the longest ride in the DuoCar data and use
# Folium to create a map of the ride route

# Setup
import folium

# Load the rides and ride_routes data from HDFS
rides = spark.read.parquet("/duocar/clean/rides")
ride_routes = spark.read.parquet("/duocar/clean/ride_routes")

# Filter the rides data to the longest ride (by distance) and join it with the
# ride_routes data
longest_ride_route = rides \
  .orderBy(F.col("distance").desc()) \
  .limit(1) \
  .join(ride_routes, F.col("id") == F.col("ride_id"))

# Make a list of the ride route coordinates
coordinates = [[float(i.lat), float(i.lon)] \
               for i in longest_ride_route.collect()]

# Make a Folium map
m = folium.Map()
m.fit_bounds(coordinates, padding=(25, 25))
folium.PolyLine(locations=coordinates, weight=5).add_to(m)
folium.Marker(coordinates[1], popup="Origin").add_to(m)
folium.Marker(coordinates[-1], popup="Destination").add_to(m)
m


# ## Cleanup

# Stop the SparkSession:
spark.stop()
