# # Exploring and Visualizing DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# Now that we have enhanced our ride data, we can begin a more systematic
# exploration of the relationships among the variables.  The insight gathered
# during this analysis may be used to improve DuoCar's day-to-day business
# operations or it may serve as preparation for more sophisticated analysis and
# modeling using machine learning algorithms.  In this module we use Spark in
# conjunction with some popular Python libraries to explore the DuoCar data.


# ## Possible work flows for big data

# * Work with all of the data on the cluster
#   * Produces accurate reports
#   * Limits analysis to tabular reports
#   * Requires more computation
# * Work with a sample of the data in local memory
#   * Opens up a wide range of tools
#   * Enables more rapid iterations
#   * Produces sampled results
# * Summarize on the cluster and visualize summarized data in local memory
#   * Produces accurate counts
#   * Allows for wide range of analysis and visualization tools


# ## Setup

# Import useful packages, modules, classes, and functions:
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Create a SparkSession:
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Load the enhanced ride data from HDFS:
rides_sdf = spark.read.parquet(S3_ROOT + "/duocar/joined_all")

# Create a random sample and load it into a pandas DataFrame:
rides_pdf = rides_sdf.sample(withReplacement=False, fraction=0.01, seed=12345).toPandas()

# **Note:** In this module we will use `sdf` to denote Spark DataFrames and
# `pdf` to denote pandas DataFrames.


# ## Exploring a single variable

# In this section we use Spark and Spark in conjunction with pandas,
# matplotlib, and seaborn to explore a single variable (i.e., column).  Many of
# the techniques presented here can be useful when inspecting variables too.

# ### Exploring a categorical variable

# Let us explore type of car service, which is an example of a categorical
# variable.

# We can use the `groupBy` method in Spark to create a one-way frequency table:
summarized_sdf = rides_sdf.groupBy("service").count().orderBy("service")
summarized_sdf.show()

# We can convert the grouped Spark DataFrame to a pandas DataFrame:
summarized_pdf = summarized_sdf.toPandas()
summarized_pdf

# **Note**: Remember that we are loading data into local memory.  In this case
# we are safe since the summarized DataFrame is small.

# Specify the desired order of the categories:
order = ["Car", "Noir", "Grand", "Elite"]

# Use the seaborn package to plot the *summarized* data:
sns.barplot(x="service", y="count", data=summarized_pdf, order=order)

# Use the seaborn package to plot the *sampled* data:
sns.countplot(x="service", data=rides_pdf, order=order)

# **Note:** The plots present the same qualitative information.


# ### Exploring a continuous variable

# We can use the `describe` method to compute basic summary statistics:
rides_sdf.describe("distance").show()

# and aggregate functions to get additional summary statistics:
rides_sdf.agg(skewness("distance"), kurtosis("distance")).show()

# We can use the `approxQuantile` method to compute approximate quantiles:
rides_sdf.approxQuantile("distance", probabilities=[0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0], relativeError=0.1)

# **Note:** Set `relativeError = 0.0` for exact (and possibly expensive)
# quantiles.

# However, a histogram is generally more informative than summary statistics.
# Create an approximate 1% random sample:
sampled_pdf = rides_sdf \
  .select("distance") \
  .dropna() \
  .sample(withReplacement=False, fraction=0.01, seed=23456) \
  .toPandas()

# Use seaborn to create a histogram on the *sampled* data:
sns.distplot(sampled_pdf["distance"], kde=False)

# Use seaborn to create a *normalized* histogram with rug plot and kernel
# density estimate:
sns.distplot(sampled_pdf["distance"], kde=True, rug=True)

# Use seaborn to create a boxplot, which displays much of the information
# computed via the `approxQuantile` method:
sns.boxplot(x="distance", data=sampled_pdf)


# ## Exploring a pair of variables

# ### Categorical-Categorical

# Let us explore the distribution of a rider's gender by student status.

# Create a two-way frequency table:
summarized_sdf = rides_sdf.groupBy("rider_student", "rider_gender").count().orderBy("rider_student", "rider_gender")
summarized_sdf.show()

# Convert the *summarized* Spark DataFrame to a pandas DataFrame:
summarized_pdf = summarized_sdf.toPandas()
summarized_pdf

# Produce a bar chart using seaborn:
hue_order = ["female", "male"]
sns.barplot(x="rider_student", y="count", hue="rider_gender", hue_order=hue_order, data=summarized_pdf)

# Replace missing values:
summarized_pdf = summarized_sdf.fillna("missing").toPandas()
hue_order = ["female", "male", "missing"]
sns.barplot(x="rider_student", y="count", hue="rider_gender", hue_order=hue_order, data=summarized_pdf)

# ### Categorical-Continuous

# Let us explore the distribution of ride distance by rider student status.

# We can produce tabular reports in Spark:
rides_sdf \
  .groupBy("rider_student") \
  .agg(count("distance"), mean("distance"), stddev("distance")) \
  .orderBy("rider_student") \
  .show()

# Alternatively, we can produce visualizations on a sample:
sampled_pdf = rides_sdf \
  .select("rider_student", "distance") \
  .sample(withReplacement=False, fraction=0.01, seed=34567) \
  .toPandas()

# Use seaborn to produce a strip plot on the sampled data:
sns.stripplot(x="rider_student", y="distance", data=sampled_pdf, jitter=True)

# **Note:** Non-students are taking the long rides.

# See the supplement for other ways to visualize this data.


# ### Continuous-Continuous

# Use the `corr`, `covar_samp`, and `covar_pop` aggregate functions to measure
# the linear relationship between two variables:
rides_sdf.agg(corr("distance", "duration"),
              covar_samp("distance", "duration"),
              covar_pop("distance", "duration")).show()

# Use the `jointplot` function to produce an enhanced scatter plot on the
# *sampled* data:
sns.jointplot(x="distance", y="duration", data=rides_pdf)

# Overlay a linear regression model:
sns.jointplot(x="distance", y="duration", data=rides_pdf, kind="reg")

# Overlay a quadratic regression model (order = 2):
sns.jointplot(x="distance", y="duration", data=rides_pdf, kind="reg", order=2)

# Use the `pairplot` function to examine several pairs at once:
sampled_pdf = rides_sdf \
  .select("distance", "duration", hour("date_time")) \
  .dropna() \
  .sample(withReplacement=False, fraction=0.01, seed=45678) \
  .toPandas()
sns.pairplot(sampled_pdf)


# ## Exercises

# (1) Look for variables that might help us predict ride duration.

# (2) Look for variables that might help us predict ride rating.


# ## References

# [The SciPy Stack](https://scipy.org/)

# [pandas](http://pandas.pydata.org/)

# [matplotlib](https://matplotlib.org/index.html)

# [seaborn](https://seaborn.pydata.org/index.html)

# [Bokeh](http://bokeh.pydata.org/en/latest/)

# [Plotly](https://plot.ly/)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
