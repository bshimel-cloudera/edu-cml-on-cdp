# # Inspecting a Spark DataFrame

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we inspect our DataFrame more carefully.  In particular, we
# inspect columns that represent the following types of variables:
 
# * Primary key variable
# * Categorical variable
# * Continuous numerical variable
# * Date and time variable
 
# In the process, we introduce various Spark SQL functionality that we cover
# more formally in subsequent modules.


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Read the raw ride data from HDFS to a Spark SQL DataFrame

rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)


# ## Inspecting a DataFrame

# Print the schema:
rides.printSchema()

# Use the *pandas* library to display the DataFrame as a scrollable HTML table:
import pandas as pd
pd.options.display.html.table_schema=True
rides.limit(5).toPandas()

# **Note:** The `limit` method returns a DataFrame with the specified number of
# rows; the `toPandas` method returns a pandas DataFrame.

# Use the `describe` method to get basic summary statistics on each column:
rides.describe().toPandas()

# **Note:**  The `describe` method returns a DataFrame.


# ## Inspecting a DataFrame column

# ### Inspecting a primary key variable

# The `id` column represents a primary key variable:
rides.select("id").show(10)

# **Note:** `select` is a DataFrame method that returns a DataFrame.

# The `id` column should be non-null and unique.  Count the number of missing
# (null) values:
rides.filter(rides.id.isNull()).count()

# **Note:** `filter` is a DataFrame method that returns a DataFrame consisting
# of the rows for which its argument is true.  `rides.id` is a Column object
# and `isNull` is a Column method.

# Count the number of rows:
rides.count()

# Count the number of distinct values:
rides.select("id").distinct().count()

# Count the number of non-missing and distinct values using Column functions:
from pyspark.sql.functions import count, countDistinct
rides.select(count("*"), count("id"), countDistinct("id")).show()

# We have been using the DataFrame API of Spark SQL.  To use the SQL API to
# count the number of non-missing and distinct values, first register the
# DataFrame as a *temporary view*:
rides.createOrReplaceTempView("rides_view")

# Then use the `sql` method to run a query:
spark.sql("SELECT COUNT(*), COUNT(id), COUNT(DISTINCT id) FROM rides_view").show()

# **Note:** The `sql` method returns a DataFrame.


# ### Inspecting a categorical variable

# The `service` column represents a categorical variable:
rides.select("service").show(10)

# **Question:** What do the missing (null) values represent?

# Count the number of missing (null) values:
rides.filter(rides.service.isNull()).count()

# Count the number of distinct values:
rides.select("service").distinct().count()

# Print the distinct values:
rides.select("service").distinct().show()

# Count the number of rides by service:
rides.groupby("service").count().show()

# Use the SQL API to count the number of rides by service:
spark.sql("SELECT service, COUNT(*) FROM rides_view GROUP BY service").show()
  
# Use pandas to plot the number of rides by service:
rides.groupby("service").count().toPandas().plot(x="service", y="count", kind="bar")


# ### Inspecting a numerical variable

# The `distance` column represents a numerical variable (stored as an
# integer):
rides.select("distance").show(10)

# Use the `describe` method to compute basic summary statistics:
rides.describe("distance").show()

# **Question:** Are there any missing (null) values?

# Use the `approxQuantile` method to get customized quantiles:
rides.approxQuantile("distance", \
	probabilities=[0.0, 0.25, 0.5, 0.75, 1.0], \
	relativeError=1e-5)

# **Note:** The `approxQuantile` method returns a Python list.

# **Question:** Why does Spark produce approximate quantiles?

# See the documentation for more details:
rides.approxQuantile?

# Use pandas to plot a basic histogram:
rides.select("distance").toPandas().plot(kind="hist")

# **Warning:** `toPandas()` is dangerous in the Spark world.  Why?
 

# ### Inspecting a date and time variable

# The `date_time` column represents a date and time variable:
rides.select("date_time").show(10)

# However, Spark read it in as a string:
rides.select("date_time").printSchema()

# Use the `cast` method to convert it to a timestamp:
dates = rides.select("date_time", rides.date_time.cast("timestamp").alias("date_time_fixed"))
dates.show(5)

# Note that timestamps are represented by Python `datetime` objects:
dates.head(5)

# Note that the `describe` method does not generate summary statistics for date
# and time variables (unless they are represented as strings):
dates.describe().show(5)


# ## Exercises

# (1) Read the raw driver data into a Spark DataFrame called `drivers`.

# (2) Examine the inferred schema.  Do the data types seem appropriate?

# (3) Verify the integrity of the putative primary key `id`.

# (4) Inspect `birth_date`.  What data type did Spark infer?

# (5) Determine the unique values of `student`.  What type of variable do you
# think `student` is?

# (6) Count the number of drivers by `vehicle_make`.  What is the most popular
# make?

# (7) Compute basic summary statistics on the `rides` column.  How does the
# mean number of rides compare to the median?

# (8) **Bonus:** Inspect additional columns of the `drivers` DataFrame.

# (9) **Bonus:** Inspect the raw rider data.


# ## References

# [Spark Python API - pyspark.sql.DataFrame
# class](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Spark Python API - pyspark.sql.Column
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)

# [Spark Python API - pyspark.sql.types
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)

# [pandas Documentation -
# Visualization](http://pandas.pydata.org/pandas-docs/stable/visualization.html)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
