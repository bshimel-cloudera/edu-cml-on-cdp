# # Transforming DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate some basic transformations on DataFrames:
# * Working with columns
#   * Selecting columns
#   * Dropping columns
#   * Specifying columns
#   * Adding columns
#   * Changing the column name
#   * Changing the column type
# * Working with rows
#   * Ordering rows
#   * Keeping a fixed number of rows
#   * Keeping distinct rows
#   * Filtering rows
#   * Sampling rows
# * Working with missing values

# **Note:** There is often more than one way to do these transformations.


# ## Spark SQL DataFrames

# * Spark SQL DataFrames are inspired by R data frames and Python pandas DataFrames

# * Spark SQL DataFrames are resilient distributed structured tables
#   * A DataFrame is a distributed collection of *Row* objects
#   * A Row object is an ordered collection of objects with names and types

# * Properties of DataFrames
#   * Immutable - DataFrames are read-only
#   * Evaluated lazily - Spark only does work when it has to
#   * Ephemeral - DataFrames disappear unless explicitly persisted

# * Operations on DataFrames
#   * *Transformations* return a DataFrame
#     * Narrow Transformations
#     * Wide Transformations
#   * *Actions* cause Spark to do work

# * Spark SQL provides two APIs
#   * SQL
#   * DataFrame

# * Spark SQL code is declarative - the Catalyst optimizer produces core Spark RDD code


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw data from HDFS:

rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
rides.printSchema()

drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
drivers.printSchema()

riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)
riders.printSchema()


# ## Working with Columns

# ### Selecting Columns

# Use the `select` method to select specific columns:
riders.select("birth_date", "student", "sex").printSchema()

# ### Dropping Columns

# Use the `drop` method to drop specific columns:
riders.drop("first_name", "last_name", "ethnicity").printSchema()

# ### Specifying Columns

# We have used the column name to reference a DataFrame column:
riders.select("first_name").printSchema()

# We actually work with `Column` objects in Spark SQL.
# Use the following syntax to reference a column object in a particular DataFrame:
riders.select(riders.first_name).printSchema()
riders.select(riders["first_name"]).printSchema()
type(riders["first_name"])

# Use the `col` or `column` function to reference a general column object:
from pyspark.sql.functions import col, column
riders.select(col("first_name")).printSchema()
riders.select(column("first_name")).printSchema()
type(column("first_name"))

# Use `*` (in quotes) to specify all columns:
riders.select("*").printSchema()

# ### Adding Columns

# Use the `withColumn` method to add a new column:
riders \
  .select("student") \
  .withColumn("student_boolean", col("student") == 1) \
  .show()

# The `select` method also works:
riders.select("student", (col("student") == 1).alias("student_boolean")).show(5)

# The `selectExpr` method accepts partial SQL expressions:
riders.selectExpr("student", "student = 1 as student_boolean").show(5)

# The `sql` method accepts full SQL statements:
riders.createOrReplaceTempView("riders_view")
spark.sql("select student, student = 1 as student_boolean from riders_view").show(5)

# ### Changing the column name

# Use the `withColumnRenamed` method to rename a column:
riders.withColumnRenamed("start_date", "join_date").printSchema()

# Chain multiple methods to rename more than one column:
riders \
  .withColumnRenamed("start_date", "join_date") \
  .withColumnRenamed("sex", "gender") \
  .printSchema()

# ### Changing the column type

# Recall that `home_block` was read in as a (long) integer:
riders.printSchema()

# Use the `withColumn` (DataFrame) method in conjunction with the `cast`
# (Column) method to change its type:
riders.withColumn("home_block", col("home_block").cast("string")).printSchema()

# **Note:** If we need to change the name and/or type of many columns, then we
# may want to consider specifying the schema on read.


# ## Working with rows

# ### Ordering rows

# Use the `sort` or `orderBy` method to sort a DataFrame by particular columns:
rides \
  .select("rider_id", "date_time") \
  .sort("rider_id", "date_time", ascending=True) \
  .show()

# **Note:** Ascending order is the default.

rides \
  .select("rider_id", "date_time") \
  .orderBy("rider_id", "date_time", ascending=False) \
  .show()

# Use the `asc` and `desc` methods to specify the sort order:
rides \
  .select("rider_id", "date_time") \
  .sort(col("rider_id").asc(), col("date_time").desc()) \
  .show()

# Alternatively, use the `asc` and `desc` functions to specify the sort order:
from pyspark.sql.functions import asc, desc
rides \
  .select("rider_id", "date_time") \
  .orderBy(asc("rider_id"), desc("date_time")) \
  .show()

# ### Selecting a fixed number of rows

# Use the `limit` method to select a fixed number of rows:
riders.select("student", "sex").limit(5).show()

# **Question:** What is the difference between `df.show(5)` and `df.limit(5).show()`?

# ### Selecting distinct rows

# Use the `distinct` method to select distinct rows:
riders.select("student", "sex").distinct().show()

# You can also use the `dropDuplicates` method:
riders.select("student", "sex").dropDuplicates().show()

# ### Filtering rows

# Use the `filter` or `where` method along with a Boolean column expression to select
# particular rows:
riders.filter(col("student") == 1).count()
riders.where(col("sex") == "female").count()
riders.filter(col("student") == 1).where(col("sex") == "female").count()

# ### Sampling rows

# Use the `sample` method to select a random sample of rows with or without
# replacement:
riders.count()
riders.sample(withReplacement=False, fraction=0.1, seed=12345).count()

# Use the `sampleBy` method to select a stratified random sample:
riders \
  .groupBy("sex") \
  .count() \
  .show() 
riders \
  .sampleBy("sex", fractions={"male": 0.2, "female": 0.8}, seed=54321) \
  .groupBy("sex") \
  .count() \
  .show()

# We have randomly sampled 20% of the male riders and 80% of the female riders.


# ## Working with missing values

# Note the missing (null) values in the following DataFrame:
riders_selected = riders.select("id", "sex", "ethnicity")
riders_selected.show(25)

# Drop rows with any missing values:
riders_selected.dropna(how="any", subset=["sex", "ethnicity"]).show(25)

# Drop rows with all missing values:
riders_selected.na.drop(how="all", subset=["sex", "ethnicity"]).show(25)

# **Note**: `dropna` and `na.drop` are equivalent.

# Replace missing values with a common value:
riders_selected.fillna("OTHER/UNKNOWN", ["sex", "ethnicity"]).show(25)

# Replace missing values with different values:
riders_missing = riders_selected.na.fill({"sex": "OTHER/UNKNOWN", "ethnicity": "MISSING"})
riders_missing.show(25)

# **Note**: `fillna` and `na.fill` are equivalent.

# **Note**: The fill value type must match the column type.

# Replace arbitrary values with a common value:
riders_missing.replace(["OTHER/UNKNOWN", "MISSING"], "NA", ["sex", "ethnicity"]).show(25)

# Replace arbitrary values with different values:
riders_missing.na.replace({"OTHER/UNKNOWN": "NA", "MISSING": "NO RESPONSE"}, None, ["sex", "ethnicity"]).show(25)

# **Note:** `replace` and `na.replace` are equivalent.


# ## Exercises

# (1) Replace the missing values in `rides.service` with the string `Car`.

# (2) Rename `rides.cancelled` to `rides.canceled`.

# (3) Sort the `rides` DataFrame in descending order with respect to
# `driver_id` and ascending order with respect to `date_time`.

# (4) Create an approximate 20% random sample of the `rides` DataFrame.

# (5) Remove the driver's name from the `drivers` DataFrame.

# (6) How many drivers have signed up?  How many female drivers have signed up?
# How many non-white, female drivers have signed up?


# ## References

# [Spark Python API - pyspark.sql.DataFrame
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Spark Python API - pyspark.sql.DataFrameNaFunctions
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameNaFunctions)

# [Spark Python API - pyspark.sql.Column
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
