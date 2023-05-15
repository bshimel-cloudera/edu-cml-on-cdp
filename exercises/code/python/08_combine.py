# # Combining and Splitting DataFrames

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to combine and split DataFrames.


# ## Combining and Splitting DataFrames

# * Spark SQL supports the usual database-style joins:
#   * Cross join
#   * Inner join
#   * Left semi join
#   * Left anti join
#   * Left outer join
#   * Right outer join
#   * Full outer join

# * Joins are expensive in the big-data world
#   * Perform joins early in the process
#   * Amortize the cost over many use cases

# * Spark SQL supports the following set operations:
#   * Union
#   * Intersection
#   * Subtraction

# * Spark SQL provides a method to split a DataFrame into random subsets


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# ## Joining DataFrames

# We will use the following DataFrames to demonstrate joins:

scientists = spark.read.csv(S3_ROOT + "/duocar/raw/data_scientists/", header=True, inferSchema=True)
scientists.show()

offices = spark.read.csv(S3_ROOT + "/duocar/raw/offices/", header=True, inferSchema=True)
offices.show()

# ### Cross join

# Use the `crossJoin` DataFrame method to join every row in the left
# (`scientists`) DataFrame with every row in the right (`offices`) DataFrame:
scientists.crossJoin(offices).show()

# **Warning:** This can result in very big DataFrames!

# **Note:** Columns with the same name are not renamed.

# **Note:** This is called the *Cartesian product* of the two DataFrames.

# ### Inner join

# Use the `join` DataFrame method with different values of the `how` argument
# to perform other types of joins.

# Use a join expression and the value `inner` to return only those rows for
# which the join expression is true:
scientists.join(offices, scientists.office_id == offices.office_id, "inner").show()

# This gives us a list of data scientists associated with an office and the
# corresponding office information.

# Since the join key has the same name on both DataFrames, we can simplify the
# join as follows:
scientists.join(offices, "office_id", "inner").show()

# Since an inner join is the default, we can further simplify the join as
# follows:
scientists.join(offices, "office_id").show()

# ### Left semi join

# Use the value `left_semi` to return the rows in the left DataFrame that match
# rows in the right DataFrame:
scientists \
  .join(offices, scientists.office_id == offices.office_id, "left_semi") \
  .show()

# This gives us a list of data scientists associated with an office.

# ### Left anti join

# Use the value `left_anti` to return the rows in the left DataFrame that do
# not match rows in the right DataFrame:
scientists \
  .join(offices, scientists.office_id == offices.office_id, "left_anti") \
  .show()

# This gives us a list of data scientists not associated with an office.

# **Note:** You can think of the left semi and left anti joins as special types
# of filters.

# ### Left outer join

# Use the value `left` or `left_outer` to return every row in the left
# DataFrame with or without matching rows in the right DataFrame:
scientists \
  .join(offices, scientists.office_id == offices.office_id, "left_outer") \
  .show()

# This gives us a list of data scientists with or without an office.

# ### Right outer join

# Use the value `right` or `right_outer` to return every row in the right
# DataFrame with or without matching rows in the left DataFrame:
scientists \
  .join(offices, scientists.office_id == offices.office_id, "right_outer") \
  .show()

# This gives us a list of offices with or without a data scientist.

# **Note:** The Paris office has two data scientists.

# ### Full outer join

# Use the value `full`, `outer`, or `full_outer` to return the union of the
# left outer and right outer joins (with duplicates removed):
scientists \
  .join(offices, scientists.office_id == offices.office_id, "full_outer") \
  .show()

# This gives us a list of all data scientists whether or not they have an
# office and all offices whether or not they have any data scientists.

# ### Example: Joining the DuoCar data

# Let us join the driver, rider, and review data with the ride data.

# Read the clean data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/clean/rides/")
drivers = spark.read.parquet(S3_ROOT + "/duocar/clean/drivers/")
riders = spark.read.parquet(S3_ROOT + "/duocar/clean/riders/")
reviews = spark.read.parquet(S3_ROOT + "/duocar/clean/ride_reviews/")

# Since we want all the ride data, we will use a sequence of left outer joins:
joined = rides \
  .join(drivers, rides.driver_id == drivers.id, "left_outer") \
  .join(riders, rides.rider_id == riders.id, "left_outer") \
  .join(reviews, rides.id == reviews.ride_id, "left_outer")
joined.printSchema()

# **Note:** We probably want to rename some columns before joining the data and
# remove the duplicate ID columns after joining the data to make this DataFrame
# more usable.  For example, see the `joined` data in the DuoCar data
# repository:
spark.read.parquet(S3_ROOT + "/duocar/joined/").printSchema()


# ## Applying set operations to DataFrames

# Spark SQL provides the following DataFrame methods that implement set
# operations:
# * `union`
# * `intersect`
# * `subtract`

# Use the `union` method to get the union of rows in two DataFrames with
# similar schema:
driver_names = drivers.select("first_name")
driver_names.count()

rider_names = riders.select("first_name")
rider_names.count()

names_union = driver_names.union(rider_names).orderBy("first_name")
names_union.count()
names_union.show()

# Note that `union` does not remove duplicates.  Use the `distinct` method to
# remove duplicates:
names_distinct = names_union.distinct()
names_distinct.count()
names_distinct.show()

# Use the `intersect` method to return rows that exist in both DataFrames:
name_intersect = driver_names.intersect(rider_names).orderBy("first_name")
name_intersect.count()
name_intersect.show()

# Use the `subtract` method to return rows in the left DataFrame that do not
# exist in the right DataFrame:
names_subtract = driver_names.subtract(rider_names).orderBy("first_name")
names_subtract.count()
names_subtract.show()


# ## Splitting a DataFrame

# Use the `randomSplit` DataFrame method to split a DataFrame into random
# subsets:
riders.count()
(train, validate, test) = riders.randomSplit(weights=[0.6, 0.2, 0.2])
(train.count(), validate.count(), test.count())

# Use the `seed` argument to ensure replicability:
(train, validate, test) = riders.randomSplit([0.6, 0.2, 0.2], seed=12345)
(train.count(), validate.count(), test.count())

# If the proportions do not add up to one, then Spark will normalize the values:
(train, validate, test) = riders.randomSplit([60.0, 20.0, 20.0], seed=12345)
(train.count(), validate.count(), test.count())

# **Note:** The weights must be doubles.

# **Note:** The same seed will result in the same random split.


# ## Exercises

# (1) Join the `rides` DataFrame with the `reviews` DataFrame.  Keep only those
# rides that have a review.

# (2) How many drivers have not provided a ride?


# ## References

# [Spark Python API - crossJoin DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.crossJoin)

# [Spark Python API - join DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.join)

# [Spark Python API - union DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.union)

# [Spark Python API - intersect DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.intersect)

# [Spark Python API - subtract DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.subtract)

# [Spark Python API - randomSplit DataFrame
# method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=catalog#pyspark.sql.DataFrame.randomSplit)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
