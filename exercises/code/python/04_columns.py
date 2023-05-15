# # Transforming DataFrame Columns

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# Spark SQL supports various column types and provides a variety of functions
# and Column methods that can be applied to each type.  In this module we
# demonstrate how to transform DataFrame columns of various types.

# * Working with numerical columns
# * Working with string columns
# * Working with datetime columns
# * Working with Boolean columns


# ## Spark SQL Data Types

# * Spark SQL data types are defined in the `pyspark.sql.types` module

# * Spark SQL supports the following basic data types:
#   * NullType
#   * StringType
#   * Byte array data type
#     * BinaryType
#   * BooleanType
#   * Integer data types
#     * ByteType
#     * ShortType
#     * IntegerType
#     * LongType
#   * Fixed-point data type
#     * DecimalType
#   * Floating-point data types
#     * FloatType
#     * DoubleType
#   * Date and time data types
#     * DateType
#     * TimestampType

# * Spark also supports the following complex (collection) types:
#   * ArrayType
#   * MapType
#   * StructType

# * Spark SQL provides various methods and functions that can be applied to the
# various data types


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the raw data from HDFS:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv(S3_ROOT + "/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv(S3_ROOT + "/duocar/raw/riders/", header=True, inferSchema=True)


# ## Working with numerical columns

# ### Example 1: Converting ride distance from meters to miles

from pyspark.sql.functions import col, round
rides \
  .select("distance", round(col("distance") / 1609.344, 2).alias("distance_in_miles")) \
  .show(5)

# **Notes:**
# * We have used the fact that 1 mile = 1609.344 meters.
# * We have used the `round` function to round the result to two decimal places.
# * We have used the `alias` method to rename the column.

# To add a new column, use the `withColumn` method with a new column name:
rides \
  .withColumn("distance_in_miles", round(col("distance") / 1609.344, 2)) \
  .printSchema()

# To replace an existing column, use the `withColumn` method with an existing
# column name:
rides \
  .withColumn("distance", round(col("distance") / 1609.344, 2)) \
  .printSchema()

# ### Example 2: Converting the ride id from an integer to a string

# Use the `format_string` function to convert `id` to a left-zero-padded string:
from pyspark.sql.functions import format_string
rides \
  .withColumn("id_fixed", format_string("%010d", "id")) \
  .select("id", "id_fixed") \
  .show(5)

# **Note:** We have used the [printf format
# string](https://en.wikipedia.org/wiki/Printf_format_string) `%010d` to
# achieve the desired format.

# ### Example 3: Converting the student flag from an integer to a Boolean

# Using a Boolean expression:
riders \
  .withColumn("student_boolean", col("student") == 1) \
  .select("student", "student_boolean") \
  .show(5)

# Using the `cast` method:
riders \
  .withColumn("student_boolean", col("student").cast("boolean")) \
  .select("student", "student_boolean") \
  .show(5)


# ## Working with string columns

# ### Example 4: Normalizing a string column

# Use the `trim` and `upper` functions to normalize `riders.sex`:
from pyspark.sql.functions import trim, upper
riders \
  .withColumn("gender", upper(trim(col("sex")))) \
  .select("sex", "gender") \
  .show(5)

# ### Example 5: Extracting a substring from a string column

# The [Census Block Group](https://en.wikipedia.org/wiki/Census_block_group) is
# the first 12 digits of the [Census
# Block](https://en.wikipedia.org/wiki/Census_block).  Use the `substring`
# function to extract the Census Block Group from `riders.home_block`:
from pyspark.sql.functions import substring
riders \
  .withColumn("home_block_group", substring("home_block", 1, 12)) \
  .select("home_block", "home_block_group") \
  .show(5)

# ### Example 6: Extracting a substring using a regular expression

# Use the `regexp_extract` function to extract the Census Block Group via a
# regular expression:
from pyspark.sql.functions import regexp_extract
riders \
  .withColumn("home_block_group", regexp_extract("home_block", "^(\d{12}).*", 1)) \
  .select("home_block", "home_block_group") \
  .show(5)

# **Note:** The third argument to `regexp_extract` is the capture group.


# ## Working with date and timestamp columns

# ### Example 7: Converting a timestamp to a date 

# Note that `riders.birth_date` and `riders.start_date` were read in as timestamps:
riders.select("birth_date", "start_date").show(5)

# Use the `cast` method to convert `riders.birth_date` to a date:
riders \
  .withColumn("birth_date_fixed", col("birth_date").cast("date")) \
  .select("birth_date", "birth_date_fixed") \
  .show(5)

# Alternatively, use the `to_date` function:
from pyspark.sql.functions import to_date
riders \
  .withColumn("birth_date_fixed", to_date("birth_date")) \
  .select("birth_date", "birth_date_fixed") \
  .show(5)

# ### Example 8: Converting a string to a timestamp

# Note that `rides.date_time` was read in as a string:
rides.printSchema()

# Use the `cast` method to convert it to a timestamp:
rides \
  .withColumn("date_time_fixed", col("date_time").cast("timestamp")) \
  .select("date_time", "date_time_fixed") \
  .show(5)

# Alternatively, use the `to_timestamp` function:
from pyspark.sql.functions import to_timestamp
rides \
  .withColumn("date_time_fixed", to_timestamp("date_time", format="yyyy-MM-dd HH:mm")) \
  .select("date_time", "date_time_fixed") \
  .show(5)

# ### Example 9: Computing the age of each rider

# Use the `current_date` and `months_between` functions to compute the age of
# each rider:
from pyspark.sql.functions import current_date, months_between, floor
riders \
  .withColumn("today", current_date()) \
  .withColumn("age", floor(months_between("today", "birth_date") / 12)) \
  .select("birth_date", "today", "age") \
  .show(5)

# **Note:** Spark implicitly casts `birth_date` or `today` as necessary.  It is
# probably safer to explicitly cast one of these columns before computing the
# number of months between.


# ## Working with Boolean columns

# ### Example 10: Predefining a Boolean column expression

# You can predefine a Boolean column expression:
studentFilter = col("student") == 1
type(studentFilter)

# You can use the predefined expression to create a new column:
riders \
  .withColumn("student_boolean", studentFilter) \
  .select("student", "student_boolean") \
  .show(5)

# Or filter a DataFrame:
riders \
  .filter(studentFilter) \
  .select("student") \
  .show(5)

# ### Example 11: Working with multiple Boolean column expressions

# Predefine the Boolean column expressions:
studentFilter = col("student") == 1
maleFilter = col("sex") == "male"

# Create a new column using the AND (`&`) operator:
riders.select("student", "sex", studentFilter & maleFilter).show(15)

# Create a new column using the OR (`|`) operator:
riders.select("student", "sex", studentFilter | maleFilter).show(15)

# **Important:** The Boolean column expression parser in Spark SQL is not very
# advanced.  Use parentheses liberally in your expressions.

# Note the difference in how nulls are treated in the computation:
# * true & null = null
# * false & null = false
# * true | null = true
# * false | null = null

# ### Example 12: Using multiple Boolean expressions in a filter

# Use `&` for a logical AND:
riders.filter(maleFilter & studentFilter).select("student", "sex").show(5)

# This is equivalent to
riders.filter(maleFilter).filter(studentFilter).select("student", "sex").show(5)

# Use `|` for a logical OR:
riders.filter(maleFilter | studentFilter).select("student", "sex").show(5)

# Be careful with missing (null) values:
riders.select("sex").distinct().show()
riders.filter(col("sex") != "male").select("sex").distinct().show()


# ## Exercises

# (1) Extract the hour of day and day of week from `rides.date_time`.

# (2) Convert `rides.duration` from seconds to minutes.

# (3) Convert `rides.cancelled` to a Boolean column.

# (4) Create an integer column named `five_star_rating` that is 1.0 if the ride
# received a five-star rating and 0.0 otherwise.

# (5) Create a new column containing the full name for each driver.

# (6) Create a new column containing the average star rating for each driver.

# (7) Find the rider names that are most similar to `Brian`.  **Hint:** Use the
# [Levenshtein](https://en.wikipedia.org/wiki/Levenshtein_distance) function.


# ## References

# [Spark Python API - pyspark.sql.types
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types)

# [Spark Python API - pyspark.sql.DataFrame
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Spark Python API - pyspark.sql.Column
# class](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.Column)

# [Spark Python API - pyspark.sql.functions
# module](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.functions)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
