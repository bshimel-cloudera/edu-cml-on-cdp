# # Type Conversions when Reading a Spark DataFrame into a pandas DataFrame

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.

# In this module we highlight some of the issues with column type conversions
# when reading a Spark DataFrame into a pandas DataFrame.  The list of issues
# presented below is by no means exhaustive, but should give the reader an idea
# of the potential issues that she may encounter.


# ## Setup

# Import some useful packages and modules:
from pyspark.sql.functions import *
import pandas as pd

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Load the enhanced rides data from HDFS:
rides_sdf = spark.read.parquet(S3_ROOT + "/duocar/joined")

# **Note:** In this module we will use "sdf" to denote Spark DataFrames and
# "pdf" to denote pandas DataFrames.


# ## Loading a sample from a Spark DataFrame into a (local) pandas DataFrame

# Use the `sample` method to select a random sample and the `toPandas` method
# to read the resulting Spark DataFrame into a pandas DataFrame.
rides_pdf = rides_sdf.sample(withReplacement=False, fraction=0.01, seed=12345).toPandas()

# **Warning:**  We are loading the data into local memory so ensure that it
# will fit.

# **Note:** Spark uses the pandas `from_records` method on the collected Spark
# DataFrame to create the pandas DataFrame.  Unfortunately, it does not make
# all of the `from_records` functionality available to the user:
# ```python
# rides_sdf.toPandas??
# ```

# We can use the `info` method to display more information on the pandas
# DataFrame:
rides_pdf.info()

# Note that some columns do not have the expected type.  Let us compare the
# types for each DataFrame:
for i in range(len(rides_sdf.columns)):
  print("%-20s %20s %20s" % (rides_sdf.columns[i], rides_sdf.schema[i].dataType, rides_pdf.dtypes[i]))

# ### StringType

# A Spark `StringType` column is converted to a Python `object` column.
# Non-null values are stored as Unicode strings:
[type(x) for x in rides_pdf["rider_gender"][pd.notnull(rides_pdf["rider_gender"])][:5]]

# Null values are stored as Python `NoneType`:
[type(x) for x in rides_pdf["rider_gender"][pd.isnull(rides_pdf["rider_gender"])][:5]]

# ### BooleanType

# A Spark `BooleanType` column **without null values** is converted to a numpy `bool` column:
[type(x) for x in rides_pdf["cancelled"][:5]]

# A Spark `BooleanType` column **with null values** is converted to a Python object column
# with the numpy `bool` type for valid values and the Python `NoneType` for null values.

# ### IntegerType

# A Spark `IntegerType` column **without null values** is converted to a numpy `int` column:
[type(x) for x in rides_pdf["utc_offset"][:5]]

# A Spark `IntegerType` column **with null values** is converted to a numpy `float` column.
# Non-null values are stored as numpy `float` type:
[type(x) for x in rides_pdf["distance"][pd.notnull(rides_pdf["distance"])][:5]]

# Null values are stored as NaN (Not a Number) values:
[x for x in rides_pdf["distance"][pd.isnull(rides_pdf["distance"])][:5]]

# ### DoubleType or FloatType

# A Spark `DoubleType` or `FloatType` column is converted to a numpy `float` column.
# Null values are converted to NaN values.

# ### DecimalType

# A Spark `DecimalType` column is converted to a Python `decimal.Decimal` column:
[type(x) for x in rides_pdf["origin_lat"][:5]]

# Many Python packages do not treat Decimal types as numeric values:
rides_pdf["origin_lat"].describe()

# Use the `astype` method to convert a Decimal column to a float column:
rides_pdf["origin_lat"].astype(float).describe()

# **Note:** The pandas `to_numeric` function does not work in this case.

# See the Python [decimal](https://docs.python.org/2/library/decimal.html) module
# for more information on working with decimals in Python.


# ### TimestampType

# A Spark `TimestampType` column is converted to a pandas `Timestamp` column:
[type(x) for x in rides_pdf["date_time"][:5]]

# pandas treats Timestamps appropriately in various functions and methods:
rides_pdf["date_time"].describe()


# ### DateType

# A Spark `DateType` column is converted to a Python `datetime.date` column:
[type(x) for x in rides_pdf["driver_birth_date"][:5]]

# pandas treats a datetime.date column as a generic Python object:
rides_pdf["driver_birth_date"].describe()

# Use the `to_datetime` method to convert to a pandas Timestamp column:
pd.to_datetime(rides_pdf["driver_birth_date"]).describe()

# See the Python [datetime](https://docs.python.org/2/library/datetime.html) module
# for more information on working with dates and times in Python.


# ### Other Data Types

# Other Spark data types such as ArrayType, MapType, and StructType may not convert
# as desired.  You may want to convert the data into the desired form before loading
# into a pandas DataFrame.

rides_sdf \
  .select(col("origin_lat").cast("float"), col("driver_birth_date").cast("timestamp")) \
  .sample(withReplacement=False, fraction=0.01) \
  .toPandas() \
  .info()
  
# This plot will not run unless the decimals are cast to floats:
rides_sdf \
  .select(col("origin_lat").cast("float"), col("origin_lon").cast("float")) \
  .sample(withReplacement=False, fraction=0.01) \
  .toPandas() \
  .plot.scatter("origin_lat", "origin_lon")


# ## Exercises

# None


# ## Cleanup

# Stop the SparkSession:
spark.stop()


# ## References

# [Spark toPandas method](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.toPandas)

# [pandas from_records method](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.from_records.html)
