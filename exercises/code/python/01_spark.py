# # Running a Spark Application from CML Native Workbench

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Overview

# In this module we demonstrate how to do the following:
# * Start a Spark application
# * Read a comma-delimited file from HDFS into a Spark SQL DataFrame
# * Examine the schema of a Spark SQL DataFrame
# * Calculate the dimensions of a Spark SQL DataFrame
# * Examine some rows of a Spark SQL DataFrame
# * Stop a Spark application 


# ## Running a Spark Application from CML Native Workbench

# * A Spark *application* runs on the Java Virtual Machine (JVM)

# * An application consists of a set of Java processes
#   * A *driver* process coordinates the work
#   * A set of *executor* processes perform the work

# * An application normally runs on the Hadoop cluster via YARN
#   * Used to process, analyze, and model large data sets
#   * The driver runs in the CML Native Workbench session engine
#   * The executors run in the worker nodes

# * An application can run locally within the CML Native Workbench session engine
#   * Used to develop and test code on small data sets
#   * The driver runs in the CML Native Workbench session engine
#   * The executors run as threads in the context of the driver process

# * Start an application by creating an instance of the `SparkSession` class

# * Stop an application by calling the `stop` method of the `SparkSession`
# instance


# ## Starting a Spark Application

# Import the `SparkSession` class from the `pyspark.sql` module:
from pyspark.sql import SparkSession

# Use the cmldata module to create a data connection and spark session
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Set environment variables
import os
os.environ["S3_HOME"] = S3_HOME
os.environ["HADOOP_ROOT_LOGGER"] = "ERROR"

# Call the optional `master` method to specify how the application runs:
# * Pass `yarn` to run on the Hadoop cluster via YARN
# * Pass `local` to run in the CML Native Workbench session engine with one thread
# * Pass `local[N]` to run in the CML Native Workbench session engine with $N$ threads
# * Pass `local[*]` to run in the CML Native Workbench session engine with all available
# threads

# Call the optional `appName` method to specify a name for the application.

# Call the required `getOrCreate` method to create the instance.

# **Note:** `spark` is the conventional name for the `SparkSession` instance.

# **Note:** The backslash `\` is the Python line continuation character.

# Access the `version` attribute of the `SparkSession` instance to get the
# Spark version number:
spark.version

# **Note:**  We are using the Cloudera Distribution of Apache Spark.


# ## Reading data into a Spark SQL DataFrame

# Use the `csv` method of the `DataFrameReader` class to read the raw ride data
# from HDFS into a DataFrame:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides/", sep=",", header=True, inferSchema=True)

# **Note:** HDFS is the default file system for Spark in CML Native Workbench.


# ## Examining the schema of a DataFrame

# Call the `printSchema` method to print the schema:
rides.printSchema()

# Access the `columns` attribute to get a list of column names:
rides.columns

# Access the `dtypes` attribute to get a list of column names and data types:
rides.dtypes

# Access the `schema` attribute to get the schema as a instance of the `StructType` class:
rides.schema


# ## Computing the number of rows and columns of a DataFrame

# Call the `count` method to compute the number of rows:
rides.count()

# Pass the list of column names to the Python `len` function to compute the
# number of columns:
len(rides.columns)


# ## Examining a few rows of a DataFrame

# Call the `show` method to print some rows of a DataFrame:
rides.show(5)
rides.show(5, truncate=5)
rides.show(5, vertical=True)

# Call the `head` or `take` method to get a list of `Row` objects from a
# DataFrame:
rides.head(5)
rides.take(5)


# ## Stopping a Spark Application

# Call the `stop` method to stop the application:
spark.stop()

# **Note:** The Spark application will also stop when you stop the CML Native Workbench session
# engine.


# ## Exercises

# (1) Create a new `SparkSession` and configure Spark to run locally with one
# thread.

# (2) Read the raw driver data from HDFS.

# (3) Examine the schema of the drivers DataFrame.

# (4) Count the number of rows of the drivers DataFrame.

# (5) Examine a few rows of the drivers DataFrame.

# (6) **Bonus:** Repeat exercises (2)-(5) with the raw rider data.

# (7) **Bonus:** Repeat exercises (2)-(5) with the raw ride review data.
# **Hint:** Verify the file format before reading the data.

# (8) Stop the SparkSession.


# ## References

# [Apache Spark](https://spark.apache.org/)

# [Spark Documentation](https://spark.apache.org/docs/latest/index.html)

# [Spark Documentation - SQL, DataFrames, and Datasets
# Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

# [Spark Python API - pyspark.sql.SparkSession
# class](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.SparkSession)

# [Spark Python API - pyspark.sql.DataFrame
# class](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame)

# [Using CDS 2.x Powered by Apache
# Spark](https://docs.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_dist_comp_with_Spark.html)
