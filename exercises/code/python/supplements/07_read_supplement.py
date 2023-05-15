# # Reading and Writing DataFrames - Supplement

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Contents

# * Reading from and writing to a SQLite database
# * Generating a DataFrame


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("read_supplement").getOrCreate()


# ## Reading from and writing to a SQLite database

# Add the JDBC driver to the Spark driver and executor Java classpaths by
# adding the following line to the `spark-defaults.conf` file:
# ```
# spark.jars /home/cdsw/resources/sqlite-jdbc-3.23.1.jar
# ```

# Specify the SQLite database parameters:
url = "jdbc:sqlite:/home/cdsw/data/duocar.db"
properties = {"driver": "org.sqlite.JDBC"}

# Use the `jdbc()` method of the `DataFrameReader` class to read from the
# SQLite database:
scientists = spark.read.jdbc(url=url, table="data_scientists", properties=properties)
scientists.printSchema()
scientists.show()

offices = spark.read.jdbc(url=url, table="offices", properties=properties)
offices.printSchema()
offices.show()

# Use the `jdbc()` method of the `DataFrameWriter` class to write to the
# SQLite database:
scientists_enhanced = scientists.join(offices, "office_id", "left_outer").drop("office_id")
scientists_enhanced.printSchema()
scientists_enhanced.show()
scientists_enhanced.write.jdbc(url=url, table="scientists_enhanced", mode="overwrite", properties=properties)

# **Important:** This code will fail when running Spark via YARN since the
# worker nodes can not access the SQLite database hosted on the CML nodes.


# ## Generating a DataFrame

# Sometimes we need to generate a Spark DataFrame from scratch, for example,
# for testing purposes.

# Use the `range` method to generate a sequence of integers and add new
# columns as appropriate.
spark.range(1000).show(5)

# Use the `rand` function to generate a uniform random variable:
from pyspark.sql.functions import rand
df_uniform = spark \
  .range(1000) \
  .withColumn("uniform", rand(12345))
df_uniform.show(5)
df_uniform.describe("uniform").show()

# Or a Bernoulli random variable with $p = 0.25$:
df_bernoulli = spark \
  .range(1000) \
  .withColumn("bernoulli", (rand(12345) < 0.25).cast("int"))
df_bernoulli.show(5)
df_bernoulli.groupby("bernoulli").count().show()

# Use the `randn` function to generate a normal random variable:
from pyspark.sql.functions import randn
df_normal = spark.range(1000).withColumn("normal", 42 + 2 * randn(54321))
df_normal.show(5)
df_normal.describe("normal").show()


# ## Cleanup

# Stop the SparkSession:
spark.stop()
