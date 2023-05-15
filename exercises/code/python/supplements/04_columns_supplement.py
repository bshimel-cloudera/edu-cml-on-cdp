# # Transforming DataFrame Columns - Supplement

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Contents

# * Using Hive functions


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Using Hive functions

# List the available Hive functions via `spark.sql`:
spark.sql("SHOW FUNCTIONS").head(20)

# List the available Hive functions via `spark.catalog`:
spark.catalog.listFunctions()[:20]

# Describe the `percentile` function:
spark.sql("DESCRIBE FUNCTION percentile").head(20)
spark.sql("DESCRIBE FUNCTION EXTENDED percentile").head(20)

# Read the ride data:
rides = spark.read.csv(S3_ROOT + "/duocar/raw/rides", header=True, inferSchema=True)

# Create a temporary view:
rides.createOrReplaceTempView("rides_view")

# Use the function via the SQL API:
spark.sql("SELECT percentile(distance, 0.5) AS median FROM rides_view").show()
spark.sql("SELECT percentile(distance, array(0.0, 0.25, 0.5, 0.75, 1.0)) AS percentiles FROM rides_view").show(truncate=False)

# Use the function via the DataFrames API:
rides.selectExpr("percentile(distance, 0.5) AS median").show()
from pyspark.sql.functions import expr
rides.select(expr("percentile(distance, array(0.0, 0.25, 0.5, 0.75, 1.0)) AS percentiles")).show(truncate=False)

# **Note:** The normal select statement does not work:
# ```python
# rides.select(percentile("distance", 0.5).alias("median")).show()
# ```

# Use the SQL API for more complex queries:
spark.sql("SELECT service, count(distance), percentile(distance, 0.5) AS median FROM rides_view GROUP BY service").show()


# ## Cleanup

# Stop the SparkSession:
spark.stop()
