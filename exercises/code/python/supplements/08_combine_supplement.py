# # Combining and Splitting DataFrames - Supplement

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Contents

# * Creating data using a cross join
# * Joining the demographic and weather data


# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Creating data using a cross join

# Create a DataFrame of years:
years = spark.createDataFrame([("2016", ), ("2017", ), ("2018", )], schema=["year"])

# Create a DataFrame of quarters:
quarters = spark.createDataFrame([("Q1", ), ("Q2", ), ("Q3", ), ("Q4", )], schema=["quarter"])

# Create all combinations of year and quarter:
from pyspark.sql.functions import concat
years.crossJoin(quarters).withColumn("yyyyqq", concat("year", "quarter")).show()


# ## Joining the demographic and weather data

# Read the joined ride data:
joined = spark.read.parquet(S3_ROOT + "/duocar/joined/")

# Read the clean demographic data for drivers:
drivers = spark.read.parquet(S3_ROOT + "/duocar/clean/demographics") \
  .withColumnRenamed("block_group", "driver_block_group") \
  .withColumnRenamed("median_income", "driver_median_income") \
  .withColumnRenamed("median_age", "driver_median_age")

# Join demographic data for drivers:
from pyspark.sql.functions import substring
joined_drivers = joined \
  .join(drivers, substring(joined.driver_home_block, 1, 12) == drivers.driver_block_group, "left_outer")

# Check join:
joined_drivers.select("driver_home_block", "driver_block_group", "driver_median_income").show(5)

# Read the clean demographic data for riders:
riders = spark.read.parquet(S3_ROOT + "/duocar/clean/demographics") \
  .withColumnRenamed("block_group", "rider_block_group") \
  .withColumnRenamed("median_income", "rider_median_income") \
  .withColumnRenamed("median_age", "rider_median_age")

# Join demographic data for riders:
joined_riders = joined_drivers \
  .join(riders, substring(joined_drivers.rider_home_block, 1, 12) == riders.rider_block_group, "left_outer")

# Check join:
joined_riders.select("rider_home_block", "rider_block_group", "rider_median_income").show(5)

# Read the clean weather data:
weather = spark.read.parquet(S3_ROOT + "/duocar/clean/weather")

# Note that there is only one weather station in Fargo, North Dakota:
weather.groupBy("Station_ID").count().show()

# Join weather data on date:
from pyspark.sql.functions import to_date
joined_all = joined_riders \
  .join(weather, to_date(joined.date_time) == weather.Date, "left_outer")
  
# Check join:
joined_all.select("date_time", "Date", "Min_TemperatureF", "Max_TemperatureF").show(5)

# Examine the final schema:
joined_all.printSchema()

# Examine the data:
import pandas as pd
pd.options.display.html.table_schema=True
joined_all.limit(5).toPandas()


# ## Cleanup

# Stop the SparkSession:
spark.stop()
