# Script to make joined versions of DuoCar data in Parquet files

# Copyright 2017 Cloudera, Inc.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import cml.data_v1 as cmldata
import sys

print("Executing: " + __file__)

CONNECTION_NAME = sys.argv[1]
HDFS_ROOT = sys.argv[2]

print("CONNECTION_NAME = " + CONNECTION_NAME)
print("HDFS_ROOT = " + HDFS_ROOT)

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# join rides, drivers, riders, ride_reviews

# load drivers dataframe
drivers = spark.read.parquet(HDFS_ROOT + '/duocar/clean/drivers/')

# rename some columns to prefix with 'driver_'
# other possible ways to do this: https://stackoverflow.com/questions/34077353/how-to-change-dataframe-column-names-in-pyspark
drivers = drivers.withColumnRenamed('id', 'driver_id')
drivers = drivers.withColumnRenamed('birth_date', 'driver_birth_date')
drivers = drivers.withColumnRenamed('start_date', 'driver_start_date')
drivers = drivers.withColumnRenamed('first_name', 'driver_first_name')
drivers = drivers.withColumnRenamed('last_name', 'driver_last_name')
drivers = drivers.withColumnRenamed('gender', 'driver_gender')
drivers = drivers.withColumnRenamed('ethnicity', 'driver_ethnicity')
drivers = drivers.withColumnRenamed('student', 'driver_student')
drivers = drivers.withColumnRenamed('home_block', 'driver_home_block')
drivers = drivers.withColumnRenamed('home_lat', 'driver_home_lat')
drivers = drivers.withColumnRenamed('home_lon', 'driver_home_lon')
drivers = drivers.withColumnRenamed('rides', 'driver_rides')
drivers = drivers.withColumnRenamed('stars', 'driver_stars')

# load riders dataframe
riders = spark.read.parquet(HDFS_ROOT + '/duocar/clean/riders/')

# rename some columns to prefix with 'rider_'
riders = riders.withColumnRenamed('id', 'rider_id')
riders = riders.withColumnRenamed('birth_date', 'rider_birth_date')
riders = riders.withColumnRenamed('start_date', 'rider_start_date')
riders = riders.withColumnRenamed('first_name', 'rider_first_name')
riders = riders.withColumnRenamed('last_name', 'rider_last_name')
riders = riders.withColumnRenamed('gender', 'rider_gender')
riders = riders.withColumnRenamed('ethnicity', 'rider_ethnicity')
riders = riders.withColumnRenamed('student', 'rider_student')
riders = riders.withColumnRenamed('home_block', 'rider_home_block')
riders = riders.withColumnRenamed('home_lat', 'rider_home_lat')
riders = riders.withColumnRenamed('home_lon', 'rider_home_lon')
riders = riders.withColumnRenamed('work_lat', 'rider_work_lat')
riders = riders.withColumnRenamed('work_lon', 'rider_work_lon')

# load rides dataframe
rides = spark.read.parquet(HDFS_ROOT + '/duocar/clean/rides/')

# rename id column to 'ride_id'
rides = rides.withColumnRenamed('id', 'ride_id')

# load ride_reviews dataframe
ride_reviews = spark.read.parquet(HDFS_ROOT + '/duocar/clean/ride_reviews/')

# join the data
rides_drivers = rides.join(drivers, 'driver_id', 'left_outer')
rides_drivers_riders = rides_drivers.join(riders, 'rider_id', 'left_outer')
rides_drivers_riders_reviews = rides_drivers_riders.join(ride_reviews, 'ride_id', 'left_outer')
joined = rides_drivers_riders_reviews

# write the data
joined.write.parquet(path=HDFS_ROOT + '/duocar/joined/', mode='overwrite')


# now also join weather and demographics

# load demographics dataframe
demographics = spark.read.parquet(HDFS_ROOT + '/duocar/clean/demographics/')

# drop the median_age column (we know actual driver and rider ages so we don't need to know median ages)
demographics = demographics.drop("median_age")

# make copies of the demographics data for the riders and drivers
driver_demographics = demographics
rider_demographics = demographics

# rename some columns to prefix with 'rider_' or 'driver_'
driver_demographics = driver_demographics.withColumnRenamed('block_group', 'driver_block_group')
driver_demographics = driver_demographics.withColumnRenamed('median_income', 'driver_median_income')
rider_demographics = rider_demographics.withColumnRenamed('block_group', 'rider_block_group')
rider_demographics = rider_demographics.withColumnRenamed('median_income', 'rider_median_income')

# get driver and rider block groups in joined data
joined = joined.withColumn('driver_block_group', col('driver_home_block').substr(0, 12))
joined = joined.withColumn('rider_block_group', col('rider_home_block').substr(0, 12))

# join the demographics data
joined_driver_demo = joined.join(driver_demographics, 'driver_block_group', 'left_outer')
joined_driver_demo_rider_demo = joined_driver_demo.join(rider_demographics, 'rider_block_group', 'left_outer')

# drop unneeded columns
joined_driver_demo_rider_demo = joined_driver_demo_rider_demo.drop('driver_block_group').drop('rider_block_group')

# load weather dataframe
weather = spark.read.parquet(HDFS_ROOT + '/duocar/clean/weather/')

# get the day from date_time in the joined data
joined_driver_demo_rider_demo = joined_driver_demo_rider_demo.withColumn('Date', col('date_time').substr(0, 10))

# join the weather data
joined_driver_demo_rider_demo_weather = joined_driver_demo_rider_demo.join(weather, 'Date', 'left_outer')

# drop unneeded columns
joined_driver_demo_rider_demo_weather = joined_driver_demo_rider_demo_weather.drop('Date').drop('Station_ID')
joined_all = joined_driver_demo_rider_demo_weather

# write the data
joined_all.write.parquet(path=HDFS_ROOT + '/duocar/joined_all/', mode='overwrite')

# disconnect
spark.stop()
