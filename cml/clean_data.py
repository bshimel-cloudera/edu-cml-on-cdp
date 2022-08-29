# Script to make clean version of DuoCar data in Parquet files

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

### drivers #################################################################################################

schema = StructType([
    StructField("id", StringType()),
    StructField("birth_date", DateType()),
    StructField("start_date", DateType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("ethnicity", StringType()),
    StructField("student", IntegerType()),
    StructField("home_block", StringType()),
    StructField("home_lat", DecimalType(9,6)),
    StructField("home_lon", DecimalType(9,6)),
    StructField("vehicle_make", StringType()),
    StructField("vehicle_model", StringType()),
    StructField("vehicle_year", IntegerType()),
    StructField("vehicle_color", StringType()),
    StructField("vehicle_grand", IntegerType()),
    StructField("vehicle_noir", IntegerType()),
    StructField("vehicle_elite", IntegerType()),
    StructField("rides", IntegerType()),
    StructField("stars", IntegerType())
])
print("Loading from " + HDFS_ROOT + '/duocar/raw/drivers/')
drivers = spark.read.format('csv').option('sep', ',').option('header', True).schema(schema).load(HDFS_ROOT + '/duocar/raw/drivers/')
drivers = drivers.withColumn('student', col('student').cast('Boolean'))
drivers = drivers.withColumn('vehicle_grand', col('vehicle_grand').cast('Boolean'))
drivers = drivers.withColumn('vehicle_noir', col('vehicle_noir').cast('Boolean'))
drivers = drivers.withColumn('vehicle_elite', col('vehicle_elite').cast('Boolean'))

drivers.write.parquet(path=HDFS_ROOT + "/duocar/clean/drivers/", mode="overwrite")


### riders ##################################################################################################

schema = StructType([
    StructField("id", StringType()),
    StructField("birth_date", DateType()),
    StructField("start_date", DateType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("ethnicity", StringType()),
    StructField("student", IntegerType()),
    StructField("home_block", StringType()),
    StructField("home_lat", DecimalType(9,6)),
    StructField("home_lon", DecimalType(9,6)),
    StructField("work_lat", DecimalType(9,6)),
    StructField("work_lon", DecimalType(9,6))
])
riders = spark.read.format('csv').option('sep', ',').option('header', True).schema(schema).load(HDFS_ROOT + '/duocar/raw/riders/')
riders = riders.withColumn('student', col('student').cast('Boolean'))

riders.write.parquet(path=HDFS_ROOT + "/duocar/clean/riders/", mode="overwrite")


### rides ###################################################################################################

schema = StructType([
    StructField("id", StringType()),
    StructField("driver_id", StringType()),
    StructField("rider_id", StringType()),
    StructField("date_time", StringType()),
    StructField("utc_offset", IntegerType()),
    StructField("service", StringType()),
    StructField("origin_lat", DecimalType(9,6)),
    StructField("origin_lon", DecimalType(9,6)),
    StructField("dest_lat", DecimalType(9,6)),
    StructField("dest_lon", DecimalType(9,6)),
    StructField("distance", IntegerType()),
    StructField("duration", IntegerType()),
    StructField("cancelled", IntegerType()),
    StructField("star_rating", IntegerType())
])
rides = spark.read.format('csv').option('sep', ',').option('header', True).schema(schema).load(HDFS_ROOT + '/duocar/raw/rides/')
rides = rides.withColumn('date_time', concat(col('date_time'), lit(':00')).cast('Timestamp'))
rides = rides.withColumn('cancelled', col('cancelled').cast('Boolean'))
rides = rides.withColumn('service', when(isnull(col('service')), lit('Car')).otherwise(col('service')))

rides.write.parquet(path=HDFS_ROOT + "/duocar/clean/rides/", mode="overwrite")


### ride_routes #############################################################################################

schema = StructType([
    StructField("ride_id", StringType()),
    StructField("step_id", StringType()),
    StructField("lat", DecimalType(9,6)),
    StructField("lon", DecimalType(9,6)),
    StructField("distance", IntegerType()),
    StructField("duration", IntegerType())
])
ride_routes = spark.read.format('csv').option('sep', '\t').option('header', False).schema(schema).load(HDFS_ROOT + '/duocar/raw/ride_routes/')

ride_routes.write.parquet(path=HDFS_ROOT + "/duocar/clean/ride_routes/", mode="overwrite")


### ride_reviews ############################################################################################

schema = StructType([
    StructField("ride_id", StringType()),
    StructField("review", StringType())
])
ride_reviews = spark.read.format('csv').option('sep', '\t').option('header', False).schema(schema).load(HDFS_ROOT + '/duocar/raw/ride_reviews/')

ride_reviews.write.parquet(path=HDFS_ROOT + "/duocar/clean/ride_reviews/", mode="overwrite")


### weather ############################################################################################

schema = StructType([
    StructField("Station_ID", StringType()),
    StructField("Date", DateType()),
    StructField("Max_TemperatureF", IntegerType()),
    StructField("Mean_TemperatureF", IntegerType()),
    StructField("Min_TemperatureF", IntegerType()),
    StructField("Max_Dew_PointF", IntegerType()),
    StructField("MeanDew_PointF", IntegerType()),
    StructField("Min_DewpointF", IntegerType()),
    StructField("Max_Humidity", IntegerType()),
    StructField("Mean_Humidity", IntegerType()),
    StructField("Min_Humidity", IntegerType()),
    StructField("Max_Sea_Level_PressureIn", DecimalType(4,2)),
    StructField("Mean_Sea_Level_PressureIn", DecimalType(4,2)),
    StructField("Min_Sea_Level_PressureIn", DecimalType(4,2)),
    StructField("Max_VisibilityMiles", IntegerType()),
    StructField("Mean_VisibilityMiles", IntegerType()),
    StructField("Min_VisibilityMiles", IntegerType()),
    StructField("Max_Wind_SpeedMPH", IntegerType()),
    StructField("Mean_Wind_SpeedMPH", IntegerType()),
    StructField("Max_Gust_SpeedMPH", IntegerType()),
    StructField("PrecipitationIn", StringType()),
    StructField("CloudCover", IntegerType()),
    StructField("Events", StringType()),
    StructField("WindDirDegrees", IntegerType())
])
weather = spark.read.format('csv').option('sep', ',').option('header', True).schema(schema).load(HDFS_ROOT + '/duocar/raw/weather/')
weather = weather.withColumn('PrecipitationIn', when(col('PrecipitationIn') == "T", "0.00").otherwise(col('PrecipitationIn')))
weather = weather.withColumn('PrecipitationIn', col('PrecipitationIn').cast('Decimal(4,2)'))

weather.write.parquet(path=HDFS_ROOT + "/duocar/clean/weather/", mode="overwrite")


### demographics ############################################################################################

schema = StructType([
    StructField("block_group", StringType()),
    StructField("median_income", IntegerType()),
    StructField("median_age", DecimalType(3,1))
])
demographics = spark.read.format('csv').option('sep', '\t').option('header', True).schema(schema).load(HDFS_ROOT + '/duocar/raw/demographics/')

demographics.write.parquet(path=HDFS_ROOT + "/duocar/clean/demographics/", mode="overwrite")


###

spark.stop()
