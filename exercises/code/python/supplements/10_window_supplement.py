# # Window Functions - Supplement

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Contents
# * Examples of various window functions
# * Compute mean star rating over last five rides

# ## Setup

# Create a SparkSession:
from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# Read the enhanced ride data from HDFS:
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Examples of various window functions

# Create a simple DataFrame:
data = [(100, ), (95, ), (95, ), (88, ), (73, ), (73, )]
df = spark.createDataFrame(data, ["score"])
df.show()

# Create a simple window specification:
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
ws = Window.orderBy(desc("score"))

from pyspark.sql.functions import row_number, cume_dist, ntile
df.select("score", row_number().over(ws).alias("row_number")).show()
df.select("score", cume_dist().over(ws).alias("cume_dist")).show()
df.select("score", ntile(2).over(ws).alias("ntile(2)")).show()

from pyspark.sql.functions import rank, dense_rank, percent_rank
df.select("score", rank().over(ws).alias("rank"), dense_rank().over(ws).alias("dense_rank")).show()
df.select("score", percent_rank().over(ws).alias("percent_rank")).show()

from pyspark.sql.functions import lag, lead
df.select("score", lag("score", count=1).over(ws).alias("lag"), lead("score", count=2).over(ws).alias("lead")).show()


# ## Compute mean star rating over last five rides

ws = Window.partitionBy("driver_id").orderBy("date_time").rowsBetween(-4, 0)

from pyspark.sql.functions import count, mean, last
rides \
  .select("driver_id", "date_time", "star_rating", mean("star_rating").over(ws).alias("moving_average")) \
  .groupby("driver_id") \
  .agg(count("driver_id"), mean("star_rating"), last("moving_average")) \
  .show(30)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
