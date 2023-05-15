# # Fitting and Evaluating Recommender Models

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Recommender Models

# * *Recommender models* help users find relevant items in a very large set
# (when they do not know what they are looking for):
#   * Products
#   * Books
#   * Music
#   * Movies

# * A recommender model is usually part of a larger *recommender system* that
# includes a user interface to present recommendations and gather user
# preferences.

# * There are two main classes of recommender algorithms:
#   * Content based
#   * Collaborative filtering

# * *Collaborative filtering* algorithms recommend items based on the
# preferences of "similar" users.
#   * Explicit preferences (e.g., star rating)
#   * Implicit preferences (e.g., artist playcount)


# * Collaborative filtering algorithms fall somewhere between supervised and
# unsupervised learning algorithms.
#   * Some item preferences are known
#   * Most item preferences are unknown

# * Spark MLlib provides the *alternating least squares* (ALS) algorithm for
# collaborative filtering

# * The ALS algorithm takes a DataFrame with user, item, and rating columns as input.


# ## Scenario

# Le DuoCar is owned by a private equity (PE) firm.  This PE firm has a
# streaming music company in its portfolio called Earcloud. Earcloud is having
# a hard time hiring data scientists in this competitive environment, so the PE
# firm has asked the DuoCar data science team to help the Earcloud analysts and
# engineers develop a musical artist recommender system.  Earcloud has provided
# Apache web server logs from which we can extract the user's listening
# behavior.  We will use this behavior to fit and evaluate a collaborative
# filtering model.


# ## Setup

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract


# ## Create a SparkSession

import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )


# ## Read the data

# Read the Apache access logs from Earcloud:
access_logs = spark.read.text(S3_ROOT + "/duocar/earcloud/apache_logs/")
access_logs.printSchema()
access_logs.head(5)
access_logs.count()


# ## Prepare the data for modeling

# Create user, artist, and playcount columns:
pattern = "^.*artist=(.*)&playcount=(.*) HTTP.*USER=(.*)\".*"
playcounts = access_logs.filter(col("value").contains("/listen?")) \
  .withColumn("user", regexp_extract("value", pattern, 3).cast("integer")) \
  .withColumn("artist", regexp_extract("value", pattern, 1).cast("integer")) \
  .withColumn("playcount", regexp_extract("value", pattern, 2).cast("integer")) \
  .persist()
playcounts.head(20)

# Note that the playcount column includes some negative, binary values:
playcounts.filter("playcount < 0").show()

# Fix the playcount column:
from pyspark.sql.functions import when, abs, conv
playcounts_fixed = playcounts.withColumn("playcount_fixed", when(col("playcount") < 0, conv(abs(col("playcount")), 2, 10).cast("integer")).otherwise(col("playcount")))
playcounts_fixed.printSchema()                            
playcounts_fixed.filter("playcount < 0").select("playcount", "playcount_fixed").show()

# **Note:** The `conv()` function returns a string.

# Select the modeling data:
recommendation_data = playcounts_fixed \
    .select("user", "artist", "playcount_fixed") \
    .withColumnRenamed("playcount_fixed", "playcount")
recommendation_data.show()

# Save the modeling data:
recommendation_data.write.parquet(S3_HOME + "/data/recommendation_data/", mode="overwrite")


# ## Create train and test datasets

(train, test) = recommendation_data.randomSplit(weights=[0.75, 0.25], seed=12345)


# ## Specify and fit an ALS model

from pyspark.ml.recommendation import ALS
als = ALS(userCol="user", itemCol="artist", ratingCol="playcount", implicitPrefs=True, seed=23456)
print(als.explainParams())
als.setColdStartStrategy("drop")
als_model = als.fit(train)


# ## Examine the ALS model

als_model.userFactors.head(5)
als_model.itemFactors.head(5)

# **Note:** Some artists are not represented in the training data:
als_model.userFactors.count()
als_model.itemFactors.count()


# ## Apply the model

train_predictions = als_model.transform(train)
train_predictions.printSchema()
train_predictions.sort("user").show()


# ## Evaluate the model

# Evaluate the model on the train dataset:
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="playcount", metricName="rmse")
evaluator.evaluate(train_predictions)

# Evaluate the model on the test dataset:
test_predictions = als_model.transform(test)
evaluator.evaluate(test_predictions)


# ## Generate recommendations

# Recommend the top $n$ items for each user:
als_model.recommendForAllUsers(5).sort("user").head(5)

# Recommend the top $n$ users for each item:
als_model.recommendForAllItems(5).sort("artist").head(5)

# Generate recommendations for a subset of users or items by using the
# following methods:
#```python
#als_model.recommendForUserSubset
#als_model.recommendForItemSubset
#```


# ## Exercises

# None


# ## References

# [Spark Python API - pyspark.ml.recommendation.ALS
# class](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALS)

# [Spark Python API - pyspark.ml.recommendation.ALSModel
# class](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.recommendation.ALSModel)


# ## Cleanup

# Stop the SparkSession:
spark.stop()
