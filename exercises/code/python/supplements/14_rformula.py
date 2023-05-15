# # The RFormula Transformer

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# The RFormula Transformer will look familiar to R users.  It can be useful in
# some cases, for example, when you have many categorical features.  In this
# module we demonstrate the basic functionality of the RFormula Transformer.


# ## Setup

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = (
            SparkSession.builder.appName(conn.app_name)
            .config("spark.sql.hive.hwc.execution.mode", "spark")
            .config("spark.yarn.access.hadoopFileSystems", conn.hive_external_dir)
            .getOrCreate()
        )
rides = spark.read.parquet(S3_ROOT + "/duocar/joined/")


# ## Example

from pyspark.ml.feature import RFormula

formula = "star_rating ~ vehicle_year + vehicle_color"

rformula = RFormula(formula=formula)

rformula_model = rformula.fit(rides)

transformed = rformula_model.transform(rides)

transformed.select("vehicle_year", "vehicle_color", "star_rating", "features", "label").show(truncate=False)

# **Question:** How do we get the mapping for `vehicle_color`?


# ## References

# [RFormula class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RFormula)

# [RFormulaModel class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RFormulaModel)

# [R formula](http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html)


# ## Cleanup

spark.stop()
