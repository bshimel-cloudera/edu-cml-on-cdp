# # Processing Text and Fitting and Evaluating Topic Models - Solutions

# Copyright © 2010–2022 Cloudera. All rights reserved.
# Not to be reproduced or shared without prior written 
# consent from Cloudera.


# ## Introduction

# * Topic modeling algorithms such as Latent Dirichlet allocation extract
# themes from a set of text documents

# * Clustering algorithms such as K-means and Gaussian mixture models assume
# that an observation belongs to one and only one cluster

# * Latent Dirichlet allocation assumes that each document belongs to one or
# more topics

# * The number of topics is a hyperparameter

# * Topic models can be used to categorize news articles


# ## Scenario

# In this demonstration we will use latent Dirichlet allocation (LDA) to look
# for topics in the ride reviews.  We will also perform some basic natural
# language processing (NLP) to prepare the data for LDA.


# ## Setup

# None


# ## Create a SparkSession

from pyspark.sql import SparkSession
import cml.data_v1 as cmldata
from env import S3_ROOT, S3_HOME, CONNECTION_NAME

conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()


# ## Load the data

# Read the (clean) ride review data from HDFS:
reviews = spark.read.parquet(S3_ROOT + "/duocar/clean/ride_reviews/")
reviews.head(5)


# ## Extract and transform features

# The ride reviews are not in a form amenable to machine learning algorithms.
# Spark MLlib provides a number of feature extractors and feature transformers
# to preprocess the ride reviews into a form appropriate for modeling.


# ### Parse the ride reviews

# Use the
# [Tokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.Tokenizer)
# class to tokenize the reviews:
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="review", outputCol="words")
tokenized = tokenizer.transform(reviews)
tokenized.drop("ride_id").head(5)

# Note that punctuation is not being handled properly.  Use the
# [RegexTokenizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.RegexTokenizer)
# class to improve the tokenization:
from pyspark.ml.feature import RegexTokenizer
tokenizer = RegexTokenizer(inputCol="review", outputCol="words", gaps=False, pattern="[a-zA-Z-']+")
tokenized = tokenizer.transform(reviews)
tokenized.drop("ride_id").head(5)

# The arguments `gaps` and `pattern` are set to extract words consisting of
# lowercase letters, uppercase letters, hyphens, and apostrophes.

# Define a function to plot a word cloud:
def plot_word_cloud(df, col):
  # Compute the word count:
  from pyspark.sql.functions import explode
  word_count = df.select(explode(col).alias("word")).groupBy("word").count().collect()
  # Generate the word cloud image:
  from wordcloud import WordCloud
  wordcloud = WordCloud(random_state=12345).fit_words(dict(word_count))
  # Display the word cloud image:
  import matplotlib.pyplot as plt
  plt.imshow(wordcloud, interpolation="bilinear")
  plt.axis("off")

# Plot the word cloud:
plot_word_cloud(tokenized, "words")


# ### Remove common (stop) words from each review

# Note that the ride reviews contain a number of common words such as "the"
# that we do not expect to be relevant.
# Use the
# [StopWordsRemover](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.StopWordsRemover)
# class to remove these so-called *stop words*:
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="words", outputCol="relevant_words")
remover.getStopWords()[:10]
removed = remover.transform(tokenized)
removed.select("words", "relevant_words").head(5)

# Plot the word cloud:
plot_word_cloud(removed, "relevant_words")


# ### Count the frequency of words in each review

# Use the
# [CountVectorizer](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.feature.CountVectorizer)
# class to compute the term frequency:
from pyspark.ml.feature import CountVectorizer
vectorizer = CountVectorizer(inputCol="relevant_words", outputCol="word_count_vector", vocabSize=100)

# The `fit` method computes the top $N$ words where $N$ is set via the
# `vocabSize` hyperparameter:
vectorizer_model = vectorizer.fit(removed)
vectorizer_model.vocabulary[:10]

# The `transform` method counts the number of times each vocabulary word
# appears in each review:
vectorized = vectorizer_model.transform(removed)
vectorized.select("words", "word_count_vector").head(5)

# **Note:** The resulting word count vector is stored as a
# [SparseVector](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.linalg.SparseVector).


# ## Specify and fit a topic model using latent Dirichlet allocation (LDA)

# Use the `LDA` class to specify an LDA model:
from pyspark.ml.clustering import LDA
lda = LDA(featuresCol="word_count_vector", k=2, seed=23456)

# Use the `explainParams` method to examine additional hyperparameters:
print(lda.explainParams())

# Use the `fit` method to fit the LDA model:
lda_model = lda.fit(vectorized)

# The resulting model is an instance of the `LDAModel` class:
type(lda_model)


# ## Examine the LDA topic model fit

lda_model.logLikelihood(vectorized)
lda_model.logPerplexity(vectorized)


# ## Examine the LDA topic model

# Examine the estimated distribution of topics:
lda_model.estimatedDocConcentration()

# Examine the estimated distribution of words for each topic:
lda_model.topicsMatrix()

# Examine the most important words in each topic:
lda_model.describeTopics().head(5)

# Plot the terms and weights for each topic:
def plot_topics(model, n_terms, vocabulary):
  import matplotlib.pyplot as plt
  import seaborn as sns
  rows = model.describeTopics(n_terms).collect()
  for row in rows:
    title = "Topic %s" % row["topic"]
    x = row["termWeights"]
    y = [vocabulary[i] for i in row["termIndices"]]
    plt.figure()
    sns.barplot(x, y)
    plt.title(title)
    plt.xlabel("Weight")
plot_topics(lda_model, 5, vectorizer_model.vocabulary)

 
# ## Apply the topic model

predictions = lda_model.transform(vectorized)
predictions.select("review", "topicDistribution").head(5)


# ## Exercises

# (1) Fit an LDA model with $k=3$ topics.

# * Use the `setK` method to change the number of topics for the `lda` instance:

lda.setK(3)

# * Use the `fit` method to fit the LDA model to the `vectorized` DataFrame:

lda_model = lda.fit(vectorized)

# * Use the `plot_topics` function to examine the topics:

plot_topics(lda_model, 5, vectorizer_model.vocabulary)

# * Use the `transform` method to apply the LDA model to the `vectorized` DataFrame:

predictions = lda_model.transform(vectorized)

# * Print out a few rows of the transformed DataFrame:

predictions.select("review", "topicDistribution").head(5) 

# (2) Use the `NGram` transformer to generate pairs of words (bigrams) from the tokenized reviews.

# * Import the `NGram` class from the `pyspark.ml.feature` module:

from pyspark.ml.feature import NGram

# * Create an instance of the `NGram` class:

ngramer = NGram(inputCol="words", outputCol="bigrams", n=2)

# * Use the `transform` method to apply the `NGram` instance to the `tokenized` DataFrame:

ngramed = ngramer.transform(tokenized)

# * Print out a few rows of the transformed DataFrame:

ngramed.printSchema()
ngramed.select("words", "bigrams").head(5)


# ## References

# [Wikipedia - Topic model](https://en.wikipedia.org/wiki/Topic_model)

# [Wikipedia - Latent Dirichlet
# allocation](https://en.wikipedia.org/wiki/Latent_Dirichlet_allocation)

# [Spark Documentation - Latent Dirichlet
# allocation](http://spark.apache.org/docs/latest/ml-clustering.html#latent-dirichlet-allocation-lda)

# [Spark Python API - pyspark.ml.clustering.LDA
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDA)

# [Spark Python API - pyspark.ml.clustering.LDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LDAModel)

# [Spark Python API - pyspark.ml.clustering.LocalLDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.LocalLDAModel)

# [Spark Python API - psypark.ml.clustering.DistributedLDAModel
# class](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.clustering.DistributedLDAModel)


# ## Stop the SparkSession

spark.stop()
