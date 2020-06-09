Overview
========
This is a small program to recommend movies. In this project, I have learned the basics of Scala and Spark, how to use RDDs and transformations although
there are easier and efficients ways to do it, like sparkSQL or MLlib.

I used a collaborative filtering system with a cosine similarity to recommend movies based on rating and genre similarity.

Feel free to use this project.


Prerequisites
============

Before you continue, ensure you met the following requirements:

 * You have installed Scala.
 * You have installed Spark.
 

Usage
=========

Search for recommendations:

```sh
$ cd SparkScala
$ spark-submit --class spark.MovieRecommender MovieRecommender.jar 1 0.90 50
```
The last three digits are (movieID, cosineSimilarityScore, ocurrences)
 * movieID: you can see the id of the movies at the ml-100k/u.item file
 * cosineSimilarityScore: a range between [0, 1]
 * ocurrences: number of times people rated both movies
