# README #

This is a starter project for 'Dive into data with Apache Spark' workshop on Codepot 2015

### Prerequisites ###

* Clone this repo https://github.com/codepot-spark/spark-workshop
* Get an IntelliJ Idea with Scala support installed https://www.jetbrains.com/idea/download/

### Starter project structure ###

This is a typical sbt project with spark as a dependency and a few skeleton classes to help you get started.

### How to run my job? ###

You can execute tests, e.g. `org.codepot.jobs.ExampleSparkJobSpec`

type `./sbt tests` to run all tests

type `./sbt 'test-only ./sbt test-only org.codepot.jobs.ExampleSparkJobSpec'`

### How to write my own job? ###

You can follow an example of `org.codepot.jobs.Example`

### Demo ###

Word count on some plain text data
   - showing spark
        - in a standalone program
        - in a repl
   - API basics

### Data ###

In /src/test/resources/ml-1m you have data from movielens database about movies, users and their ratings.
It is a sample of 1M ratings, if you want more - you can download 20M sample from http://grouplens.org/datasets/movielens/ and put it in the same structure as ml-1m
For a description of the data and its format see /src/test/resources/ml-1m/README

### Tasks ###

We provided an integration test case for each task, you can use it if you want, but that's not mandatory.

1. Word count on movie titles (org.codepot.spark.jobs.CountWordsInMovieTitlesJobSpec)
    - from movies database extract movie titles and do a word count
2. Word count on movie titles with SQL (org.codepot.spark.jobs.CountWordsInMovieTitlesJobSpec)
   - same as in 1. but using Spark SQL
3. Best rated movies
    a) Top 10 movies with highest average rating (movie must have at least 5 ratings from men and 5 ratings from women) (org.codepot.spark.jobs.BestRatedJobSpec)
    b) Top 10 'male'/'female' movies (movies with biggest difference between average male rating and average female rating) (org.codepot.spark.jobs.BestMoviesForGenderSpec)

Optional:
4. Find favourite movie genres for women and men - find the list of favourite movie genres per gender (sorry, no spec for this yet)
6. Movie rating prediction - predict the rating for a given user and movie. (you can start from org.codepot.spark.jobs.RatingPredictionSpec and write your own model)




