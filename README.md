# us-election-twitter-stats

## Introduction
PySpark script to show some statistics about the 2020 Presidential US Election tweets. 

Statistics covered:
* Number of tweets by continent
* Number of tweets by country
* Number of tweets by state
* Number of tweets by city
* Most retweeted tweet
* Tweet with most likes
* Most active day and hour

The scripts was writter in Python with Pyspark libraries and uses a dataset from Kaggle. The dataset is split in two files, one for each candidate.

## Datasets

* Joe Biden Tweets   : https://www.kaggle.com/manchunhui/us-election-2020-tweets?select=hashtag_joebiden.csv
* Donald Trump Tweets: https://www.kaggle.com/manchunhui/us-election-2020-tweets?select=hashtag_donaldtrump.csv

## Used tools
* Hadoop 3.2.1
* Spark 3.0.1
* Python 3.6.9

## Environmental Variables
 * JAVA_HOME - Home directory for the target java version. 
  `export JAVA_HOME = /usr/lib/jvm/java-8-openjdk-amd64`
 * HADOOP_HOME - Hadoop's installation home directory. 
  `export HADOOP_HOME = /home/hadoop/hadoop`
 * SPARK_HOME - Spark's installation home directory. 
  `export SPARK_HOME = $HOME/spark-3.0.1-bin-hadoop3.2`
 * HADOOP_CONF_DIR - Hadoop's configuration directory.
  `export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop`
 * PATH - System's standard environmental variable.
  `export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin`
 * PYTHONIOENCODING - Enconding code to be used by Python
  `export PYTHONIOENCODING=utf8`

## Starting the enviroment

**USAGE:**  
spark-submit uselection-tweet-stats.py

The code execution will print all the results to the terminal.

Enjoy. And leave your contribution... :)
