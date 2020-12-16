# us-election-twitter-stats
PySpark script to show some statistics about the 2020 Presidential US Election tweets. 

Statistics covered:
* Number of tweets by continent
* Number of tweets by country
* Number of tweets by state
* Number of tweets by city
* Most retweeted tweet
* Tweet with most likes
* Most active day and hour

The scripts was writter in Python with Pyspark libraries and uses a dataset from Kaggle which you can find down below. The dataset is split in two files, one for each candidate.

* Joe Biden Tweets   : https://www.kaggle.com/manchunhui/us-election-2020-tweets?select=hashtag_joebiden.csv
* Donald Trump Tweets: https://www.kaggle.com/manchunhui/us-election-2020-tweets?select=hashtag_donaldtrump.csv

Once you have Hadoop up and running, put the CSV in a HDFS folder and **reference it in the python script** (where the CSV is loaded). 

Then make sure Spark is also running and call spark-submit as below. I am assuming you have your environment all set, with the appropriate path variables and so on.

**USAGE:**  
spark-submit uselection-tweet-stats.py

The code execution will print all the results to the terminal.

Enjoy. And leave your contribution... :)
