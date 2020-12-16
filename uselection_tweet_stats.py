''' 
US Presidential Election 2020 Twitter Statistics
by Rafael Souza, Maria Barrett, Luigi Ferreira, Carolina Festugatto
https://github.com/rafaelvsouza/us-election-twitter-stats/
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
from pyspark.sql.functions import date_trunc, to_timestamp, date_format

spark = SparkSession.builder.appName('USElectionTweets').getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
      .add("created_at",TimestampType(),True) \
      .add("tweet_id",IntegerType(),True) \
      .add("tweet",StringType(),True) \
      .add("likes",DecimalType(),True) \
      .add("retweet_count",DecimalType(),True) \
      .add("source",StringType(),True) \
      .add("user_id",IntegerType(),True) \
      .add("user_name",StringType(),True) \
      .add("user_screen_name",StringType(),True) \
      .add("user_description",StringType(),True) \
      .add("user_join_date",TimestampType(),True) \
      .add("user_followers_count",DecimalType(),True) \
      .add("user_location",StringType(),True) \
      .add("lat",DoubleType(),True) \
      .add("long",DoubleType(),True) \
      .add("city",StringType(),True) \
      .add("country",StringType(),True) \
      .add("continent",StringType(),True) \
      .add("state",StringType(),True) \
      .add("state_code",StringType(),True) \
      .add("collected_at",TimestampType(),True)

print('=====================================================')
print('US ELECTION TWITTER STATS')
print('=====================================================')
print('Reading CSV file... \n')

df_trump = spark.read.format("csv") \
      .option("header", True) \
      .option("multiLine", True) \
      .option("escape", "\"") \
      .schema(schema) \
      .load("/user/hadoop/tweets/donaldtrump")
#      .load("/user/rafael/twitter/trump")

df_biden = spark.read.format("csv") \
      .option("header", True) \
      .option("multiLine", True) \
      .option("escape", "\"") \
      .schema(schema) \
      .load("/user/hadoop/tweets/joebiden")

df_union = df_trump.union(df_biden)

print("after the union: {}".format(df_union.count()))

df = df_union.distinct()

print("after the distinct: {}".format(df.count()))

print('CSV Schema... \n')

df.printSchema()

#df.show()

print('Calculating... \n')

row_count = df.count()
gb_city = df.filter(df.city.isNotNull()).groupBy(df.city).count().sort('count', ascending=False)
gb_state = df.filter(df.state.isNotNull()).groupBy(df.state).count().sort('count', ascending=False)
gb_country = df.filter(df.country.isNotNull()).groupBy(df.country).count().sort('count', ascending=False)
gb_cont = df.filter(df.continent.isNotNull()).groupBy(df.continent).count().sort('count', ascending=False)

max_retweet = df.where(df.retweet_count.isNotNull()).agg({'retweet_count': 'max'})
max_retweet = max_retweet.withColumnRenamed('max(retweet_count)', 'Tweets')

max_likes = df.where(df.retweet_count.isNotNull()).agg({'likes': 'max'})
max_likes = max_likes.withColumnRenamed('max(likes)', 'Tweets')

max_hour = df.withColumn('created_at', date_trunc('hour', to_timestamp('created_at')))
max_hour = max_hour.groupBy('created_at').count().sort('count', ascending=False)
max_hour = max_hour.withColumn('created_at', date_format('created_at', 'dd/MMM hha'))
max_hour = max_hour.withColumnRenamed("created_at","Date and Time")
max_hour = max_hour.withColumnRenamed("count","Tweets")

most_liked_tweet = df.select('tweet','likes','user_name').orderBy(col('likes').desc()).limit(1)

most_retweeted_tweet = df.select('tweet','retweet_count','user_name').orderBy(col('retweet_count').desc()).limit(1)

print('RESULTS')
print("-"*119)
print('Total tweets count: {}'.format(row_count))
print("-"*119)
print('')
print('')
print("-"*119)
print('Number os tweets posted by CONTINENT:')
print("-"*119)
gb_cont.limit(5).show()
print("-"*119)
print('')
print('')
print("-"*119)
print('Number os tweets posted by COUNTRY:')
print("-"*119)
gb_country.limit(10).show()
print("-"*119)
print('')
print('')
print("-"*119)
print('Number os tweets posted by CITY:')
print("-"*119)
gb_city.limit(10).show()
print("-"*119)
print('')
print('')
print("-"*119)
print('Number os tweets posted by STATE:')
print("-"*119)
gb_state.limit(10).show()
print("-"*119)
print('')
print('')
print("-"*119)
print("The most liked tweet was from \"{}\" with {} likes:".format(most_liked_tweet.collect()[0]['user_name']
                                                              ,most_liked_tweet.collect()[0]['likes']))
print("-"*119)
print("\""+most_liked_tweet.collect()[0]['tweet']+"\"")
print("-"*119)
print('')
print('')
print("-"*119)
print("The most retweeted tweet was from \"{}\" with {} retweets:".format(most_retweeted_tweet.collect()[0]['user_name']
                                                                     ,most_retweeted_tweet.collect()[0]['retweet_count']))
print("-"*119)
print("\""+most_retweeted_tweet.collect()[0]['tweet']+"\"")
print("-"*119)
print('')
print('')
print("-"*119)
print('Most active hours (by number of tweets):')
print("-"*119)
max_hour.limit(10).show()
print("-"*119)
print('')
