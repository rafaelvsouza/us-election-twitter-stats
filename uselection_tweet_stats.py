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

df = spark.read.format("csv") \
      .option("header", True) \
      .option("multiLine", True) \
      .option("escape", "\"") \
      .schema(schema) \
      .load("/user/rafael/twitter/trump")

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

print('RESULTS')
print('----------------------------------')
print('Total tweets count: {}'.format(row_count))
print('')
print('Number os tweets posted by CONTINENT:')
gb_cont.limit(5).show()
print('Number os tweets posted by COUNTRY:')
gb_country.limit(10).show()
print('Number os tweets posted by CITY:')
gb_city.limit(10).show()
print('Number os tweets posted by STATE:')
gb_state.limit(10).show()
print('One tweet has been retweeted {} times.\n'.format(max_retweet.collect()[0]['Tweets']))
print('One tweet received {} likes.\n'.format(max_likes.collect()[0]['Tweets']))
print('Most active hours (by number of tweets):')
max_hour.limit(10).show()
print('================================')
print('')