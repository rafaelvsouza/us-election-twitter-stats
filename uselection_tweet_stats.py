import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains
from pyspark.sql.functions import date_trunc, to_timestamp

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

df = spark.read.format("csv") \
      .option("header", True) \
      .option("multiLine", True) \
      .option("escape", "\"") \
      .schema(schema) \
      .load("/user/rafael/twitter/biden")

df.printSchema()

row_count = df.count()
gb_city = df.groupBy(df.city).count().sort('count', ascending=False)
gb_state = df.groupBy(df.state).count().sort('count', ascending=False)
gb_country = df.groupBy(df.country).count().sort('count', ascending=False)
gb_cont = df.groupBy(df.continent).count().sort('count', ascending=False)
max_retweet = df.where(df.retweet_count.isNotNull()).agg({'retweet_count': 'max'})
max_likes = df.where(df.retweet_count.isNotNull()).agg({'likes': 'max'})

max_hour = df.withColumn('created_at', date_trunc('hour', to_timestamp('created_at'))) \
  .groupBy('created_at').count().sort('count', ascending=False)

print('')
print('================================')
print('Tweets Count: {}'.format(row_count))
gb_city.show()
gb_state.show()
gb_country.show()
gb_cont.show()
max_retweet.show()
max_likes.show()
max_hour.show()
print('================================')
print('')