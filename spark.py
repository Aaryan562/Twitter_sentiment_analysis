from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.ml.feature import Tokenizer, RegexTokenizer
import re
from textblob import TextBlob

def write_row_in_mongo(df, batch_id):
    mongoURL = "mongodb+srv://root:eWZq8x4cKfenxRQl@cluster0.oqftjmc.mongodb.net/Test.twittdata?retryWrites=true&w=majority"
    df.write.format("mongo").mode("append").option("spark.mongodb.output.uri", mongoURL).save()
    pass

# Create a function to get the subjectivity
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


# Create a function to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'


def main():
    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.mongodb.input.uri",
                "mongodb+srv://root:eWZq8x4cKfenxRQl@cluster0.oqftjmc.mongodb.net/Test?retryWrites=true&w=majority") \
        .config("spark.mongodb.output.uri",
                "mongodb+srv://root:eWZq8x4cKfenxRQl@cluster0.oqftjmc.mongodb.net/Test?retryWrites=true&w=majority") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    
    def clean_text(text):
        text = re.sub(r'http\S+', '', text)  # Remove URLs
        text = re.sub(r'@[^\s]+', '', text)  # Remove mentions
        text = re.sub(r'#([^\s]+)', r'\1', text)
        text = re.sub(r'^RT[\s@]+|RT[\s@]+', '', text)
        text = re.sub(r'\n', ' ', text)  # Remove newlines
        return text.strip()
    


    schema = StructType([StructField("value", StringType(), True)])
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter_test") \
        .option("auto.offset.reset", "latest") \
        .load()
    df = df.selectExpr("CAST(value AS STRING)")
    clean_text_udf = F.udf(clean_text, StringType())
    new_df = df.withColumn("processed_text", clean_text_udf(df.value))

    subjectivity = F.udf(getSubjectivity, FloatType())
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())

    subjectivity_tweets = new_df.withColumn('subjectivity', subjectivity(col("processed_text")))
    polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
    sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))
    org_str=df.value
    
    query = sentiment_tweets \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .queryName("test_tweets") \
        .foreachBatch(write_row_in_mongo)\
        .start() 
    query.awaitTermination()
    



if __name__ == '__main__':
    main()

