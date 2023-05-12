The data being used is the Twitter data that is obtained through the official Twitter API. The data
obtained through the Twitter API is a collection of tweets, each of which contains a wealth of
information about the tweet's author, content, location, and engagement. The fields present in
this data can vary depending on the API endpoint used and the level of access granted.
Some of the common fields that may be present in the Twitter data obtained through the API include:
* Tweet ID: A unique identifier for the tweet.
* User ID: A unique identifier for the user who posted the tweet.
* Username: The username of the user who posted the tweet.
* Full name: The full name of the user who posted the tweet.
* Tweet text: The text content of the tweet.


In this case the fields that we are using are just the tweet text, which is being further used for
sentiment analysis. Firstly, a query is decided upon and the tweets related to those tweets are
extracted from the Twitter API. These tweets are the data that is used throughout the project.
The design of the data pipeline is quite simple. It consists of three main components – Fetching
Twitter data API, streaming the data through Kafka and receiving it through Apache Spark, here
we are using Pyspark.
The data pipeline for Twitter API, Kafka streaming, and Pyspark typically involves the following
steps:
1. Data Extraction: The Twitter API is used to extract tweets in real-time. The API can be
configured to extract tweets based on a set of keywords, hashtags, or location
2. Data Ingestion: The extracted tweets are then sent to Apache Kafka, a distributed
streaming platform that can handle high-volume data streams. Kafka ensures reliable and
scalable data ingestion, making it ideal for streaming data applications.
3. Data Processing: Once the tweets are ingested into Kafka, they are consumed by a
Pyspark application running on a distributed cluster. Pyspark provides a set of powerful
tools and libraries for big data processing, including real-time streaming processing with
Spark Streaming.
4. Sentiment Analysis: Using Pyspark, sentiment analysis is performed on the tweets to
determine the overall sentiment (positive, negative, or neutral) expressed in each tweet.
The sentiment analysis can be performed using a variety of natural language processing
techniques, such as text classification or sentiment lexicons.
5. Data Storage: Finally, the results of the sentiment analysis are stored in a database or a
file system. This data can be used for further analysis or to build visualizations that help
users better understand the sentiment of Twitter users on a particular topic.
Overall, this data pipeline enables the processing of real-time data streams from Twitter and the
extraction of valuable insights through sentiment analysis. It is a powerful tool for businesses and
organizations that want to monitor their brand reputation, track emerging trends, or gain insight
into public opinion on various topics.

In conclusion, this project demonstrates the potential of using big data technologies to perform
sentiment analysis on Twitter data in real-time. By utilizing the Tweepy library to connect to the
Twitter API, Kafka streaming to process incoming data in real-time, and Pyspark for preprocessing and analysis, 
the project showcases a powerful and scalable data pipeline.
The sentiment analysis of the tweets was performed using the Textblob library, which provides a
simple and easy-to-use interface for natural language processing tasks. The results obtained from
the sentiment analysis provide insights into the emotional content of the tweets and can be used
for various applications such as brand monitoring, political analysis, and public opinion research.
The future scope of this project includes improving the accuracy of the sentiment analysis model
by incorporating machine learning algorithms and training the model on a larger dataset.
Additionally, the project can be extended to perform more advanced natural language processing
tasks such as entity recognition, topic modelling, and summarization.
Overall, this project showcases the power and potential of big data technologies for real-time
sentiment analysis of Twitter data and provides a foundation for future research in this field.
