import tweepy
from textblob import TextBlob
# import pandas as pd 
# import numpy as np
from json import dumps
# import re 
# import matplotlib.pyplot as plt
from kafka import KafkaProducer
import logging

access_key = ""
access_secret = ""
consumer_key = ""
consumer_secret = ""


auth = tweepy.OAuthHandler(access_key, access_secret)   
auth.set_access_token(consumer_key, consumer_secret) 

# # # Creating an API object 
api = tweepy.API(auth)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda K:dumps(K).encode('utf-8'))
cursor = tweepy.Cursor(api.search_tweets,q="stocks",tweet_mode='extended').items(2)
tw = {}
for i,tweet in enumerate(cursor):
   tw[i] = tweet
   try:
      producer.send('twitter_test',tweet.full_text,partition=0)
      producer.flush()
   except Exception as e:
      print(e)   
   
# print(tw[0])
