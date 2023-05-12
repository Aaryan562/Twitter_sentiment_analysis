from kafka import KafkaConsumer
import json

topic_name = 'twitter_test'

consumer = KafkaConsumer(
     topic_name,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     value_deserializer=lambda x: json.loads(x.decode('utf-8')))

try:
    for message in consumer:
        print(message.value)
except json.decoder.JSONDecodeError as e:
    print(f"Error decoding message: {e}")
