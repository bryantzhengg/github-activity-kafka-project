from kafka import KafkaConsumer
import json

from s3fs import S3FileSystem
from time import sleep
from json import dumps,loads

consumer = KafkaConsumer(
    'demo_test',
     bootstrap_servers=[':9092'], #add your IP here
    value_deserializer=lambda x: loads(x.decode('utf-8')))



for count, i in enumerate(consumer):
    with s3.open("s3://kafka-github-activity.json".format(count), 'w') as file:
        json.dump(i.value, file)    

s3 = S3FileSystem()