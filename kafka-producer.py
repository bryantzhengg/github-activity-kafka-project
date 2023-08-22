from kafka import KafkaConsumer
import json

from s3fs import S3FileSystem
from time import sleep
from json import dumps,loads

producer = KafkaProducer(bootstrap_servers=[':9092'], #change ip here
    value_serializer=lambda x: 
    dumps(x).encode('utf-8'))

producer.send('demo_test', value={'surnasdasdame':'parasdasdmar'})

df = pd.read_csv("data/indexProcessed.csv")
df.head()

while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('demo_test', value=dict_stock)
    sleep(1)

producer.flush() #clear data from kafka server