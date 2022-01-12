import tweepy
import sys
sys.path.append("/home/stephen/Workspace/Metanauts_BD")
import config
import json
from kafka import KafkaProducer
from time import sleep


consumer_key = config.Api_Key
consumer_secret = config.Api_Key_Secret
access_token = config.Acess_Token
access_token_secret = config.Access_Token_Secret

data_arr = []
# Class to get real time data
class StreamData(tweepy.Stream):

    def on_status(self, status):
        self.process_data(status)
        return True

    def process_data(self, status):
        data = status._json

        data_dict = {
            "id":data["id"],
            "sticker": data["entities"]["hashtags"],
            "reply_count": data["reply_count"],
            "lang": data["lang"],
            "followers_count": data["user"]["followers_count"],
            "text": data["text"],
            "timestamp_ms": data["timestamp_ms"],
            "source": data["source"],
            "screen_name": data["user"]["screen_name"],
            "created_at": data["created_at"]

        }
        data_arr.append(data_dict)
        producer = KafkaProducer(bootstrap_servers = ["localhost:9092"], value_serializer = lambda x: json.dumps(x).encode('utf-8'))
        producer.send('pipeline2', value=data_arr)
        sleep(8)
    def on_error(self, status_code):
        if status_code == 420:
            return False
            

# Initialize instance of the subclass
stream_data = StreamData(
  consumer_key, consumer_secret,
  access_token, access_token_secret
)

# Filter realtime Tweets by keyword
stream_data.filter(track=["python,", "java", "scala", "spark"])