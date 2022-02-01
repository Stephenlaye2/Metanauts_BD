# ********** TWITTER STREAMING DATA *************
import tweepy
import sys
sys.path.append('/home/stephen/Workspace/Metanauts_BD')
import config
import capstone_producer as producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master('local[1]').appName('tweetsJob').getOrCreate()


consumer_key = config.Api_Key
consumer_secret = config.Api_Key_Secret
access_token = config.Acess_Token
access_token_secret = config.Access_Token_Secret


# Class to get real time data
class StreamData(tweepy.Stream):
    data_arr=[]
    def on_status(self, status):
        self.process_data(status)
        return True

    def process_data(self, status):
        data = status._json
        data_dict = {
            "id": data["id"],
            "text": data["text"],
            "created_at": data["created_at"],
            "screen_name": data["user"]["screen_name"],
            "followers_count": data["user"]["followers_count"],
            "favourite_count": data["favorite_count"],
            "retweet_count": data["retweet_count"],
        }

        self.data_arr.append(data_dict)
        # producer = KafkaProducer(bootstrap_servers = ["localhost:9092"], value_serializer = lambda x: json.dumps(x).encode('utf-8'))
        producer.send_data('capstone_tweet', self.data_arr)
    def on_error(self, status_code):
        if status_code == 420:
            return False
            


    
# Initialize instance of the subclass
stream_data = StreamData(
  consumer_key, consumer_secret,
  access_token, access_token_secret
)

""" ***** GET STREAMING TWEET Of THE TOP 3 GAMES ****** """
df = spark.read.format("json").load("hdfs://localhost:9000/Pipeline/rawgpy_data.json")
# df.select("id", "name", "reviews_count", "rating", "ratings_count", "reviews_count", "platforms", "stores").show()

# Filter out top 3 rated platform
top3_df = df.filter((col("rating") >= 4) & (col("ratings_count") >= 4000)).select("name").rdd
def get_games(index):
    top3_df.flatMap(lambda x: x).take(3)[index]

stream_data.filter(track=[f"{get_games(0)}",\
     f"{get_games(1)}", f"{get_games(2)}"])

# Filter realtime Tweets by keyword
# stream_data.filter(track=["python,", "java", "scala", "spark"])