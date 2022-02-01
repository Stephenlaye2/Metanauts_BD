import requests
import datetime
import tweepy
from time import sleep
import sys
sys.path.append('/home/stephen/Workspace/Metanauts_BD')
import config
import capstone_producer as producer

consumer_key = config.Api_Key
consumer_secret = config.Api_Key_Secret
access_token = config.Acess_Token
access_token_secret = config.Access_Token_Secret

rawgpy_key = config.Rawgpy_Key

today = datetime.datetime.now()
prev_30_days = datetime.timedelta(days=30)
mth_before = today - prev_30_days
current_mth = today.date()
last_mth = mth_before.date()

rawgpy_url = f"https://api.rawg.io/api/games?key={rawgpy_key}&\
    dates={last_mth},{current_mth}&platforms=1,4,187"


""" ******** RAWGPY API DATA ********* """

class RawpyAPI:
    def __init__(self) -> None:
        pass
    
    def fetch_data(self, url):
        response = requests.get(url)
        return response.json()["results"]


# ************* PRODUCER *****************

class IngestRawgpy:
    def __init__(self) -> None:
        self.data = []
   
    def ingest_into_topic(self, topic_name, json_data):
        id = 1
        for data in json_data:
            new_data = {
                "id": id,
                "name": data["name"], 
                "platforms": data["platforms"],
                "stores": data["stores"],
                "released": data["released"], 
                "rating": data["rating"],
                "reviews_count": data["reviews_count"],
                "ratings_count": data["ratings_count"],
                "ratings": data["ratings"], 
                "updated": data["updated"]
                }
            self.data.append(new_data)
            producer.send_data(topic_name, self.data)
            sleep(2)
            id += 1
            
            
# Fetch Rawgpy Api Data
rawgpyApi = RawpyAPI()
rawgpy_data = rawgpyApi.fetch_data(rawgpy_url)

# print(rawgpy_data)

# Send Rawgpy Api Data Into topic
ingest_rawgpy = IngestRawgpy()
ingest_rawgpy.ingest_into_topic('rawgpy_topic', rawgpy_data)


