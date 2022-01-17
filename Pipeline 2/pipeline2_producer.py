import requests
import json
from time import sleep
from kafka import KafkaProducer
import sys
sys.path.append("/home/stephen/Workspace/Metanauts_BD")
import config
# Class for getting the API data
class GetData:
    def __init__(self, url):
        self.url = url
        self.headers = {
    'x-rapidapi-host': config.RapidAPI_Host,
    'x-rapidapi-key': config.RapidAPI_Key
    }

    def the_data(self):
        resp = requests.request('GET', self.url, headers = self.headers)
        return resp.json()["response"]


class Producer:
    def __init__(self, json_data):
        self.data = []
        self.json_data = json_data
        self.bootstrap_servers = ['localhost:9092']
        self.value_serializer = lambda x: json.dumps(x).encode('utf-8')
    
    def produce(self):
        producer = KafkaProducer(bootstrap_servers = self.bootstrap_servers, value_serializer = self.value_serializer)
        return producer
    
    def injest_into_topic(self, topic_name):
        for data in self.json_data:
            self.data.append(data)
            self.produce().send(topic_name, value = self.data)
            sleep(6)


url = "https://covid-193.p.rapidapi.com/statistics"

# Instanstiate the class
getdata = GetData(url)
producer = Producer(getdata.the_data())
producer.injest_into_topic('Pipeline2')
# print(getdata.the_data())

