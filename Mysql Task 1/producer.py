import requests
from time import sleep
import sys
sys.path.append("/home/stephen/Workspace/Metanauts_BD")
import config
import json
from kafka import KafkaProducer, producer

class GetData:
    def __init__(self):
        self.url = "https://covid-193.p.rapidapi.com/statistics"
        self.headers = {'x-rapidapi-host': config.RapidAPI_Host, 'x-rapidapi-key': config.RapidAPI_Key}

    def data(self):
        resp = requests.request("GET", self.url, headers=self.headers)
        return resp.json()["response"]


class Producer:
    def __init__(self, json_data):
        self.json_data = json_data
        self.data = []
        self.bootstrap_servers = ['localhost:9092']
        self.value_serializer = lambda x: json.dumps(x).encode('utf-8')
    def produce(self):
        producer = KafkaProducer(bootstrap_servers = self.bootstrap_servers, value_serializer = self.value_serializer )
        return producer

    def ingest_into_topic(self, topic_name):
        for resp in self.json_data:
            self.data.append(resp)
            self.produce().send(topic_name, value = self.data)
            sleep(6)

getdata = GetData()
producer = Producer(getdata.data())
producer.ingest_into_topic("Mysql1")