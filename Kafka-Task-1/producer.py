from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests
import json


url = "https://api-football-beta.p.rapidapi.com/leagues"

querystring = {"code":"gb"}

headers = {
    'x-rapidapi-host': "api-football-beta.p.rapidapi.com",
    'x-rapidapi-key': "f8953a8bf2msha6c08e082f1b3e6p1892cejsn71699d062950"
    }

res = requests.request("GET", url, headers=headers, params=querystring)
res_data = res.json()["response"]

print(res_data)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

for e in res_data:
	data = e
	producer.send('apitest', value = data)
	sleep(5)
