import json
from kafka import KafkaProducer

def producer():
    return KafkaProducer(bootstrap_servers = ['localhost:9092'],\
        value_serializer = lambda x: json.dumps(x).encode('utf-8') )

def send_data(topic_name, data):
    return producer().send(topic_name, value = data)