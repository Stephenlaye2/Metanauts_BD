from kafka import KafkaConsumer
from pydoop import hdfs
import json

hdfs_path = 'hdfs://localhost:9000/players_data/football_stat.json'

consumer = KafkaConsumer('football-data')

for consumer_data in consumer:
	data = consumer_data.value
	with hdfs.open(hdfs_path, 'w') as hdfs_file:
		hdfs_file.write(data)
