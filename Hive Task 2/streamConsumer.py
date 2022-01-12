from kafka import KafkaConsumer
from pydoop import hdfs
hdfs_path = 'hdfs://localhost:9000/players_data/streaming.json'
consumer = KafkaConsumer('pipeline2')

for message in consumer:
    
    with hdfs.open(hdfs_path, 'w') as hdfs_file:
        hdfs_file.write(message.value)

