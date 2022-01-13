from kafka import KafkaConsumer
from pydoop import hdfs

class Consumer:
    def __init__(self, topic_name, hdfs_path):
        self.topic_name = topic_name
        self.hdfs_path = hdfs_path
        self.consumer = KafkaConsumer(self.topic_name)

    def consume_data(self):
        for message in self.consumer:
            with hdfs.open(self.hdfs_path, 'w') as hdfs_file:
                hdfs_file.write(message.value)
    
consumer = Consumer('Mysql1', 'hdfs://localhost:9000/data-space/covid_data.json')
consumer.consume_data()

