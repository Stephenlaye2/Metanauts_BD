from pydoop import hdfs
from kafka import KafkaConsumer

class Consumer:
    def __init__(self, hdfs_path):
        self.hdfs_path = hdfs_path

    def consume(self, topic_name):
        consumer = KafkaConsumer(topic_name)
        for message in consumer:
            with hdfs.open(self.hdfs_path, 'w') as hdfs_file:
                hdfs_file.write(message.value)


# consumer = Consumer('hdfs://localhost:9000/Pipeline/rawgpy_data.json')
# consumer.consume('rawgpy_topic')

consumer = Consumer('hdfs://localhost:9000/Pipeline/tweet_data.json')
consumer.consume('capstone_tweet')
