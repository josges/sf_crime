import json
import time
from kafka import KafkaConsumer


class ConsumerServer(KafkaConsumer):
    def __init__(self, topic, **kwargs):
        self.topic = topic
        super().__init__(**kwargs)

    def binary_to_dict(self, json_string):
        return json.loads(json_string)

    def receive_data(self):
        self.subscribe(topics=[self.topic, ])
        for msg in self:
            print(f"{self.binary_to_dict(msg.value)}")
            time.sleep(1)


if __name__ == "__main__":
    consumer = ConsumerServer(
        topic="org.sf.police.calls",
        bootstrap_servers="localhost:9092",
        client_id="crime_consumer"
    )
    consumer.receive_data()
