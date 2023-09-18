import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import Config

class RedPandaPublisher:
    def __init__(self, config: Config):
        self.topic = config.RedpandaTopic
        self.producer = KafkaProducer(
            bootstrap_servers = config.RedpandaBrokerHost
        )

    def _on_success(self, metadata):
        print(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")

    def _on_error(self, e):
        print(f"Error sending message: {e}")

    def publish(self, msg: dict):
      future = self.producer.send(
        self.topic,
        value=bytes(json.dumps(msg),'UTF-8')
      )
      future.add_callback(self._on_success)
      future.add_errback(self._on_error)
      self.producer.flush()

    def __del__(self):
        self.producer.close()
