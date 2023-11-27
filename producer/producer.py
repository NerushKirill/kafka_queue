import json
import time

from kafka import KafkaProducer

from producer.schemas import Config, Message


class MsgProducer:
    def __init__(self, config: Config, funct_get_message):
        self.producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            sasl_mechanism="SCRAM-SHA-256",
            security_protocol="SASL_SSL",
            sasl_plain_username=config.username,
            sasl_plain_password=config.password,
            value_serializer=lambda v: json.dumps(v).encode(config.encoding),
        )

        self.get_message = funct_get_message

    def send_loop(self):
        try:
            while True:
                message = Message(**self.get_message("applications"))

                future = self.producer.send(
                    topic=message.topic,
                    partition=message.partition,
                    key=str.encode(message.key),
                    value=message.value,
                )

                record_metadata = future.get(timeout=60)

                print(message, record_metadata, sep="\n")

                # time.sleep(10)
        except Exception as e:
            print(f"Error producing message: {e}")
        finally:
            self.close_producer()

    def close_producer(self):
        self.producer.close()
