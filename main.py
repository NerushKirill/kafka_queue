# -*- encoding: utf-8 -*-
from settings import settings

from producer.schemas import Config
from producer.producer import MsgProducer

from tests.data import generate_message


def main():
    conf = Config(
        bootstrap_servers=[settings.kafka_url, ],
        username=settings.kafka_username,
        password=settings.kafka_password,
    )

    producer_instance = MsgProducer(
        config=conf,
        funct_get_message=generate_message)

    producer_instance.send_loop()


if __name__ == '__main__':
    main()
