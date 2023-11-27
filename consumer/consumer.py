from datetime import datetime

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from producer.schemas import Config


class MsgConsumer:
    def __init__(
        self, topic_name: str, part_num: int, config: Config, funct_receiving_message
    ):
        self.consumer = KafkaConsumer(
            bootstrap_servers=config.bootstrap_servers,
            sasl_mechanism="SCRAM-SHA-256",
            security_protocol="SASL_SSL",
            sasl_plain_username=config.username,
            sasl_plain_password=config.password,
            group_id="$GROUP_NAME",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )

        topic_partition = TopicPartition(
            topic_name, part_num
        )  # указываем имя топика и номер раздела
        self.consumer.assign(
            [topic_partition]
        )  # назначаем потребителя на этот топик и раздел

        # Если хотим считать с определенной позиции
        offset = 290  # устанавливаем позицию
        self.consumer.seek(topic_partition, offset)  # устанавливаем позицию

        # Если хотим считать с определенного момента времени
        # определяем момент времени, с которого начнется чтение
        timestamp = datetime(2023, 7, 1, 0, 50, 0).timestamp() * 1000  # переводим в миллисекунды
        offsets = self.consumer.offsets_for_times({topic_partition: timestamp})  # определяем смещение для каждой
        # партиции в топике

        # Если хотим считать с определенного момента времени или с определенной позиции,
        # устанавливаем позицию для каждой партиции в топике
        if offsets[topic_partition] is not None:
            self.consumer.seek(topic_partition, offsets[topic_partition].offset)

        self.get_message = funct_receiving_message

    def receiving_loop(self):
        pass

    def close_producer(self):
        self.consumer.close()
