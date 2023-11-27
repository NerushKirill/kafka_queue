import random
from datetime import datetime

from faker import Faker


def generate_message(topic: str):
    fake = Faker()

    now = datetime.now()

    return {
        "topic": topic,
        "partition": random.choice([1, 0]),
        "key": f'type_{random.choice(["question", "request"])}',
        "value": {
            "id": now.strftime("%m/%d/%Y %H:%M:%S"),
            "name": fake.name(),
            "address": fake.address(),
            "text": fake.text(),
        },
    }


def introduce_message(topic: str, part: int):
    pass
