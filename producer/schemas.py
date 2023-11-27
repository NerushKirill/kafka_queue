from pydantic import BaseModel


class Config(BaseModel):
    bootstrap_servers: list[str]
    username: str
    password: str
    encoding: str = "utf-8"


class Message(BaseModel):
    topic: str
    partition: int
    key: str
    value: dict
