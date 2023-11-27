from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env.development", ".env.secret"),
        env_file_encoding="utf-8",
    )

    kafka_url: str
    kafka_username: str
    kafka_password: str


settings = Settings()
