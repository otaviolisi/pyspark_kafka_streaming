import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    SQL_SERVER_DRIVER = os.getenv("SQL_SERVER_DRIVER")
    SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST")
    SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT")
    SQL_SERVER_DATABASE = os.getenv("SQL_SERVER_DATABASE")
    SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
    SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD")
    SQL_SERVER_TRUST_CERTIFICATE = os.getenv("SQL_SERVER_TRUST_CERTIFICATE", "yes")

    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

    PUBLISHER_BATCH_SIZE = int(os.getenv("PUBLISHER_BATCH_SIZE", 10))
    PUBLISHER_SLEEP_SECONDS = int(os.getenv("PUBLISHER_SLEEP_SECONDS", 5))


settings = Settings()