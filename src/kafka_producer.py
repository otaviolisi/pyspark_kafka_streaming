import json
from datetime import datetime
from decimal import Decimal

from kafka import KafkaProducer
from config import settings


def json_serializer(value):
    def default_converter(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return str(obj)

    return json.dumps(value, default=default_converter).encode("utf-8")


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
        key_serializer=lambda key: str(key).encode("utf-8"),
        acks="all",
        retries=3,
    )