import time

from config import settings
from database import get_sql_server_connection
from kafka_producer import create_kafka_producer
from outbox_repository import (
    get_pending_events,
    mark_as_published,
    mark_as_error,
)


def publish_events():
    producer = create_kafka_producer()

    print("Starting inventory outbox publisher...")

    while True:
        conn = get_sql_server_connection()

        try:
            events = get_pending_events(
                conn=conn,
                batch_size=settings.PUBLISHER_BATCH_SIZE,
            )

            if not events:
                print("No pending events found.")
                time.sleep(settings.PUBLISHER_SLEEP_SECONDS)
                continue

            for event in events:
                event_id = event["event_id"]
                movement_id = event["movement_id"]

                try:
                    producer.send(
                        topic=settings.KAFKA_TOPIC,
                        key=movement_id,
                        value=event,
                    )

                    producer.flush()

                    mark_as_published(conn, event_id)

                    print(
                        f"Published event_id={event_id}, "
                        f"movement_id={movement_id}"
                    )

                except Exception as error:
                    mark_as_error(conn, event_id)
                    print(f"Error publishing event_id={event_id}: {error}")

        finally:
            conn.close()

        time.sleep(settings.PUBLISHER_SLEEP_SECONDS)


if __name__ == "__main__":
    publish_events()