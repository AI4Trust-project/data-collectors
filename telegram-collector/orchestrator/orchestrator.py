import json
import os

import psycopg
from kafka import KafkaProducer


def init_context(context):
    connection = psycopg.connect(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PW"],
        host=os.environ["POSTGRES_IP"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
    )
    setattr(context, "connection", connection)

    broker = os.environ.get("KAFKA_BROKER")
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def handler(context, event):
    producer = context.producer
    connection = context.connection

    with connection.cursor() as cur:
        # TODO: also WHERE last_queried_at is inferior to some value
        # in prio, take into account number of already high-priority of same language?
        cur.execute(
            "SELECT id, access_hash, distance_from_core"
            " FROM channels_to_query"
            " ORDER BY collection_priority DESC"
            " LIMIT 1"
        )
        chan_to_query = cur.fetchone()

    # TODO: delete entry? or rather put flag / last queried at?
    if chan_to_query is not None:
        channel_id, access_hash, distance_from_core = chan_to_query
        producer.send(
            "chans_to_message",
            value={
                "channel_id": channel_id,
                "access_hash": access_hash,
                "distance_from_core": distance_from_core,
            },
        )

        return {
                "channel_id": channel_id,
                "access_hash": access_hash,
                "distance_from_core": distance_from_core,
            }
    # TODO: else?
