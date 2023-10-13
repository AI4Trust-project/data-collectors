import io
import json
import os

import pandas as pd
from minio import Minio
from kafka import KafkaProducer


def init_context(context):
    client = Minio(
        os.environ.get("MINIO_HOME"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        secure=False,
    )

    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)
    setattr(context, "client", client)


def create_or_update_csv_in_minio(context, data):
    file_exists = False
    try:
        obj = context.client.get_object("csvbucket", "test.csv")
        df = pd.read_csv(obj)
        file_exists = True
    except Exception:
        df = pd.DataFrame(data, index=data.keys())

    if file_exists:
        my_new_data = pd.DataFrame(data, index=data.keys())
        df = (
            pd.concat([df, my_new_data], keys=data.keys())
            .drop_duplicates()
            .reset_index(drop=True)
        )

    csv = df.to_csv(index=False).encode("utf-8")

    try:
        context.client.put_object(
            "csvbucket",
            "test.csv",
            data=io.BytesIO(csv),
            length=len(csv),
            content_type="application/csv",
        )
    except Exception as e:
        print(e)


def handler(context, event):
    comment = json.loads(event.body.decode("utf-8"))
    create_or_update_csv_in_minio(context, comment)
