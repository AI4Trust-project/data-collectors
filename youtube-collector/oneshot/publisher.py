import json
import os

from minio import Minio
from kafka import KafkaProducer

def handler(context, event):
    
    producer = KafkaProducer(
        bootstrap_servers=["my-cluster-kafka-brokers.kafka:9092"],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    client = Minio(
        os.environ.get("MINIO_HOME"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        secure=False,
    )

    bucket_name = "inputdata"
    object_name = "video_ids"

    obj = client.get_object(bucket_name, object_name)
    json_data = obj.read().decode()

    data = json.loads(json_data)

    for d in data:

        producer.send("youtube", value=d)

