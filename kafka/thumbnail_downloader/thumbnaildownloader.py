import json
import os
import urllib.request

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


def handler(context, event):
    
    comment = json.loads(event.body.decode("utf-8"))

    try:
        image_url = comment["comment_thumbnail"]
        image_name = comment["video_id"] + ".jpg"
        image_file = urllib.request.urlopen(image_url)

        context.client.put_object(
            "thumbnails",
            image_name,
            image_file,
            length=int(image_file.headers["Content-Length"]),
            content_type="image/jpeg",
        )

        image_file.close()

        comment["img_location"] = "thumbnails/{}".format(image_name)
        comment["type"] = "minio_stack"
        context.producer.send("uni-topic", value=comment)
    
    except Exception as e:
        print(e)

