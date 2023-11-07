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


def generate_folder(video_id, keyword):
    folder_name = ["videos", keyword]
    # count 2 caracters to create subfolder
    for i in range(0, len(video_id), 2):
        # Check if the remaining characters are less than 2
        if i + 1 == len(video_id):
            # Read only the last character
            char = video_id[i]
        else:
            # Read 2 characters at a time
            char = video_id[i : i + 2]

        folder_name.append(char)

    return "/".join(folder_name)


def handler(context, event):
    data = json.loads(event.body.decode("utf-8"))
    video_id = data["video_id"]
    keyword = data["keyword"]
    bucket = "videos"

    thumb_types = [
        "default",
        "mqdefault",
        "sddefault",
        "hqdefault",
        "sddefault",
        "maxresdefault",
    ]

    for image_types in thumb_types:
        try:
            image_url = f"https://img.youtube.com/vi/{video_id}/{image_types}.jpg"
            image_name = "{}/{}/{}.jpg".format(
                generate_folder(video_id, keyword), "thumbnails", image_types
            )
            image_file = urllib.request.urlopen(image_url)

            context.client.put_object(
                bucket,
                image_name,
                image_file,
                length=int(image_file.headers["Content-Length"]),
                content_type="image/jpeg",
            )

            image_file.close()
        except Exception as e:
            print(e)
            continue
