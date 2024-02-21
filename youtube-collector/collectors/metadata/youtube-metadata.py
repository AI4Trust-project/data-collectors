import json
import os

from minio import Minio
from kafka import KafkaProducer
from googleapiclient.discovery import build


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

    api_key = os.environ.get("YOUTUBE_API_KEY")
    youtube = build("youtube", "v3", developerKey=api_key)

    setattr(context, "producer", producer)
    setattr(context, "client", client)
    setattr(context, "youtube", youtube)


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

    try:
        videos_response = (
            context.youtube.videos()
            .list(
                part=[
                    "contentDetails",
                    "id",
                    "liveStreamingDetails",
                    "localizations",
                    "player",
                    "recordingDetails",
                    "snippet",
                    "statistics",
                    "status",
                    "topicDetails",
                ],
                id=video_id,
            )
            .execute()
        )

        with open("{}.json".format("meta"), "w", encoding="utf-8") as f:
            json.dump(videos_response, f, ensure_ascii=False, indent=4)

        # Upload the JSON file to Minio in a streaming fashion.
        object_name = "{}/{}".format(generate_folder(video_id, keyword), "meta.json")
        context.client.fput_object(
            "videos", object_name, "meta.json", content_type="application/json"
        )

        os.remove("meta.json")
        

    except Exception as e:
        print(e)