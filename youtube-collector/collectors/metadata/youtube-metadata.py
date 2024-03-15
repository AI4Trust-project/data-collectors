import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

from googleapiclient.discovery import build
from minio import Minio

from kafka import KafkaProducer


def init_context(context):
    client = Minio(
        os.environ.get("MINIO_HOME"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
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


def generate_folder(video_id, keyword, bucket_name):
    folder_name = [bucket_name, keyword]
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


def wait_unti_midnight():
    # Get the current time in PDT
    now = datetime.now(timezone(timedelta(hours=-7)))

    # Calculate the time until the next midnight PDT
    midnight_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_tomorrow = midnight_today + timedelta(days=1)

    # Check if midnight has already passed today
    if now > midnight_today:
        # Wait until tomorrow midnight
        wait_time = (midnight_tomorrow - now).total_seconds()
    else:
        # Wait until tonight's midnight
        wait_time = (midnight_today - now).total_seconds()

    print("WAITING UNTIL MIDNIGHT PDT")

    # Wait for the calculated time
    time.sleep(wait_time)


def handler(context, event):

    data = json.loads(event.body.decode("utf-8"))
    video_id = data["video_id"]
    keyword = data["keyword"]

    bucket_name = "youtube-artifacts"

    response = False

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
        object_name = "{}/{}".format(
            generate_folder(video_id, keyword, bucket_name), "meta.json"
        )
        context.client.fput_object(
            bucket_name, object_name, "meta.json", content_type="application/json"
        )

        os.remove("meta.json")

        response = True

    except Exception as e:
        print(e)
        if "quota" in str(e).lower():
            wait_unti_midnight()

    if response:

        # Create Iceberg tables

        # Query table

        date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        query_uuid = str(uuid.uuid4())

        try:
            row = {
                "table": "youtube-query",
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
                "resource": "videos.list",
                "part": [
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
                "id": video_id,
            }

            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)
        except Exception as e:
            print("Error Query Table: {}".format(e))

        # Metada table

        try:
            row = {
                "table": "youtube-video-metadata",
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
                "s3Location": generate_folder(video_id, keyword, bucket_name),
            }

            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)

        except Exception as e:
            print("Error Metadata Table: {}".format(e))

        # parse response

        items = videos_response["items"][0]

        # Statistics table

        try:
            stats = items["statistics"]

            row = {
                "table": "youtube-video-statistics",
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
            }

            for k, v in stats.items():
                row[k] = v

            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)
        except Exception as e:
            print("Error Statistics Table: {}".format(e))

        # snippet table

        try:

            snippet = items["snippet"]

            row = {
                "table": "youtube-video-snippet",
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
            }

            for k, v in snippet.items():
                row[k] = v

            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)

        except Exception as e:
            print("Error Snippet Table: {}".format(e))

        # topicDetails table

        try:

            topicDetails = items["topicDetails"]

            row = {
                "table": "youtube-video-topicDetails",
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
            }

            for k, v in topicDetails.items():
                row[k] = v

            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)

        except Exception as e:
            print("Error TopicDetails Table: {}".format(e))

        # contentDetails table

        try:
            contentDetails = items["contentDetails"]

            row = {
                "table": "youtube-video-contentDetails",
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
            }

            for k, v in contentDetails.items():
                row[k] = v

            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)
        except Exception as e:
            print("Error ContentDetails Table: {}".format(e))

        # status table

        try:
            status = items["status"]

            row = {
                "table": "youtube-video-status",
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
            }

            for k, v in status.items():
                row[k] = v

            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)
        except Exception as e:
            print("Error Status Table: {}".format(e))
