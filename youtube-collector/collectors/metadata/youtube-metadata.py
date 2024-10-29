import json
import os
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
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

    dbname = os.environ.get("DATABASE_NAME")
    user = os.environ.get("DATABASE_USER")
    password = os.environ.get("DATABASE_PWD")
    host = os.environ.get("DATABASE_HOST")
    port = os.environ.get("DATABASE_PORT")

    conn = psycopg2.connect(
        dbname=dbname, user=user, password=password, host=host, port=port
    )

    setattr(context, "producer", producer)
    setattr(context, "client", client)
    setattr(context, "youtube", youtube)
    setattr(context, "conn", conn)


def generate_folder(search_id, video_id, keyword, bucket_name):
    folder_name = [bucket_name, search_id, keyword]
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


def wait_until_midnight():
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


def insert_into_postgres(data: dict, conn):
    cur = None
    try:

        cur = conn.cursor()

        query = (
            "INSERT INTO yt_metadata (collection_id, data_owner, collection_date,"
            " query_id, video_id, search_keyword, results_path, keyword_id,"
            " producer) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
        cur.execute(
            query,
            (
                data["collectionId"],
                data["dataOwner"],
                data["collectionDate"],
                data["queryId"],
                data["videoId"],
                data["searchKeyword"],
                data["resultsPath"],
                data["keywordId"],
                data["producer"],
            ),
        )

        # commit the changes to the database
        conn.commit()

    except Exception as e:
        print("ERROR INSERTING ytMetadata")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def handler(context, event):

    data = json.loads(event.body.decode("utf-8"))
    video_id = data["videoId"]
    keyword = data["searchKeyword"]
    producer = data["producer"]

    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    query_uuid = str(uuid.uuid4())
    # pass search uuid and use on the tables

    dataOwner = "FBK-YOUTUBE"

    bucket_name = "youtube-artifacts"

    response = False

    # test minio
    try:
        if not context.client.bucket_exists(bucket_name):
            print("YT METADATA: Object storage connected")
    except Exception as e:
        context.producer.send("youtube", value=data)
        print("YT METADATA: MINIO NOT REACHABLE")
        wait_until_midnight()

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

        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, "w", encoding="utf-8") as f:
            videos_response["dataOwner"] = dataOwner
            videos_response["createdAt"] = date
            videos_response["queryId"] = query_uuid
            videos_response["producer"] = producer
            json.dump(videos_response, f, ensure_ascii=False, indent=4)
        object_name = "{}/{}".format(
            generate_folder(data["producer"], video_id, keyword, bucket_name),
            "meta.json",
        )
        context.client.fput_object(
            bucket_name, object_name, tmp.name, content_type="application/json"
        )
        tmp.close()

        response = True

    except Exception as e:
        print(e)
        if "quota" in str(e).lower():
            wait_until_midnight()

    if response:

        # Create Iceberg tables

        # Metada table
        try:
            row = {
                "collectionId": data["collectionId"],
                "dataOwner": dataOwner,
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
                "resultsPath": generate_folder(
                    data["producer"], video_id, keyword, bucket_name
                ),
                "keywordId": data["keywordId"],
                "producer": data["producer"],
            }

            insert_into_postgres(data=row, conn=context.conn)

            row["table"] = "youtube-video-metadata"

            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)
            # send data to be merged
            context.producer.send("youtuber-merger", value=m)

        except Exception as e:
            print("Error Metadata Table: {}".format(e))

        # parse response
        items = videos_response["items"][0]

        # Statistics table

        try:
            stats = items["statistics"]

            row = {
                "table": "youtube-video-statistics",
                "collectionId": data["collectionId"],
                "dataOwner": dataOwner,
                "producer": data["producer"],
                "collectionDate": date,
                "keywordId": data["keywordId"],
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
                "collectionId": data["collectionId"],
                "dataOwner": dataOwner,
                "producer": data["producer"],
                "keywordId": data["keywordId"],
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
                "collectionId": data["collectionId"],
                "dataOwner": dataOwner,
                "producer": data["producer"],
                "keywordId": data["keywordId"],
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
                "collectionId": data["collectionId"],
                "dataOwner": dataOwner,
                "producer": data["producer"],
                "collectionDate": date,
                "keywordId": data["keywordId"],
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
                "collectionId": data["collectionId"],
                "dataOwner": dataOwner,
                "producer": data["producer"],
                "collectionDate": date,
                "keywordId": data["keywordId"],
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
