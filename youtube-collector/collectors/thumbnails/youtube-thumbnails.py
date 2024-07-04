import hashlib
import json
import os
import tempfile
import urllib.request
import uuid
from datetime import datetime, timezone

import psycopg2
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
    setattr(context, "conn", conn)


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


def insert_into_postgres(data, conn):
    cur = None
    try:
        cur = conn.cursor()

        query = (
            "INSERT INTO yt_thumbnail (data_owner, collection_date,"
            " query_id, search_keyword, results_path, keyword_id, producer, file_hash, video_id)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        # execute the query with parameters
        cur.execute(
            query,
            (
                data["dataOwner"],
                data["collectionDate"],
                data["queryId"],
                data["searchKeyword"],
                data["resultsPath"],
                data["keywordId"],
                data["producer"],
                data["hash"],
                data["videoId"]
            ),
        )

        # commit the changes to the database
        conn.commit()

    except Exception as e:
        print("ERROR INSERTING ytThumb")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def handler(context, event):
    data = json.loads(event.body.decode("utf-8"))
    video_id = data["videoId"]
    keyword = data["searchKeyword"]
    bucket_name = "youtube-artifacts"
    dataOwner = "FBK-YOUTUBE"

    # thumb_types = [
    #     "default",
    #     "mqdefault",
    #     "sddefault",
    #     "hqdefault",
    #     "sddefault",
    #     "maxresdefault",
    # ]
    thumb_types = ["default"]
    tmp = tempfile.NamedTemporaryFile()

    for image_types in thumb_types:
        try:
            image_url = f"https://img.youtube.com/vi/{video_id}/{image_types}.jpg"
            image_name = "{}/{}/{}.jpg".format(
                generate_folder(video_id, keyword, bucket_name),
                "thumbnails",
                image_types,
            )
            image_file = urllib.request.urlopen(image_url)

            with open(tmp.name, "wb") as f:
                f.write(image_file.read())

            h = ""
            with open(tmp.name, "rb") as f:
                # build SHA-1
                h = hashlib.sha1(image_file.read()).hexdigest()

            context.client.fput_object(
                bucket_name,
                image_name,
                tmp.name,
                content_type="image/jpeg",
                metadata={"sha1": str(h)},
            )

            image_file.close()

        except Exception as e:
            print(e)
            continue

    tmp.close()

    # add reference in iceberg with the hash
    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    query_uuid = str(uuid.uuid4())

    data = {
        "dataOwner": dataOwner,
        "videoId": video_id,
        "collectionDate": date,
        "queryId": query_uuid,
        "searchKeyword": keyword,
        "resultsPath": generate_folder(video_id, keyword, bucket_name) + "/thumbnails",
        "keywordId": data["keywordId"],
        "producer": data["producer"],
        "hash": h,
    }

    # inser psql
    insert_into_postgres(data, context.conn)

    # insert in iceberg
    data["table"] = "youtube-video-thumbnails"
    m = json.loads(json.dumps(data))
    context.producer.send("collected_metadata", value=m)
    # send data to be merged
    context.producer.send("youtuber-merger", value=m)
