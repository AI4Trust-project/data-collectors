import json
import os
import tempfile
import uuid
from datetime import datetime, timezone

import psycopg2
from minio import Minio
from pytube import YouTube

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

def insert_into_psql(data, conn):
    cur = None
    try:
        cur = conn.cursor()

        query = (
            "INSERT INTO ytVideoFile (dataOwner, collectionDate,"
            " queryId, searchKeyword, resultsPath, keywordId, producer)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
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
                data["producer"]
            ),
        )

        # commit the changes to the database
        conn.commit()

    except Exception as e:
        print("ERROR INSERTING ytSearch")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def handler(context, event):

    try:
        data = json.loads(event.body.decode("utf-8"))
        bucket_name = "youtube-artifacts"
        video_id = data["videoId"]
        keyword = data["searchKeyword"]
        dataOwner = "FBK-YOUTUBE"

        yt = YouTube("https://youtu.be/{}".format(video_id))

        tmp = tempfile.NamedTemporaryFile()

        yt.streams.filter(progressive=True, file_extension="mp4").order_by(
            "resolution"
        ).asc().first().download(filename=tmp.name)

        # upload
        file_name = "{}.mp4".format(video_id)

        object_name = "{}/{}".format(
            generate_folder(video_id, keyword, bucket_name), file_name
        )

        context.client.fput_object(
            bucket_name, object_name, tmp.name, content_type="application/mp4"
        )

        tmp.close()

        # # download captions
        # # slipt the captions
        # for caption in yt.captions:
        #     file_name = "{}.xml".format(caption)

        #     object_name = "{}/{}/{}".format(
        #         generate_folder(video_id, keyword, bucket_name), "captions", file_name
        #     )

        #     with open(file_name, "w") as f:
        #         f.write(str(caption.xml_captions))
        #     context.client.fput_object(
        #         bucket_name, object_name, file_name, content_type="application/xml"
        #     )
        #     os.remove(file_name)

        # insert data in postgres and iceberg

        date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        query_uuid = str(uuid.uuid4())

        data = {
            "dataOwner": dataOwner,
            "collectionDate": date,
            "queryId": query_uuid,
            "videoId": video_id,
            "searchKeyword": keyword,
            "resultsPath": object_name,
            "keywordId": data["keywordId"],
            "producer": data["producer"],
        }

        insert_into_psql(data, context.conn)

        # insert in iceberg
        data["table"] = "youtube-video-videofile"
        m = json.loads(json.dumps(data))
        context.producer.send("collected_metadata", value=m)

    except Exception as e:
        print("YT DOWNLOAD ERROR")
        print(e)