import json
import os
import tempfile
import uuid
from datetime import datetime, timezone

import psycopg2
from minio import Minio
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter

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


def insert_into_psql(data, conn):
    cur = None
    try:
        cur = conn.cursor()

        query = (
            "INSERT INTO yt_video_transcription (collection_id, data_owner, collection_date,"
            " query_id, search_keyword, results_path, keyword_id, producer, video_id, num_transcriptions)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        # execute the query with parameters
        cur.execute(
            query,
            (
                data["collectionId"],
                data["dataOwner"],
                data["collectionDate"],
                data["queryId"],
                data["searchKeyword"],
                data["resultsPath"],
                data["keywordId"],
                data["producer"],
                data["videoId"],
                data["numTranscriptions"],
            ),
        )

        # commit the changes to the database
        conn.commit()

    except Exception as e:
        print("ERROR INSERTING yt_video_transcription ")
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

        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        formatter = JSONFormatter()
        transcript_files = []
        for transcript in transcript_list:

            language_name = str(transcript.language).replace(" ", "-").lower()

            base_name = f"{transcript.language_code}_{language_name}"

            if transcript.is_generated:
                base_name += f"_generated"

            tmp = tempfile.NamedTemporaryFile()

            t = transcript.fetch()
            json_t = formatter.format_transcript(t)

            with open(tmp.name, "w") as f:
                json.dump(json_t, f, ensure_ascii=False)

            # upload
            file_name = f"{base_name}.json"

            object_name = "{}/transcriptions/{}".format(
                generate_folder(data["producer"], video_id, keyword, bucket_name),
                file_name,
            )

            transcript_files.append(object_name)

            context.client.fput_object(
                bucket_name,
                object_name,
                tmp.name,
                content_type="application/json",
            )

            tmp.close()

        # insert data in postgres and iceberg

        date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        query_uuid = str(uuid.uuid4())

        # insert every transcript in postgres and iceberg
        for file_name in transcript_files:
            search_info = {
                "collectionId": data["collectionId"],
                "dataOwner": dataOwner,
                "collectionDate": date,
                "queryId": query_uuid,
                "videoId": video_id,
                "searchKeyword": keyword,
                "resultsPath": file_name,
                "keywordId": data["keywordId"],
                "producer": data["producer"],
                "numTranscriptions": len(transcript_files),
            }

            insert_into_psql(search_info, context.conn)
            search_info["table"] = "youtube-video-transcript"
            m = json.loads(json.dumps(search_info))
            context.producer.send("collected_metadata", value=m)

        # data for merger

        search_info = {
            "collectionId": data["collectionId"],
            "dataOwner": dataOwner,
            "collectionDate": date,
            "queryId": query_uuid,
            "videoId": video_id,
            "searchKeyword": keyword,
            "resultsPath": "{}/transcriptions".format(
                generate_folder(data["producer"], video_id, keyword, bucket_name)
            ),
            "keywordId": data["keywordId"],
            "producer": data["producer"],
        }

        # send data to be merged
        search_info["table"] = "youtube-video-transcript"
        m = json.loads(json.dumps(search_info))
        context.producer.send("youtuber-merger", value=m)

    except Exception as e:
        print("YT TRANSCRIPT ERROR")
        print(e)
