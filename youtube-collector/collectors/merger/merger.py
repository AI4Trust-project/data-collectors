import json
import os
from datetime import datetime, timezone

import psycopg2

from kafka import KafkaProducer


def init_context(context):
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
    setattr(context, "conn", conn)


def find_on_database(conn, data):

    cur = None
    row = []

    try:

        cur = conn.cursor()

        query = (
            "SELECT * FROM youtube_video"
            " WHERE video_id = %s AND producer = %s AND keyword_id = %s"
        )

        cur.execute(query, (data["videoId"], data["producer"], data["keywordId"]))

        row = cur.fetchone()
    except Exception as e:
        print("ERROR FIND ON DB:")
        print(e)
    finally:
        cur.close()

    return row if row else []


def insert_data(conn, data):
    cur = None
    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        cur = conn.cursor()

        query = (
            "INSERT INTO youtube_video"
            " (data_owner, created_at, last_update, producer, video_id, keyword_id, keyword,"
            " relevance_language, region_code)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        # Execute the query with parameters
        cur.execute(
            query,
            (
                data["dataOwner"],
                date,
                date,
                data["producer"],
                data["videoId"],
                data["keywordId"],
                data["searchKeyword"],
                data["relevanceLanguage"],
                data["regionCode"],
            ),
        )

        # Commit the changes to the database
        conn.commit()
    except Exception as e:
        print("ERROR INSERT DATA:")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def update_data(data, conn):

    if "table" not in data:
        return -1

    query = ""
    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if data["table"] == "youtube-video-comments":
        query = (
            "UPDATE youtube_video"
            " SET last_update = %s, comments_id = %s, comments_path = %s"
            " WHERE video_id = %s AND producer = %s AND keyword_id = %s"
        )

    elif data["table"] == "youtube-video-metadata":
        query = (
            "UPDATE youtube_video"
            " SET last_update = %s, metadata_id = %s, metadata_path = %s"
            " WHERE video_id = %s AND producer = %s AND keyword_id = %s"
        )

    elif data["table"] == "youtube-video-thumbnails":
        query = (
            "UPDATE youtube_video"
            " SET last_update = %s, thumbnails_id = %s, thumbnails_path = %s"
            " WHERE video_id = %s AND producer = %s AND keyword_id = %s"
        )

    elif data["table"] == "youtube-video-videofile":
        query = (
            "UPDATE youtube_video"
            " SET last_update = %s, videofile_id = %s, videofile_path = %s"
            " WHERE video_id = %s AND producer = %s AND keyword_id = %s"
        )

    elif data["table"] == "youtube-video-transcript":
        query = (
            "UPDATE youtube_video"
            " SET last_update = %s, transcript_id = %s, transcript_path = %s"
            " WHERE video_id = %s AND producer = %s AND keyword_id = %s"
        )

    cur = None
    try:
        cur = conn.cursor()
        cur.execute(
            query,
            (
                date,
                data["queryId"],
                data["resultsPath"],
                data["videoId"],
                data["producer"],
                data["keywordId"],
            ),
        )
        conn.commit()
    except Exception as e:
        print("ERROR updating yt_video")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()

def update_subs_metric(data, conn):
    cur = None
    try:
        cur = conn.cursor()
        date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        query = (
            "UPDATE youtube_video"
            " SET last_update = %s, normalised_subscribers = %s"
            " WHERE video_id = %s AND producer = %s AND keyword_id = %s"
        )
        cur.execute(
            query,
            (
                date,
                data["normalisedSubscribers"],
                data["videoId"],
                data["producer"],
                data["keywordId"],
            ),
        )
        conn.commit()
    except Exception as e:
        print("ERROR updating yt_video metric")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def handler(context, event):

    data = json.loads(event.body.decode("utf-8"))

    if data["table"] == "youtube-collection":
        insert_data(data=data, conn=context.conn)
    elif data["table"] == "youtube-video-normalised-subscribers":
        result = find_on_database(context.conn, data)
        if result:
            update_subs_metric(data=data, conn=context.conn)
    else:
        # verify if is on database
        result = find_on_database(context.conn, data)
        if result:
            update_data(data, context.conn)
