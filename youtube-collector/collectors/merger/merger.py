import json
import os

import psycopg2
from psycopg2.extras import Json

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


def find_on_queue(conn, video_id):

    cur = None
    row = []

    try:

        cur = conn.cursor()

        query = "SELECT videoId, type, data FROM mergerqueue WHERE videoID = %s"

        cur.execute(query, (video_id,))

        row = cur.fetchone()
    except Exception as e:
        print("ERROR FIND ON QUEUE:")
        print(e)
    finally:
        cur.close()

    return row if row else []


def insert_data(conn, data):
    cur = None
    try:
        cur = conn.cursor()

        query = "INSERT INTO mergerqueue (videoID, type, data) VALUES (%s, %s, %s)"

        # Execute the query with parameters
        cur.execute(query, (data["videoId"], data["type"], Json(data)))

        # Commit the changes to the database
        conn.commit()
    except Exception as e:
        print("ERROR INSERT DATA:")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def delete_data(conn, data):
    cur = None
    try:
        cur = conn.cursor()

        query = "DELETE FROM mergerqueue WHERE videoID = %s"

        # Execute the query with parameters
        cur.execute(query, (data["videoId"],))

        # Commit the changes to the database
        conn.commit()
    except Exception as e:
        print("ERROR DELETE DATA:")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def insert_merger(conn, data):
    cur = None
    try:
        cur = conn.cursor()

        query = "INSERT INTO mergerdata (videoID, collectionDate, searchKeyword, metadataQueryId, commentQueryId) VALUES (%s, %s, %s, %s, %s)"

        # Execute the query with parameters
        cur.execute(
            query,
            (
                data["videoId"],
                data["collectionDate"],
                data["searchKeyword"],
                data["metadataQueryId"],
                data["commentQueryId"],
            ),
        )

        # Commit the changes to the database
        conn.commit()
    except Exception as e:
        print("ERROR INSERT MERGER:")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def handler(context, event):

    data = json.loads(event.body.decode("utf-8"))
    videoId = data["videoId"]

    # verify if is on database

    result = find_on_queue(context.conn, videoId)

    if result:

        if result[1] != data["type"]:
            # create merger

            result_json = result[2]

            base = {
                "videoId": videoId,
                "collectionDate": result_json["collectionDate"],
                "searchKeyword": data["searchKeyword"],
            }

            if data["type"] == "metadata":
                base["metadataQueryId"] = data["queryId"]
                base["commentQueryId"] = result_json["queryId"]
            else:
                base["metadataQueryId"] = result_json["queryId"]
                base["commentQueryId"] = data["queryId"]

            # insert on iceberg

            m = json.loads(json.dumps(base))
            context.producer.send("collected_youtube_data", value=m)

            # insert on psql
            insert_merger(context.conn, base)
            # delete from queue:
            delete_data(context.conn, data=data)

        else:
            delete_data(context.conn, data=data)
            insert_data(context.conn, data)
    else:
        # insert into the database
        insert_data(context.conn, data)
