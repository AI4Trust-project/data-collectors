import json
import os
import time
import tempfile
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
        secure=False,
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


def insert_comments(video_response, search_info, file_name, context):
    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if "items" in video_response.keys():
        items = video_response["items"]
        for i in items:
            try:
                base_message = {
                    "table": "youtube-video-comments",
                    "collectionId": search_info["collectionId"],
                    "dataOwner": search_info["dataOwner"],
                    "collectionDate": date,
                    "queryId": search_info["queryId"],
                    "searchKeyword": search_info["searchKeyword"],
                    "resultsPath": file_name,
                    "keywordId": search_info["keywordId"],
                    "producer": search_info["producer"],
                }
                base_message["id"] = i["id"]
                comment = i["snippet"]
                # insert snippet data
                base_message["canReply"] = comment["canReply"]
                base_message["totalReplyCount"] = comment["totalReplyCount"]
                base_message["isPublic"] = comment["isPublic"]
                # insert comment data
                comment = comment["topLevelComment"]
                comment = comment["snippet"]
                for k, v in comment.items():
                    base_message[k] = v

                # send comment
                m = json.loads(json.dumps(base_message))
                context.producer.send("collected_comments", value=m)

                # verify replies
                if "replies" in i:
                    replies = i["replies"]
                    replies = replies["comments"]
                    for r in replies:
                        # create base message
                        base_message = {
                            "table": "youtube-video-comments",
                            "collectionId": search_info["collectionId"],
                            "dataOwner": search_info["dataOwner"],
                            "collectionDate": date,
                            "queryId": search_info["queryId"],
                            "searchKeyword": search_info["searchKeyword"],
                            "resultsPath": file_name,
                            "keywordId": search_info["keywordId"],
                            "producer": search_info["producer"],
                        }
                        # add id
                        base_message["id"] = r["id"]
                        # add snippet data
                        s = r["snippet"]
                        for k, v in s.items():
                            base_message[k] = v
                        # save reply
                        m = json.loads(json.dumps(base_message))
                        context.producer.send("collected_comments", value=m)
            except Exception as e:
                # print("ERROR INSERT COMMENTS")
                # print(e)
                continue


def insert_into_postgres(data, conn):
    cur = None
    try:

        cur = conn.cursor()

        query = (
            "INSERT INTO yt_comments (collection_id, data_owner, collection_date,"
            " query_id, search_keyword, results_path, keyword_id,"
            " producer, yt_part, video_id, text_format, max_results,"
            " yt_order, n_pages)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )
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
                data["part"],
                data["videoId"],
                data["textFormat"],
                data["maxResults"],
                data["order"],
                data["pages"],
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
    print(event)

    data = json.loads(event.body.decode("utf-8"))
    video_id = data["videoId"]
    keyword = data["searchKeyword"]

    bucket_name = "youtube-artifacts"
    dataOwner = "FBK-YOUTUBE"
    query_uuid = str(uuid.uuid4())
    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # test minio
    try:
        if context.client.bucket_exists(bucket_name):
            print("YT COMMENTS: Object storage connected")
    except Exception as e:
        context.producer.send("youtube", value=data)
        print("YT COMMENTS: MINIO NOT REACHABLE")
        wait_until_midnight()

    search_info = {
        "dataOwner": dataOwner,
        "collectionDate": date,
        "queryId": query_uuid,
        "searchKeyword": keyword,
        "resultsPath": generate_folder(data["producer"], video_id, keyword, bucket_name)
        + "/comments",
        "keywordId": data["keywordId"],
        "producer": data["producer"],
        "part": ["id", "snippet", "replies"],
        "videoId": video_id,
        "textFormat": "plainText",
        "maxResults": 100,
        "order": "time",
        "pages": 0,
        "collectionId": data["collectionId"],
    }

    nxPage = "start"
    # iterate in the comment pages

    response = False

    while nxPage != "":
        try:
            if nxPage == "start":
                comment_threads = (
                    context.youtube.commentThreads()
                    .list(
                        part=search_info["part"],
                        videoId=search_info["videoId"],
                        textFormat=search_info["textFormat"],
                        maxResults=search_info["maxResults"],
                        order=search_info["order"],
                    )
                    .execute()
                )
            else:
                comment_threads = (
                    context.youtube.commentThreads()
                    .list(
                        part=search_info["part"],
                        videoId=search_info["videoId"],
                        textFormat=search_info["textFormat"],
                        maxResults=search_info["maxResults"],
                        order=search_info["order"],
                        pageToken=nxPage,
                    )
                    .execute()
                )

            # insert file in S3

            tmp = tempfile.NamedTemporaryFile()

            with open(tmp.name, "w", encoding="utf-8") as f:
                json.dump(comment_threads, f, ensure_ascii=False, indent=4)
            file_name = "page-{:03d}.json".format(search_info["pages"])
            object_name = "{}/{}/{}".format(
                generate_folder(data["producer"], video_id, keyword, bucket_name),
                "comments",
                file_name,
            )
            context.client.fput_object(
                bucket_name, object_name, tmp.name, content_type="application/json"
            )

            tmp.close()

            # insert comments on iceberg
            insert_comments(
                video_response=comment_threads,
                search_info=search_info,
                file_name=object_name,
                context=context,
            )

            if "nextPageToken" in comment_threads.keys():
                nxPage = comment_threads["nextPageToken"]
                search_info["pages"] += 1
            else:
                nxPage = ""
                search_info["pages"] += 1

            response = True

        except Exception as e:
            nxPage = ""
            response = False
            print("Error handler")
            print(e)
            if "quota" in str(e).lower():
                wait_until_midnight()

    # upload meta and insert postgres

    if response:

        # insert data in postgres
        insert_into_postgres(data=search_info, conn=context.conn)

        # insert meta.json in S3
        try:

            tmp = tempfile.NamedTemporaryFile()

            with open(tmp.name, "w", encoding="utf-8") as f:
                json.dump(search_info, f, ensure_ascii=False, indent=4)

            object_name = "{}/{}/{}".format(
                generate_folder(data["producer"], video_id, keyword, bucket_name),
                "comments",
                "meta.json",
            )
            context.client.fput_object(
                bucket_name, object_name, tmp.name, content_type="application/json"
            )
            tmp.close()

        except Exception as e:
            print("ERROR upload meta in s3")
            print(e)

        # insert into iceberg

        search_info["table"] = "youtube-video-comments"
        m = json.loads(json.dumps(search_info))
        # send data to be merged
        context.producer.send("youtuber-merger", value=m)
