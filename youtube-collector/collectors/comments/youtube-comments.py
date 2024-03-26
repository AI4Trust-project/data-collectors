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


def insert_comments(video_response, query_uuid, keyword):
    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    data = []
    if "items" in video_response.keys():
        items = video_response["items"]
        for i in items:
            try:
                base_message = {
                    "table": "youtube-video-comment",
                    "collectionDate": date,
                    "queryId": query_uuid,
                    "searchKeyword": keyword,
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

                # save comment
                data.append(base_message)

                # verify replies
                if "replies" in i:
                    replies = i["replies"]
                    replies = replies["comments"]
                    for r in replies:
                        # create base message
                        base_message = {
                            "table": "youtube-video-comment",
                            "collectionDate": date,
                            "queryId": query_uuid,
                            "searchKeyword": keyword,
                        }
                        # add id
                        base_message["id"] = r["id"]
                        # add snippet data
                        s = r["snippet"]
                        for k, v in s.items():
                            base_message[k] = v
                        # save reply
                        data.append(base_message)
            except Exception as e:
                print(e)
                continue

    return data


def insert_query_table(date, query_uuid, video_id, keyword, search_info):

    row = {
        "table": "youtube-query",
        "collectionDate": date,
        "queryId": query_uuid,
        "videoId": video_id,
        "searchKeyword": keyword,
        "resource": "commentThreads.list",
        "part": search_info["part"],
        "textFormat": search_info["textFormat"],
        "maxResults": search_info["maxResults"],
        "order": search_info["order"],
    }

    m = json.loads(json.dumps(row))

    return m


def insert_files_table(date, query_uuid, video_id, keyword, location):
    m = {
        "table": "youtube-video-comments-files",
        "collectionDate": date,
        "queryId": query_uuid,
        "videoId": video_id,
        "searchKeyword": keyword,
        "s3Location": location,
    }

    return m


def handler(context, event):
    print(event)

    data = json.loads(event.body.decode("utf-8"))
    video_id = data["video_id"]
    keyword = data["keyword"]

    bucket_name = "youtube-artifacts"

    search_info = {
        "part": ["id", "snippet", "replies"],
        "videoId": video_id,
        "textFormat": "plainText",
        "maxResults": 100,
        "order": "time",
        "pages": 0,
    }

    nxPage = "start"
    query_uuid = str(uuid.uuid4())
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

            file_name = "page-{:03d}.json".format(search_info["pages"])

            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(comment_threads, f, ensure_ascii=False, indent=4)

            object_name = "{}/{}/{}".format(
                generate_folder(video_id, keyword, bucket_name), "comments", file_name
            )
            context.client.fput_object(
                bucket_name, object_name, file_name, content_type="application/json"
            )

            # insert query table
            date = (
                datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            )

            if nxPage == "start":

                q = insert_query_table(date, query_uuid, video_id, keyword, search_info)
                m = json.loads(json.dumps(q))
                context.producer.send("collected_comments", value=m)

            # insert comments in the database
            comments = insert_comments(
                video_response=comment_threads, query_uuid=query_uuid, keyword=keyword
            )
            for c in comments:
                m = json.loads(json.dumps(c))
                context.producer.send("collected_comments", value=m)

            # insert file table
            q = insert_files_table(date, query_uuid, video_id, keyword, object_name)
            m = json.loads(json.dumps(q))
            context.producer.send("collected_comments", value=m)

            os.remove(file_name)

            if "nextPageToken" in comment_threads.keys():
                nxPage = comment_threads["nextPageToken"]
                search_info["pages"] += 1
            else:
                nxPage = ""

            response = True

        except Exception as e:
            nxPage = ""
            response = False
            print(e)
            if "quota" in str(e).lower():
                wait_unti_midnight()

    # upload meta

    if response:

        try:

            meta_file = "meta.json"
            search_info["pages"] += 1
            with open(meta_file, "w", encoding="utf-8") as f:
                json.dump(search_info, f, ensure_ascii=False, indent=4)

            object_name = "{}/{}/{}".format(
                generate_folder(video_id, keyword, bucket_name), "comments", meta_file
            )
            context.client.fput_object(
                bucket_name, object_name, meta_file, content_type="application/json"
            )
            os.remove(meta_file)

        except Exception as e:
            print(e)
