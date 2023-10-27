import json
import os

from minio import Minio
from requests import HTTPError
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


def generate_folder(video_id):
    folder_name = []
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
    print(event)
    video_id = event.body.decode("utf-8")

    search_info = {
        "part": ["id", "snippet", "replies"],
        "videoId": video_id,
        "textFormat": "plainText",
        "maxResults": 100,
        "order": "time",
        "pages": 0,
    }

    nxPage = "start"

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

            object_name = "{}/{}/{}".format(generate_folder(video_id), "comments", file_name)
            context.client.fput_object(
                "videos", object_name, file_name, content_type="application/json"
            )

            os.remove(file_name)

            if "nextPageToken" in comment_threads.keys():
                nxPage = comment_threads["nextPageToken"]
                search_info["pages"] += 1
            else:
                nxPage = ""

        except Exception as e:
            nxPage = ""
            print(e)

    # upload meta
    
    try:
        meta_file = "meta.json"
        search_info["pages"] += 1
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(search_info, f, ensure_ascii=False, indent=4)

        object_name = "{}/{}/{}".format(generate_folder(video_id), "comments", meta_file)
        context.client.fput_object(
            "videos", object_name, meta_file, content_type="application/json"
        )
        os.remove(meta_file)

    except Exception as e:
        print(e)
