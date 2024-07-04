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


def generate_folder(bucket_name, keyword):
    folder_name = [bucket_name, "search", keyword]

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


def search_keyword(keyword: str, search_info: dict, context):
    responses = []
    try:
        nxPage = "start"

        # search keyword
        while nxPage != "":
            videos_response = {}

            if nxPage == "start":
                videos_response = (
                    context.youtube.search()
                    .list(
                        part=search_info["part"],
                        q=search_info["q"],
                        maxResults=search_info["maxResults"],
                        order=search_info["order"],
                        safeSearch=search_info["safeSearch"],
                        relevanceLanguage=search_info["relevanceLanguage"],
                        type=search_info["type"],
                        regionCode=search_info["regionCode"],
                    )
                    .execute()
                )
            else:
                videos_response = (
                    context.youtube.search()
                    .list(
                        part=search_info["part"],
                        q=search_info["q"],
                        maxResults=search_info["maxResults"],
                        order=search_info["order"],
                        safeSearch=search_info["safeSearch"],
                        relevanceLanguage=search_info["relevanceLanguage"],
                        type=search_info["type"],
                        regionCode=search_info["regionCode"],
                        pageToken=nxPage,
                    )
                    .execute()
                )

            responses.append(videos_response)

            if "nextPageToken" in videos_response.keys():
                nxPage = videos_response["nextPageToken"]
                search_info["pages"] += 1
            else:
                nxPage = ""
                search_info["pages"] += 1
    except Exception as e:
        print("YT SEARCH: ERROR SEARCH KEYWORD")
        if "quota" in str(e).lower():
            wait_until_midnight()

    return responses


def insert_raw_data_minio(video_responses: list, path: str, bucket_name: str, context):
    try:
        for i, response in zip(range(len(video_responses)), video_responses):
            Fname = "page-{:03d}.json".format(i)
            tmp = tempfile.NamedTemporaryFile()
            with open(tmp.name, "w") as f:
                json.dump(response, f, ensure_ascii=False, indent=4)
            object_name = "{}/{}".format(path, Fname)
            context.client.fput_object(
                bucket_name,
                object_name,
                tmp.name,
                content_type="application/json",
            )
            tmp.close()
    except Exception as e:
        print("YT SEARCH: ERROR INSERTING FILES ON MINIO")
        print(e)


def produce_messages_for_collection(video_responses: list, search_info: dict, context):

    for json_object in video_responses:
        try:
            if "items" in json_object.keys():
                for item in json_object["items"]:
                    row = {
                        "dataOwner": search_info["dataOwner"],
                        "createdAt": search_info["createdAt"],
                        "searchKeyword": search_info["q"],
                        "keywordId": search_info["keywordId"],
                        "videoId": item["id"]["videoId"],
                        "producer": "youtube-search.{}".format(search_info["searchId"]),
                    }
                    m = json.loads(json.dumps(row))
                    # send data to be collected
                    context.producer.send("youtube", value=m)
        except Exception as e:
            print(e)
            continue


def create_meta(search_info: dict, bucket_name: str, path: str, context):
    try:
        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, "w") as f:
            json.dump(search_info, f, ensure_ascii=False, indent=4)
        object_name = "{}/{}".format(path, "meta.json")
        context.client.fput_object(
            bucket_name, object_name, tmp.name, content_type="application/json"
        )
        tmp.close()
    except Exception as e:
        print("YT SEARCH ERROR INSERTING META.JSON")
        print(e)


def get_keywords(context):
    cur = None
    try:

        cur = context.conn.cursor()

        query = (
            "SELECT keyword, relevance_language, region_code,"
            " max_results, safe_search, keyword_id FROM search_keywords"
        )

        cur.execute(query)

        row = cur.fetchall()
    except Exception as e:
        print("ERROR FIND KEYWORDS:")
        print(e)
    finally:
        cur.close()

    return row if row else []


def insert_search_postgres(search_info: dict, conn):
    cur = None
    try:
        cur = conn.cursor()

        query = (
            "INSERT INTO yt_search (data_owner, created_at,"
            " search_id, yt_part, q, max_results, yt_order, safe_search,"
            " relevance_language, search_type, region_code, results_path,"
            " keyword_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        # execute the query with parameters
        cur.execute(
            query,
            (
                search_info["dataOwner"],
                search_info["createdAt"],
                search_info["searchId"],
                search_info["part"],
                search_info["q"],
                search_info["maxResults"],
                search_info["order"],
                search_info["safeSearch"],
                search_info["relevanceLanguage"],
                search_info["type"],
                search_info["regionCode"],
                search_info["resultsPath"],
                search_info["keywordId"],
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

    bucket_name = "youtube-artifacts"

    dataOwner = "FBK-YOUTUBE"

    # data = json.loads(event.body.decode("utf-8"))

    keywords = get_keywords(context=context)

    for (
        keyword,
        relevanceLanguage,
        regionCode,
        maxResults,
        safeSearch,
        keywordId,
    ) in keywords:

        query_uuid = str(uuid.uuid4())
        path = generate_folder(bucket_name=bucket_name, keyword=keyword)
        date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        search_info = {
            "dataOwner": dataOwner,
            "createdAt": date,
            "searchId": query_uuid,
            "part": ["snippet", "id"],
            "q": keyword,
            "maxResults": maxResults,
            "order": "viewCount",
            "safeSearch": safeSearch,
            "relevanceLanguage": relevanceLanguage,
            "type": "video",
            "regionCode": regionCode,
            "resultsPath": path,
            "keywordId": keywordId,
            "pages": 0,
        }

        responses = search_keyword(
            keyword=keyword, search_info=search_info, context=context
        )

        if len(responses) > 0:

            insert_search_postgres(search_info, context.conn)

            # insert files into Minio

            insert_raw_data_minio(
                video_responses=responses,
                path=path,
                bucket_name=bucket_name,
                context=context,
            )

            # create the registry of the search in Iceberg
            row = dict(search_info)
            row["table"] = "youtube-search"
            m = json.loads(json.dumps(row))
            context.producer.send("collected_metadata", value=m)

            # collect video IDs and send to be collected
            produce_messages_for_collection(
                video_responses=responses,
                search_info=search_info,
                context=context,
            )
            # create meta
            create_meta(
                search_info=search_info,
                bucket_name=bucket_name,
                path=path,
                context=context,
            )
