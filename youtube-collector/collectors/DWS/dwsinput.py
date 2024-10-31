import json
import os
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


def get_channel_id(video_id, context):
    """
    Get the channel ID associated with a given video ID.

    Args:
    video_id (str): The ID of the YouTube video.

    Returns:
    str: The channel ID associated with the video.
    """
    request = context.youtube.videos().list(part="snippet", id=video_id)
    response = request.execute()
    return response["items"][0]["snippet"]["channelId"]


def get_subscriber_count(channel_id, context):
    """
    Get the number of subscribers for a given channel ID.

    Args:
    channel_id (str): The ID of the YouTube channel.

    Returns:
    int: The number of subscribers for the channel.
    """
    request = context.youtube.channels().list(part="statistics", id=channel_id)
    response = request.execute()
    return int(response["items"][0]["statistics"]["subscriberCount"])


def normalize_subscribers(subscriber_count, max_value=300_000_000, min_value=0):
    """
    Normalize the subscriber count to a value between 0 and 1.

    Args:
    subscriber_count (int): The number of subscribers.
    max_value (int): The maximum value for normalization (default is 300,000,000).
    min_value (int): The minimum value for normalization (default is 0).

    Returns:
    float: The normalized subscriber count.
    """
    if subscriber_count > max_value:
        return 1
    return (subscriber_count - min_value) / (max_value - min_value)


def get_normalized_subscriber_count(video_id, context):
    """
    Get the normalized subscriber count for the channel of a given video ID.

    Args:
    video_id (str): The ID of the YouTube video.

    Returns:
    float: The normalized subscriber count for the channel.
    """
    normalized_value = 0
    try:
        channel_id = get_channel_id(
            video_id, context
        )  # Fetch the channel ID using the video ID
        subscriber_count = get_subscriber_count(
            channel_id, context
        )  # Fetch the subscriber count using the channel ID
        normalized_value = normalize_subscribers(
            subscriber_count
        )  # Normalize the subscriber count
    except Exception as e:
        print("ERROR COLLECTING SUBSCRIBERS")
        print(e)
        if "quota" in str(e).lower():
            wait_until_midnight()

    return normalized_value


def handler(context, event):

    data = json.loads(event.body.decode("utf-8"))
    video_id = data["videoId"]
    keyword = data["searchKeyword"]
    dataOwner = "FBK-YOUTUBE"

    normalized_value = get_normalized_subscriber_count(video_id, context)

    date = datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    message = {
        "collectionId": data["collectionId"],
        "dataOwner": dataOwner,
        "collectionDate": date,
        "videoId": video_id,
        "searchKeyword": keyword,
        "keywordId": data["keywordId"],
        "producer": data["producer"],
        "video_url": "https://www.youtube.com/watch?v={}".format(video_id),
        "relevance_language": data.get("defaultAudioLanguage", "en"),
        "description": data.get("description", ""),
        "title": data.get("title", ""),
        "thumbnail": "https://img.youtube.com/vi/{}/default.jpg".format(video_id),
        "normalisedSubscribers": float(normalized_value),
    }

    message["table"] = "youtube-video-dws"
    m = json.loads(json.dumps(message))
    # send data to iceberg
    context.producer.send("collected_metadata", value=m)
    # send data to be dws
    context.producer.send("youtube-dws", value=m)
