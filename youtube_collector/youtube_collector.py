from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import json
from pulsar import Client
import uuid

# setup pulsar client and topic
client = Client('pulsar://192.168.49.2:31482')
producer = client.create_producer('popular-italy')


# Set up the API client
api_key = 'your_apikey'
youtube = build('youtube', 'v3', developerKey=api_key)

# Get the video IDs for the current trending videos
video_ids = []
try:
    videos_response = youtube.videos().list(
        part='id',
        chart='mostPopular',
        regionCode='IT',
        maxResults=10
    ).execute()
    for video in videos_response.get('items', []):
        video_ids.append(video['id'])
except Exception as e:
    print('Error get popular videos:', e)
print("VIDEO IDS")
print(video_ids)
# Get the comments for each video
for video_id in video_ids:
    try:
        comment_threads = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            textFormat='plainText'
        ).execute()
        for item in comment_threads['items']:

            comment = {}
            comment['uuid'] = str(uuid.uuid4())
            comment['comment_thumbnail'] = f'https://img.youtube.com/vi/{video_id}/hqdefault.jpg'
            comment['video_id'] = video_id
            comment['comment_id'] = item["snippet"]["topLevelComment"]["id"]
            comment['comment_text'] = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"] # or textOriginal
            comment['comment_author_id'] = item["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"]
            comment['comment_author_name'] = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
            comment['comment_likes_count'] = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            comment['comment_date'] = item["snippet"]["topLevelComment"]["snippet"]["updatedAt"] # or (originally) publishedAt
            comment['comment_replies_count'] = item['snippet']['totalReplyCount']
            comment['comment_public'] = item["snippet"]["isPublic"]
    
            # send comment
            producer.send(json.dumps(comment).encode('utf-8'))
            time.sleep(10)
            
    except Exception as e:
        print('Error get comment:', e)
        print(video_id)