import datetime
import json
import os
import time

import pytz
from googleapiclient.discovery import build
from tqdm import trange

api_key = os.environ.get("YOUTUBE_API_KEY")

youtube = build("youtube", "v3", developerKey=api_key)

keywords_file = "keywords.txt"

datafolder = "data"


def sleep_until_midnight_pacific_time():
    """Sleeps until midnight Pacific Time."""

    # Get the current time in Pacific Time.
    pacific_time = datetime.datetime.now(pytz.timezone("US/Pacific"))

    # Calculate the next midnight in Pacific Time.
    next_midnight_pacific_time = pacific_time.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + datetime.timedelta(days=1)

    # Sleep until the next midnight in Pacific Time.
    time_to_sleep = next_midnight_pacific_time - pacific_time
    time_to_sleep_in_seconds = time_to_sleep.total_seconds()
    time.sleep(time_to_sleep_in_seconds)


keywords = []

# retrive keywords from file
with open(keywords_file, "r") as file:
    for line in file:
        line = line.replace(".", "")
        line = line.replace("\n", "")
        words = line.split(",")
        for word in words:
            if word[0] == " ":
                word = word[1:]
            keywords.append(word)

for i in trange(len(keywords)):
    keyword = keywords.pop(0)
    keyword = keyword.lower()
    print(keyword)

    nxPage = "start"

    try:
        # create folder
        keyword_folder = os.path.join(datafolder, keyword)

        isExists = os.path.exists(keyword_folder)
        if not isExists:
            os.makedirs(keyword_folder)

        search_info = {
            "part": ["snippet", "id"],
            "q": keyword,
            "maxResults": 50,
            "order": "viewCount",
            "safeSearch": "none",
            "relevanceLanguage": "en",
            "type": "video",
            "regionCode": "gb",
            "pages": 0,
        }

        while nxPage != "":
            videos_response = {}

            if nxPage == "start":
                videos_response = (
                    youtube.search()
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
                    youtube.search()
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

            Fname = "page-{:03d}.json".format(search_info["pages"])
            file_name = os.path.join(keyword_folder, Fname)

            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(videos_response, f, ensure_ascii=False, indent=4)

            if "nextPageToken" in videos_response.keys():
                nxPage = videos_response["nextPageToken"]
                search_info["pages"] += 1
            else:
                nxPage = ""

        # update keywords
        with open(keywords_file, "w") as file:
            file.write(",".join(keywords))

        # create meta
        meta_file = os.path.join(keyword_folder, "meta.json")
        search_info["pages"] += 1
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(search_info, f, ensure_ascii=False, indent=4)

    except Exception as e:
        print("Error searching: {}".format(e))
        keywords.append(keyword)
        with open(keywords_file, "w") as file:
            file.write(",".join(keywords))
        if "quotaExceeded" in str(e):
            now = datetime.datetime.now()
            print(now)
            print("API quota has been reached.")
            exit()
        else:
            continue
