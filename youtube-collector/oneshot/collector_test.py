import json
import os
import time
import datetime
import pytz

from googleapiclient.discovery import build
from tqdm import trange

api_key = os.environ.get("YOUTUBE_API_KEY")

youtube = build("youtube", "v3", developerKey=api_key)

keywords_file = "keywords.txt"

def sleep_until_midnight_pacific_time():
    """Sleeps until midnight Pacific Time."""

    # Get the current time in Pacific Time.
    pacific_time = datetime.datetime.now(pytz.timezone('US/Pacific'))

    # Calculate the next midnight in Pacific Time.
    next_midnight_pacific_time = pacific_time.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)

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
    print(keyword)
    try:
        search_info = {
            "q": keyword,
            "page": 0,
            "order": "relevance",
            "relevanceLanguage": "en",
        }

        nxPage = ""

        videos_response = ""

        try:
            videos_response = (
                youtube.search()
                .list(
                    part=["snippet", "id"],
                    q=search_info["q"],
                    maxResults=50,
                    order=search_info["order"],
                    safeSearch="none",
                    relevanceLanguage=search_info["relevanceLanguage"],
                    type="video",
                    regionCode="it",
                )
                .execute()
            )
        except Exception as e:
            print("Error searching: {}".format(e))
            keywords.append(keyword)
            with open(keywords_file, "w") as file:
                file.write(",".join(keywords))
            exit()

        if videos_response != "":
            videos_response["search_info"] = search_info

            # dump

            with open("data/{}.json".format(keyword), "w", encoding="utf-8") as f:
                json.dump(videos_response, f, ensure_ascii=False, indent=4)

            resuts_per_page = int(videos_response["pageInfo"]["resultsPerPage"])

            if "nextPageToken" in videos_response.keys():
                nxPage = videos_response["nextPageToken"]

        while nxPage != "":
            search_info["page"] += 1
            videos_response = ""

            try:
                videos_response = (
                    youtube.search()
                    .list(
                        part=["snippet", "id"],
                        q=search_info["q"],
                        maxResults=50,
                        order=search_info["order"],
                        safeSearch="none",
                        relevanceLanguage=search_info["relevanceLanguage"],
                        type="video",
                        regionCode="it",
                        pageToken=nxPage,
                    )
                    .execute()
                )
            except Exception as e:
                if "quotaExceeded" in str(e):
                    now = datetime.datetime.now()
                    print(now)
                    print("API quota has been reached.")
                    exit()
                else:
                    print("Error getting page: {}".format(e))
                    nxPage = ""

            if videos_response != "":
                videos_response["search_info"] = search_info

                with open("data/{}.json".format(keyword), "a", encoding="utf-8") as f:
                    f.write("\n")
                    json.dump(videos_response, f, ensure_ascii=False, indent=4)

                if "nextPageToken" in videos_response.keys():
                    nxPage = videos_response["nextPageToken"]
                else:
                    nxPage = ""

        # update keywords
        with open(keywords_file, "w") as file:
            file.write(",".join(keywords))

    except Exception as e:
        print("Error:", e)
        keywords.append(keyword)
        with open(keywords_file, "w") as file:
            file.write(",".join(keywords))
        continue
