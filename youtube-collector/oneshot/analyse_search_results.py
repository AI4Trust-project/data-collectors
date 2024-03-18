import glob
import json
import os
import pandas as pd
import datetime


def read_json_files(path):
    """
    Reads all JSON files in the given path and its subfolders.

    Args:
      path: The path to the folder containing the JSON files.

    Returns:
      A list of all the JSON objects in the given path and its subfolders.
    """

    json_objects = []
    for filename in glob.glob(os.path.join(path, "**/*.json"), recursive=True):
        with open(filename, "r") as f:
            json_object = json.load(f)
            json_objects.append(json_object)
    return json_objects


def extract_video_ids(json_objects, keyword):
    """
    Extracts the video IDs from the given JSON objects.

    Args:
      json_objects: A list of JSON objects.

    Returns:
      A list of all the video IDs in the given JSON objects.
    """

    video_ids = []
    for json_object in json_objects:
        if "items" in json_object.keys():
            for item in json_object["items"]:
                my_record = {
                    "keyword": keyword,
                    "video_id": item["id"]["videoId"],
                    "publishedAt": item["snippet"]["publishedAt"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "channelTitle": item["snippet"]["channelTitle"]
                }
                video_ids.append(my_record)
    return video_ids


def main():
    """
    The main function.
    """

    # Get the path to the folder containing the JSON files.


    keywords = []

    keywords_file = "keywords.txt"
    # retrive keywords from file
    with open(keywords_file, "r") as file:
        for line in file:
            line = line.replace(".", "")
            line = line.replace("\n", "")
            words = line.split(",")
            for word in words:
                if word[0] == " ":
                    word = word[1:]
                keywords.append(word.lower())
      
    all_records = []

    for keyword in keywords:
      path = f"/home/vbezerra/Documents/ai4trust.pulsar-poc/data/{keyword}"

      

      # Read all the JSON files in the given path and its subfolders.
      json_objects = read_json_files(path)

      # Extract the video IDs from the given JSON objects.
      data = extract_video_ids(json_objects, keyword=keyword)

      # Sort data by publishedAt (assuming it's a string, convert to datetime.datetime if needed)
    
      try:
          data.sort(key=lambda item: datetime.datetime.strptime(item['publishedAt'], '%Y-%m-%dT%H:%M:%SZ'))
      except ValueError:  # Handle cases where publishedAt is not a string
          data.sort(key=lambda item: item['publishedAt'])


      all_records.extend(data)

    

    df = pd.DataFrame.from_records(all_records)
    df.to_csv("output_csv.csv", encoding='utf-8', index=False)
    



if __name__ == "__main__":
    main()
