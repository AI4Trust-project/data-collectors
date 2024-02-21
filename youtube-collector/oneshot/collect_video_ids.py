import glob
import json
import os


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


def extract_video_ids(json_objects):
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
                video_ids.append(item["id"]["videoId"])
    return video_ids


def main():
    """
    The main function.
    """

    # Get the path to the folder containing the JSON files.
    
    keyword = "weather"
    path = f"/home/vbezerra/Documents/temp/data/{keyword}"

    data = []

    # Read all the JSON files in the given path and its subfolders.
    json_objects = read_json_files(path)

    # Extract the video IDs from the given JSON objects.
    video_ids = extract_video_ids(json_objects)

    # Print the video IDs.
    print("The video IDs are:")
    print(len(video_ids))
    for v in video_ids:
      a = {"keyword": keyword, "video_id": v}
      data.append(a)

    with open("video_ids", "w") as f:
      f.write(json.dumps(data))



if __name__ == "__main__":
    main()
