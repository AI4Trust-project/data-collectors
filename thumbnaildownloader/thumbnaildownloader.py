import json
import os
import urllib.request

from minio import Minio
from pulsar import Function


class ThumbnailDownloader(Function):
    def __init__(self):
        self.minio_client = Minio(
            os.environ.get("MINIO_HOME"),
            access_key=os.environ.get("MINIO_ACCESS_KEY"),
            secret_key=os.environ.get("MINIO_SECRET_KEY"),
            secure=False,
        )

    def process(self, input, context):
        logger = context.get_logger()
        comment = json.loads(input)
        try:
            image_url = comment["comment_thumbnail"]
            image_name = comment["video_id"] + ".jpg"
            image_file = urllib.request.urlopen(image_url)
            self.minio_client.put_object(
                "thumbnails",
                image_name,
                image_file,
                length=int(image_file.headers["Content-Length"]),
                content_type="image/jpeg",
            )
            image_file.close()
            comment["img_location"] = "thumbnails/{}".format(image_name)
            comment["type"] = "minio_stack"
            return json.dumps(comment).encode("utf-8")
        except Exception as e:
            print(e)

