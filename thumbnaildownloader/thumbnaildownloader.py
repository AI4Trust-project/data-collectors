from pulsar import Function
from minio import Minio
import urllib.request
import json

class ThumbnailDownloader(Function):
  def __init__(self):
    self.minio_client = Minio(
      "minio_addr",
      access_key="key",
      secret_key="key",
      secure=False
      )

  def process(self, input, context):
    logger = context.get_logger()
    comment = json.loads(input)
    
    try:
      image_url = comment['comment_thumbnail']
      image_name = comment['video_id'] + ".jpg"
      image_file = urllib.request.urlopen(image_url)
      self.minio_client.put_object("thumbnails", image_name, image_file, length=int(image_file.headers["Content-Length"]), content_type="image/jpeg")
      image_file.close()
      comment['img_location'] = "thumbnails/{}".format(image_name)
      comment['type'] = "minio_stack"
      return json.dumps(comment).encode("utf-8")
    except Exception as e:
       print(e)
