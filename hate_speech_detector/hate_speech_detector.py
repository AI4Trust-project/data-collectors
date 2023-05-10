import json
import random
import time

from minio import Minio
from pulsar import Function


class HateSpeechDetector(Function):
    def __init__(self) -> None:
        # try to load model
        print("load model")

    def process(self, input, context):
        logger = context.get_logger()
        comment = json.loads(input)
        # processing
        time.sleep(15)
        random_label = random.choice(["HATE_SPEECH", "NOT_HATE_SPEECH"])
        comment["hate_speech_classification"] = str(random_label)
        comment["type"] = "processing_ml"
        return json.dumps(comment).encode("utf-8")

