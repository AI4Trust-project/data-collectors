import json
import random
import time
import os

from kafka import KafkaProducer


def init_context(context):
    # try to load model
    print("load model")
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)

def handler(context, event):

    comment = json.loads(event.body.decode("utf-8"))

    # processing
    time.sleep(2)
    random_label = random.choice(["HATE_SPEECH", "NOT_HATE_SPEECH"])
    comment["hate_speech_classification"] = str(random_label)
    comment["type"] = "processing_ml"

    context.producer.send("hate", value=comment)

