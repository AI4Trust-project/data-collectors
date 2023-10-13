import json
import os

from kafka import KafkaProducer

thumbnail_topic = "thumbnails"
liked_comment_topic = "liked-comments"
common_words_topic = "common-ita-words"
italian_common_words = [
    "di",
    "che",
    "è",
    "la",
    "il",
    "a",
    "non",
    "e",
    "un",
    "in",
    "con",
    "mi",
    "per",
    "sono",
    "ma",
    "hai",
    "ci",
    "ti",
    "lo",
    "l'",
]


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)


def find_key_words(comment: str):
    for word in italian_common_words:
        if word in comment:
            return True
    return False


def handler(context, event):
    context.logger.info("Message Content: {}".format(event))

    comment = json.loads(event.body.decode("utf-8"))

    # create new topic
    if comment["comment_likes_count"] > 0:
        context.producer.send(liked_comment_topic, value=comment)

    # find if the comment has common words
    if find_key_words(comment["comment_text"]):
        context.producer.send(common_words_topic, value=comment)

    # send data to download the thumbnail
    context.producer.send(thumbnail_topic, value=comment)
