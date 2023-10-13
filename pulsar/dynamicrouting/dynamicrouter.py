from pulsar import Function

import json


class DynamicRouting(Function):
    def __init__(self) -> None:
        self.thumbnail_topic = "persistent://public/default/thumbnails"
        self.liked_comment_topic = "persistent://public/default/liked-comments"
        self.common_words_topic = "persistent://public/default/common-ita-words"
        self.italian_common_words = [
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

    def find_key_words(self, comment):
        for word in self.italian_common_words:
            if word in comment:
                return True
        return False

    def process(self, input, context):
        logger = context.get_logger()
        logger.info("Message Content: {}".format(input))

        comment = json.loads(input)

        # create new topic
        if comment["comment_likes_count"] > 0:
            context.publish(self.liked_comment_topic, input)

        # find if the comment has common words
        if self.find_key_words(comment["comment_text"]):
            context.publish(self.common_words_topic, input)

        # send data to download the thumbnail
        context.publish(self.thumbnail_topic, input)
