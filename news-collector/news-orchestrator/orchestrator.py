import json
import os
from datetime import datetime, timedelta, timezone
import uuid

from kafka import KafkaProducer


def init_context(context):
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get("KAFKA_BROKER")],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    setattr(context, "producer", producer)


def get_keywords():
    """Get keywords from database"""
    data = []
    config = {
        "keyword": "Climate Change",
        "num_records": 250,
        "country": "UK",
    }
    data.append(config)
    return data


def handler(context, event):
    """Generate Kafka messages based on the configuration and send them."""
    producer = context.producer
    configs = get_keywords()

    for config in configs:

        keyword = config["keyword"]
        num_records = config["num_records"]
        country = config["country"]
        domain = config.get("domain", None)
        domain_exact = config.get("domain_exact", None)
        theme = config.get("theme", None)
        near = config.get("near", None)
        repeat = config.get("repeat", None)

        data_owner = "FBK-NEWS"

        # start_date = datetime.now() - timedelta(days=365 * 5) # 5 years ago

        end_date = datetime.now()
        start_date = datetime.now() - timedelta(days=2)  # 2 days ago

        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        message = {
            "data_owner": data_owner,
            "created_at": datetime.now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "search_id": str(uuid.uuid4()),
            "keyword": keyword,
            "num_records": num_records,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "country": country,
            "domain": domain,
            "domain_exact": domain_exact,
            "theme": theme,
            "near": near,
            "repeat": repeat,
        }

        producer.send("search_parameters", value=json.dumps(message))
