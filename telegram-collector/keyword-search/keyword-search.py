import datetime
import json
import os
import uuid
from datetime import timezone
from pathlib import Path

import collegram
import fsspec
import nest_asyncio
import psycopg
from kafka import KafkaProducer
from telethon import TelegramClient
from telethon.sessions import StringSession


async def init_context(context):
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret = os.environ["MINIO_SECRET_KEY"]
    minio_home = os.environ["MINIO_HOME"]
    storage_options = {
        "endpoint_url": f"https://{minio_home}",
        "key": access_key,
        "secret": secret,
    }
    fs = fsspec.filesystem("s3", **storage_options)
    setattr(context, "fs", fs)

    # Connect to an existing database
    connection = psycopg.connect(
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PW"],
        host=os.environ["POSTGRES_IP"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
    )
    setattr(context, "connection", connection)

    # prefix = os.environ["TELEGRAM_OWNER"].upper()
    client = TelegramClient(
        StringSession(os.environ["AI4TRUST_TG_SESSION"]),
        os.environ["AI4TRUST_API_ID"],
        os.environ["AI4TRUST_API_HASH"],
        flood_sleep_threshold=24 * 3600,
    )

    await client.start(os.environ["AI4TRUST_PHONE_NUMBER"])
    setattr(context, "client", client)

    broker = os.environ.get("KAFKA_BROKER")
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda x: json.dumps(x, default=_json_default).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    else:
        return repr(value)


def _iceberg_json_default(value):
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return repr(value)


def insert_into_postgres(conn, values: list):
    cur = None
    try:
        cur = conn.cursor()
        query = (
            "INSERT INTO channels_to_query"
            "(id, access_hash, query_id, search_date, data_owner,"
            " search_keyword, language_code) VALUES "
            "(%s, %s, %s, %s, %s, %s, %s)"
        )
        for v in values:
            cur.execute(
                query,
                (
                    v["id"],
                    v["access_hash"],
                    v["query_id"],
                    v["search_date"],
                    v["data_owner"],
                    v["search_keyword"],
                    v["language_code"],
                ),
            )
        # commit the changes to the database
        conn.commit()

    except Exception as e:
        print("ERROR INSERTING channels_to_query")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def remove_duplicates(conn, values: list):
    data = []
    cur = None
    try:
        cur = conn.cursor()
        for v in values:
            query = (
                "SELECT id, access_hash FROM channels_to_query"
                " WHERE id = %s AND access_hash = %s"
            )

            cur.execute(query, (v["id"], v["access_hash"]))

            row = cur.fetchone()

            if not row:
                data.append(v)

    except Exception as e:
        print("ERROR SEARCHING channels_to_query")
        print(e)
    finally:
        cur.close()

    return data


def handler(context, event):
    # event is not used, it's a cron job
    # add nest asyncio for waiting calls
    nest_asyncio.apply()

    fs = context.fs
    producer = context.producer
    client = context.client
    connection = context.connection

    context.logger.info("# Started keyword search")
    kw_dir_path = Path("/telegram") / "keywords"
    for kw_fpath in fs.ls(str(kw_dir_path)):
        language_code = Path(kw_fpath).stem
        context.logger.info(f"## Started with keywords in {language_code}")
        with fs.open(str(kw_fpath), "r", encoding="utf-8") as f:
            keywords = [line for line in f]
            for kw in keywords:
                context.logger.info(f"### Started with keyword {kw}")
                try:
                    data = []
                    date = datetime.datetime.now().astimezone(timezone.utc)
                    query_uuid = str(uuid.uuid4())
                    api_chans = collegram.channels.search_from_api(client, kw)
                    channels = api_chans

                    for c_id, c_hash in channels.items():
                        row = {
                            "id": c_id,
                            "access_hash": c_hash,
                            "query_id": query_uuid,
                            "search_date": date,
                            "data_owner": os.environ["TELEGRAM_OWNER"],
                            "search_keyword": kw,
                            "language_code": language_code,
                            "producer": "channels_to_query.{}".format(query_uuid),
                        }
                        data.append(row)

                    # verify if they already exists
                    rows_to_insert = remove_duplicates(connection, data)
                    insert_into_postgres(connection, rows_to_insert)
                    # send channels to be ranked
                    for d in rows_to_insert:
                        m = json.loads(json.dumps(d, default=_json_default))
                        producer.send("chans_to_query", value=m)
                except Exception as e:
                    print("ERRO SEARCHING KEYWORD")
                    print(kw)
                    print(e)
                    continue

    context.logger.info("# Ended keyword search")
    m = json.loads(json.dumps({"status": "init_done"}))
    producer.send("telegram-keywords", value=m)
