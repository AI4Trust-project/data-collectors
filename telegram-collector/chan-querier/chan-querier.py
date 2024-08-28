import json
import datetime
import os
from pathlib import Path

import collegram
import fsspec
import nest_asyncio
import psycopg
from kafka import KafkaProducer
from lingua import LanguageDetectorBuilder
from telethon import TelegramClient
from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    UsernameInvalidError,
)
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
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    setattr(context, "producer", producer)


def update_postgres(conn, channel_update_params: dict):
    cur = None
    try:
        cur = conn.cursor()
        query = "UPDATE channels_to_query SET collection_priority = %s, language_code = %s WHERE id = %s"
        cur.execute(
            query,
            (
                channel_update_params["collection_priority"],
                channel_update_params["language_code"],
                channel_update_params["id"],
            ),
        )

        # commit the changes to the database
        conn.commit()

    except Exception as e:
        print("ERROR UPDATING channels_to_query")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def get_priority(lang_code, lang_prios):
    return lang_prios.get(lang_code, 0)


def handler(context, event):
    nest_asyncio.apply()
    # Keep, in future if we want to use more than 1 key, this is essential
    key_name = os.environ["TELEGRAM_OWNER"]

    # TODO: update or save new every time? issue: propagate data obtained through
    # + who starts the chain by calling chan-querier?
    # iterating through mssages, such as "forwards_from".
    fs = context.fs
    producer = context.producer
    client = context.client
    connection = context.connection

    data = json.loads(event.body.decode("utf-8"))
    channel_id = data["channel_id"]
    access_hash = data["access_hash"]
    channel_username = data.get("channel_username")
    # TODO: following to be set in `messages_querier` through an SQL update, whenever a
    # forwarded channel is found
    # distance_from_core = data["distance_from_core"]
    # nr_forwarding_channels = data["nr_forwarding_channels"]

    try:
        query_time = (
            datetime.datetime.now()
            .astimezone(datetime.timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        channel_full = collegram.channels.get_full(
            client,
            channel_username=channel_username,
            channel_id=channel_id,
            access_hash=access_hash,
        )
        update_d = {"id": channel_id, "channel_last_queried_at": query_time}
        collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")

    except (
        ChannelInvalidError,
        ChannelPrivateError,
        UsernameInvalidError,
        ValueError,
    ) as e:
        # TODO What then?
        # For all but ChannelPrivateError, can try with another key (TODO: add to
        # list of new channels?).
        # logger.warning(f"could not get data for listed channel {channel_id}")
        raise e

    data_path = Path("/telegram/")
    paths = collegram.paths.ProjectPaths(data=data_path)

    def insert_anon_pair(original, anonymised):
        insert_d = {"original": original, "anonymised": anonymised}
        collegram.utils.insert_into_postgres(
            connection, table="anonymisation_map", values=insert_d
        )

    anonymiser = collegram.utils.HMAC_anonymiser(save_func=insert_anon_pair)

    lang_priorities = {lc: 1 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]}
    lang_detector = LanguageDetectorBuilder.from_all_languages().build()

    # Order chats such that one corresponding to channel_full is first, so we don't
    # query it twice
    chats = [c for c in channel_full.chats if c.id == channel_id] + [
        c for c in channel_full.chats if c.id != channel_id
    ]
    for i, chat in enumerate(chats):
        if i > 0:
            query_time = (
                datetime.datetime.now()
                .astimezone(datetime.timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            channel_full = collegram.channels.get_full(
                client,
                channel=chat,
            )
            update_d = {"id": channel_id, "channel_last_queried_at": query_time}
            collegram.utils.update_postgres(
                connection, "channels_to_query", update_d, "id"
            )

        channel_full_d = json.loads(channel_full.to_json())

        participants_iter = (
            collegram.users.get_channel_participants(client, chat)
            if channel_full_d["full_chat"].get("can_view_participants", False)
            else []
        )
        channel_full_d["participants"] = [
            json.loads(u.to_json()) for u in participants_iter
        ]

        recommended_chans = {
            c.id: c.access_hash
            for c in collegram.channels.get_recommended(client, chat)
        }
        # TODO: add into `channels_to_query`, or update, incrementing `nr_channels_recommended_in`
        channel_full_d["recommended_channels"] = list(recommended_chans.keys())

        for content_type, f in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.items():
            count = collegram.messages.get_channel_messages_count(client, chat, f)
            channel_full_d[f"{content_type}_count"] = count

        lang_code = collegram.text.detect_chan_lang(channel_full_d, lang_detector)
        prio = get_priority(lang_code, lang_priorities)
        update_d = {
            "id": chat.id,
            "collection_priority": prio,
            "language_code": lang_code,
            # 'distance_from_core': distance_from_core,
            # 'nr_forwarding_channels': nr_forwarding_channels,
        }
        update_postgres(connection, update_d)

        channel_full_d = collegram.channels.anon_full_dict(
            channel_full_d,
            anonymiser,
        )
        collegram.channels.save(channel_full_d, paths, key_name, fs=fs)
        # TODO: send any Kafka message to say this is done?
