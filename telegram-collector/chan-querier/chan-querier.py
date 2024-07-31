import json
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

    key_name = os.environ["TELEGRAM_OWNER"]
    fs = context.fs
    producer = context.producer
    client = context.client
    connection = context.connection

    data = json.loads(event.body.decode("utf-8"))
    channel_id = str(data["id"])
    access_hash = data["access_hash"]

    input_chan = collegram.channels.get_input_chan(
        client,
        channel_id=channel_id,  # here OG ID
        access_hash=access_hash,
    )

    data_path = Path("/telegram/")
    paths = collegram.paths.ProjectPaths(data=data_path)

    anon_id = channel_id
    chan_paths = collegram.paths.ChannelPaths(anon_id, paths)
    saved_channel_full_d = collegram.channels.load(anon_id, paths, fs=fs)
    anonymiser = collegram.utils.HMAC_anonymiser(save_path=chan_paths.anon_map, fs=fs)

    try:
        channel_full, channel_full_d = collegram.channels.get_full(
            client,
            paths,
            anonymiser,
            # key_name=key_name,
            channel=input_chan,
            channel_id=channel_id,
            force_query=True,
        )
    except (
        ChannelInvalidError,
        ChannelPrivateError,
        UsernameInvalidError,
        ValueError,
    ):
        # TODO What then?
        # For all but ChannelPrivateError, can try with another key (TODO: add to
        # list of new channels?).
        # logger.warning(f"could not get data for listed channel {channel_id}")
        pass

    lang_priorities = {lc: 1 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]}
    lang_detector = LanguageDetectorBuilder.from_all_languages().build()

    # Order chats such that one corresponding to channel_full is first, so we don't
    # query it twice
    chats = [c for c in channel_full.chats if c.id == channel_id] + [
        c for c in channel_full.chats if c.id != channel_id
    ]
    for i, chat in enumerate(chats):
        if i > 0:
            channel_full, channel_full_d = collegram.channels.get_full(
                client,
                paths,
                anonymiser,
                channel=chat,
                force_query=True,
            )

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

        lang_code = collegram.text.detect_chan_lang(
            channel_full_d, anonymiser.inverse_anon_map, lang_detector
        )
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
        for key in ["recommended_channels", "forwards_from"]:
            channel_full_d[key] = list(
                set(channel_full_d.get(key, [])).union(
                    saved_channel_full_d.get(key, [])
                )
            )
        collegram.channels.save(channel_full_d, paths, key_name, fs=fs)
        # TODO: send any Kafka message to say this is done?
