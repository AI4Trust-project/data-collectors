import base64
import datetime
import json
import os
import uuid
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


def iceberg_json_dumps(d: dict):
    return json.dumps(d, default=_iceberg_json_default).encode("utf-8")


def handle_recommended(
    rec_id, rec_hash, connection, pred_dist_from_core, producer, lang_priorities
):
    with connection.cursor() as cur:
        cur.execute(
            "SELECT"
            " created_at,"
            " channel_last_queried_at,"
            " language_code,"
            " nr_participants,"
            " nr_messages,"
            " nr_forwarding_channels,"
            " nr_recommending_channels,"
            " nr_linking_channels,"
            " distance_from_core"
            " FROM channels_to_query"
            f" WHERE id = {rec_id}"
        )
        prio_info = cur.fetchone()

    if prio_info is None:
        insert_d = {
            "id": rec_id,
            "access_hash": rec_hash,
            "data_owner": os.environ["TELEGRAM_OWNER"],
            "nr_recommending_channels": 1,
            "distance_from_core": pred_dist_from_core + 1,
        }
        collegram.utils.insert_into_postgres(connection, "channels_to_query", insert_d)
        producer.send("chans_to_query", value=insert_d)

    else:
        (
            created_at,
            channel_last_queried_at,
            language_code,
            participants_count,
            messages_count,
            nr_forwarding_channels,
            nr_recommending_channels,
            nr_linking_channels,
            distance_from_core,
        ) = prio_info
        new_dist_from_core = min(pred_dist_from_core + 1, distance_from_core)
        update_d = {
            "id": rec_id,
            "nr_recommending_channels": nr_recommending_channels + 1,
            "distance_from_core": new_dist_from_core,
        }

        # If channel has already been queried by `chan-querier`, then recompute
        # priority.
        if channel_last_queried_at is not None:
            lifespan_seconds = (created_at - channel_last_queried_at).total_seconds()
            priority = collegram.channels.get_explo_priority(
                language_code,
                messages_count,
                participants_count,
                lifespan_seconds,
                new_dist_from_core,
                nr_forwarding_channels,
                nr_recommending_channels + 1,
                nr_linking_channels,
                lang_priorities,
                acty_slope=5,
            )
            update_d["collection_priority"] = priority

        collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")


def handler(context, event):
    # Triggered by chans_to_query
    nest_asyncio.apply()
    # Keep, in future if we want to use more than 1 key, this is essential
    key_name = os.environ["TELEGRAM_OWNER"]
    # Set relative priority for project's languages. Since the language detection is
    # surely not 100% reliable, have to allow for popular channels not detected as using
    # these to be collectable.
    lang_priorities = {
        lc: 1e-3 for lc in ["EN", "FR", "ES", "DE", "EL", "IT", "PL", "RO"]
    }

    fs = context.fs
    producer = context.producer
    client = context.client
    connection = context.connection

    data = json.loads(event.body.decode("utf-8"))
    channel_id = data.get("id")
    access_hash = data.get("access_hash")
    channel_username = data.get("username")
    distance_from_core = data.get("distance_from_core", 0)
    nr_forwarding_channels = data.get("nr_forwarding_channels", 0)
    nr_recommending_channels = data.get("nr_recommending_channels", 0)
    nr_linking_channels = data.get("nr_linking_channels", 0)

    try:
        query_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
        channel_full = collegram.channels.get_full(
            client,
            channel_username=channel_username,
            channel_id=channel_id,
            access_hash=access_hash,
        )
        channel_id = channel_full.full_chat.id
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

    update_d = {"id": channel_id, "channel_last_queried_at": query_time}
    collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")

    query_info = {
        "query_id": str(uuid.uuid4()),
        "query_date": query_time,
        "data_owner": os.environ["TELEGRAM_OWNER"],
    }

    data_path = Path("/telegram/")
    paths = collegram.paths.ProjectPaths(data=data_path)

    def insert_anon_pair(original, anonymised):
        insert_d = {"original": original, "anonymised": anonymised}
        collegram.utils.insert_into_postgres(
            connection, table="anonymisation_map", values=insert_d
        )

    anonymiser = collegram.utils.HMAC_anonymiser(save_func=insert_anon_pair)

    lang_detector = LanguageDetectorBuilder.from_all_languages().build()

    # Order chats such that one corresponding to channel_full is first, so we don't
    # query it twice
    chats = [c for c in channel_full.chats if c.id == channel_id] + [
        c for c in channel_full.chats if c.id != channel_id
    ]
    for i, chat in enumerate(chats):
        if i > 0:
            query_time = datetime.datetime.now().astimezone(datetime.timezone.utc)
            channel_full = collegram.channels.get_full(
                client,
                channel=chat,
            )
            query_info["query_date"] = query_time
            query_info["query_id"] = str(uuid.uuid4())
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
        for rec_id, rec_hash in recommended_chans.items():
            handle_recommended(
                rec_id,
                rec_hash,
                connection,
                distance_from_core,
                producer,
                lang_priorities,
            )
        channel_full_d["recommended_channels"] = list(recommended_chans.keys())

        for content_type, f in collegram.messages.MESSAGE_CONTENT_TYPE_MAP.items():
            count = collegram.messages.get_channel_messages_count(client, chat, f)
            channel_full_d[f"{content_type}_count"] = count

        lang_code = collegram.text.detect_chan_lang(channel_full_d, lang_detector)
        lifespan_seconds = (
            chat.date.replace(tzinfo=None) - query_time.replace(tzinfo=None)
        ).total_seconds()
        prio = collegram.channels.get_explo_priority(
            lang_code,
            channel_full_d["message_count"],
            chat.participants_count,
            lifespan_seconds,
            distance_from_core,
            nr_forwarding_channels,
            nr_recommending_channels,
            nr_linking_channels,
            lang_priorities,
            acty_slope=5,
        )
        update_d = {
            "id": chat.id,
            "username": chat.username,
            "collection_priority": prio,
            "language_code": lang_code,
            "created_at": chat.date,
            "nr_participants": chat.participants_count,
            "nr_messages": channel_full_d["message_count"],
        }
        collegram.utils.update_postgres(connection, "channels_to_query", update_d, "id")

        channel_full_d = collegram.channels.anon_full_dict(
            channel_full_d,
            anonymiser,
        )
        collegram.channels.save(channel_full_d, paths, key_name, fs=fs)

        flat_channel_d = collegram.channels.flatten_dict(channel_full_d)
        flat_channel_d["table"] = "telegram-channel-metadata"
        flat_channel_d["query_id"] = query_info["query_id"]
        # send channel metadata to iceberg
        producer.send("telegram_collected_channels", value=iceberg_json_dumps(flat_channel_d))

        # Save metadata about the query itself
        query_info["channel_id"] = chat.id
        chan_paths = collegram.paths.ChannelPaths(chat.id, paths)
        query_info["result_path"] = str(chan_paths.channel.absolute())
        m = json.loads(json.dumps(query_info, default=_json_default))
        m["table"] = "telegram-queries"
        producer.send("telegram_collected_metadata", value=m)
